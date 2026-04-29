#!/usr/bin/env python3
"""
Spark SQL Column Lineage Tracer & Graph Generator (sqlglot version)

Uses sqlglot's AST parser and built-in lineage module to extract column-level
lineage from Spark SQL queries with CTEs, then generates an interactive HTML
visualization showing the trace from output columns back to source tables.

Dependencies:
    pip install sqlglot

Usage:
    python spark_sql_lineage.py --sql-file query.sql --output lineage.html
    python spark_sql_lineage.py --sql-file query.sql --output lineage.html --json
"""

import re
import json
import argparse
import sys
from collections import defaultdict
from typing import Optional

import sqlglot
from sqlglot import exp, lineage as sqlglot_lineage
from sqlglot.optimizer.scope import build_scope
from sqlglot.optimizer.qualify import qualify


# ──────────────────────────────────────────────────────────────
# Lineage Extraction (via sqlglot)
# ──────────────────────────────────────────────────────────────

class SparkSQLLineageParser:
    """
    Parse Spark SQL using sqlglot's AST and extract column-level lineage.
    
    Strategy:
    1. Split the input SQL on `;` to get one or more statements.
    2. For each non-CTE statement, treat it as an output block. Wrap any
       leading WITH ... CTEs around each output block so sqlglot.lineage
       can resolve column references.
    3. For each output column, call sqlglot_lineage.lineage() to get a
       lineage Node tree, then walk the tree to extract:
         - leaf source tables/columns
         - intermediate CTE columns
         - the SQL expression for derived columns
    """

    def __init__(self, sql: str, dialect: str = 'spark'):
        self.sql = sql
        self.dialect = dialect
        self.with_clause = None        # the shared WITH ... clause (sqlglot exp)
        self.output_statements = []    # list of (output_name, full_sql_with_cte, parsed_ast)
        self.cte_names = set()
        self.source_tables = set()
        self.cte_details = {}          # cte_name -> {columns: [...], sources: [...]}
        self.lineage_data = {}         # output_name -> col_name -> lineage tree dict
        
        self._parse()

    def _parse(self):
        """Parse SQL, identify CTEs and output statements."""
        statements = sqlglot.parse(self.sql, dialect=self.dialect)
        statements = [s for s in statements if s is not None]
        
        if not statements:
            raise ValueError("Could not parse any SQL statements")

        # Find a WITH clause from any statement (typically the first SELECT).
        # Use find(exp.With) since the args key varies across sqlglot versions.
        shared_with = None
        for stmt in statements:
            with_node = stmt.find(exp.With)
            if with_node is not None and with_node.parent is stmt:
                shared_with = with_node
                break
        
        self.with_clause = shared_with

        if shared_with:
            for cte in shared_with.expressions:
                self.cte_names.add(cte.alias.lower())
                self._extract_cte_details(cte)

        # Build a map of source-table aliases to actual table names so we can
        # resolve `cb.column` back to `credit_bureau.column` etc.
        self.alias_to_table = self._build_alias_map(statements, shared_with)

        # Identify output statements
        output_idx = 0
        for stmt in statements:
            if not isinstance(stmt, (exp.Select, exp.Insert, exp.Union)):
                continue
            
            output_idx += 1
            
            if isinstance(stmt, exp.Insert):
                output_name = stmt.this.name.lower() if stmt.this else f"output_{output_idx}"
            else:
                output_name = f"output_{output_idx}"
            
            stmt_copy = stmt.copy()
            existing_with = stmt_copy.find(exp.With)
            has_top_level_with = existing_with is not None and existing_with.parent is stmt_copy
            
            if shared_with is not None and not has_top_level_with:
                stmt_copy.set('with', shared_with.copy())
            
            # Store the AST itself, not the stringified form (avoids serialization bugs)
            self.output_statements.append((output_name, stmt_copy.sql(dialect=self.dialect), stmt_copy))
            
            # Identify source tables referenced (not CTEs)
            for table in stmt_copy.find_all(exp.Table):
                tname = table.name.lower()
                if tname and tname not in self.cte_names:
                    self.source_tables.add(tname)

        if shared_with:
            for cte in shared_with.expressions:
                for table in cte.find_all(exp.Table):
                    tname = table.name.lower()
                    if tname and tname not in self.cte_names:
                        self.source_tables.add(tname)
        
        self._extract_all_lineage()

    def _build_alias_map(self, statements, shared_with) -> dict:
        """
        Build a global fallback mapping of (alias -> actual_table_name).
        
        Note: This is only used as a last-resort fallback. The primary resolution
        path uses sqlglot's `Node.reference_node_name` for CTE refs and
        `Node.source` (a Table AST node) for leaf refs, which is much more
        robust because it doesn't suffer from alias collisions across scopes.
        """
        global_map = {}
        
        roots = list(statements)
        if shared_with:
            for cte in shared_with.expressions:
                roots.append(cte)
        
        for root in roots:
            for table in root.find_all(exp.Table):
                actual = table.name.lower()
                alias = table.alias_or_name.lower()
                if not alias:
                    continue
                # Prefer CTE name resolutions
                if alias in global_map and global_map[alias] in self.cte_names:
                    continue
                global_map[alias] = actual
        
        return {"global": global_map}

    def _extract_cte_details(self, cte_node):
        """Get column names and source tables for a CTE."""
        cte_name = cte_node.alias.lower()
        select = cte_node.this
        
        columns = []
        if isinstance(select, (exp.Select, exp.Union)):
            try:
                # Use named_selects for column aliases
                columns = [c.lower() for c in select.named_selects]
            except Exception:
                columns = []
        
        sources = []
        for table in select.find_all(exp.Table):
            tname = table.name.lower()
            alias = table.alias_or_name.lower()
            sources.append((alias, tname))
        
        self.cte_details[cte_name] = {
            "columns": columns,
            "sources": sources,
        }

    def _extract_all_lineage(self):
        """Run sqlglot.lineage for every output column and convert to dict trees."""
        for output_name, output_sql, output_ast in self.output_statements:
            self.lineage_data[output_name] = {}
            
            select_for_columns = output_ast
            if isinstance(output_ast, exp.Insert):
                select_for_columns = output_ast.expression
            
            try:
                output_columns = [c.lower() for c in select_for_columns.named_selects]
            except Exception:
                output_columns = []
            
            for col_name in output_columns:
                try:
                    # Pass AST directly — passing stringified SQL can lose the WITH
                    # clause due to args-key naming differences across sqlglot versions.
                    node = sqlglot_lineage.lineage(
                        column=col_name,
                        sql=output_ast,
                        dialect=self.dialect,
                    )
                    self.lineage_data[output_name][col_name] = self._node_to_dict(node, output_name, root=True)
                except Exception as e:
                    self.lineage_data[output_name][col_name] = {
                        "table": output_name,
                        "column": col_name,
                        "expression": f"[lineage_error: {type(e).__name__}: {str(e)[:100]}]",
                        "is_derived": False,
                        "children": [],
                    }

    def _node_to_dict(self, node, expected_output_name: str, root: bool = False) -> dict:
        """
        Convert a sqlglot lineage Node tree into our dict format.
        
        sqlglot's Node provides reliable resolution hooks:
          - `reference_node_name`: the CTE name if this node references a CTE column
          - `source`: when the node is a leaf, this is the Table AST whose
            `.name` is the actual physical table name (alias-resolved)
          - `name`: format is `column_name` for root, `alias.column` for refs
        """
        name = node.name
        
        if root:
            table = expected_output_name
            column = name.lower()
        elif '.' in name:
            parts = name.split('.', 1)
            ref_name = parts[0].lower().strip('"').strip('`')
            column = parts[1].lower().strip('"').strip('`')
            
            # Use sqlglot's reference_node_name when available — it gives us
            # the resolved CTE name directly, no alias-collision games needed.
            ref_node_name = (node.reference_node_name or "").lower()
            if ref_node_name:
                table = ref_node_name
            elif ref_name in self.cte_names:
                table = ref_name
            elif isinstance(node.source, exp.Table):
                # Leaf node — node.source IS the resolved Table AST
                table = node.source.name.lower()
            else:
                # Fallback: try to resolve via global alias map
                global_map = self.alias_to_table.get("global", {})
                table = global_map.get(ref_name, ref_name)
        else:
            table = expected_output_name
            column = name.lower()
        
        is_derived = False
        expression_sql = ""
        
        if node.expression is not None:
            try:
                if isinstance(node.expression, exp.Alias):
                    inner = node.expression.this
                    if not isinstance(inner, exp.Column):
                        is_derived = True
                        expression_sql = inner.sql(dialect=self.dialect)
                elif isinstance(node.expression, exp.Column):
                    is_derived = False
                elif isinstance(node.expression, exp.Table):
                    is_derived = False
                else:
                    is_derived = True
                    expression_sql = node.expression.sql(dialect=self.dialect)
            except Exception:
                pass

        if len(expression_sql) > 500:
            expression_sql = expression_sql[:500] + " ..."

        children = []
        for child in (node.downstream or []):
            children.append(self._node_to_dict(child, expected_output_name, root=False))

        return {
            "table": table,
            "column": column,
            "expression": expression_sql,
            "is_derived": is_derived,
            "children": children,
        }

    def get_source_tables(self) -> set:
        return self.source_tables

    def get_lineage_json(self) -> str:
        """Export full lineage metadata as JSON string for the HTML graph."""
        export = {
            "source_tables": sorted(self.source_tables),
            "ctes": sorted(self.cte_names),
            "output_tables": [name for name, _, _ in self.output_statements],
            "lineage": self.lineage_data,
            "cte_details": self.cte_details,
        }
        return json.dumps(export, indent=2)


# ──────────────────────────────────────────────────────────────
# HTML Graph Generator (unchanged from the regex version)
# ──────────────────────────────────────────────────────────────

def generate_html(lineage_json: str) -> str:
    """Generate an interactive HTML page for column lineage visualization."""
    
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Spark SQL Column Lineage</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600;700&display=swap');

  :root {
    --bg: #0f1117;
    --surface: #1a1d27;
    --surface2: #242833;
    --border: #2e3345;
    --text: #e2e4ed;
    --text-dim: #8b90a5;
    --accent: #6c8cff;
    --accent-glow: rgba(108,140,255,0.15);
    --source: #34d399;
    --source-bg: rgba(52,211,153,0.08);
    --cte: #f59e0b;
    --cte-bg: rgba(245,158,11,0.08);
    --output: #f472b6;
    --output-bg: rgba(244,114,182,0.08);
    --derived: #a78bfa;
    --highlight-line: rgba(108,140,255,0.6);
    --dim-line: rgba(100,106,130,0.2);
  }

  * { margin: 0; padding: 0; box-sizing: border-box; }
  
  body {
    font-family: 'IBM Plex Sans', sans-serif;
    background: var(--bg);
    color: var(--text);
    overflow: hidden;
    height: 100vh;
  }

  #app {
    display: flex;
    height: 100vh;
  }

  #sidebar {
    width: 320px;
    min-width: 320px;
    background: var(--surface);
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }

  #sidebar-header {
    padding: 20px;
    border-bottom: 1px solid var(--border);
  }

  #sidebar-header h1 {
    font-size: 15px;
    font-weight: 600;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    color: var(--text-dim);
    margin-bottom: 12px;
  }

  #search {
    width: 100%;
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 10px 14px;
    color: var(--text);
    font-family: 'IBM Plex Mono', monospace;
    font-size: 13px;
    outline: none;
    transition: border-color 0.2s;
  }
  #search:focus { border-color: var(--accent); }
  #search::placeholder { color: var(--text-dim); }

  #sidebar-content {
    flex: 1;
    overflow-y: auto;
    padding: 12px;
  }

  .table-group { margin-bottom: 16px; }

  .table-group-header {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    font-weight: 600;
    padding: 8px 10px;
    border-radius: 6px;
    cursor: pointer;
    display: flex;
    align-items: center;
    gap: 8px;
    user-select: none;
  }
  .table-group-header:hover { filter: brightness(1.2); }

  .table-group-header .dot { width: 8px; height: 8px; border-radius: 50%; }

  .table-group-header .arrow {
    margin-left: auto;
    font-size: 10px;
    transition: transform 0.2s;
  }
  .table-group.collapsed .arrow { transform: rotate(-90deg); }

  .table-group.source .table-group-header { background: var(--source-bg); color: var(--source); }
  .table-group.source .dot { background: var(--source); }
  .table-group.cte .table-group-header { background: var(--cte-bg); color: var(--cte); }
  .table-group.cte .dot { background: var(--cte); }
  .table-group.output .table-group-header { background: var(--output-bg); color: var(--output); }
  .table-group.output .dot { background: var(--output); }

  .column-list { padding: 4px 0 0 18px; }
  .table-group.collapsed .column-list { display: none; }

  .column-item {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    padding: 5px 10px;
    border-radius: 4px;
    cursor: pointer;
    color: var(--text-dim);
    transition: all 0.15s;
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .column-item:hover { background: var(--surface2); color: var(--text); }
  .column-item.active { background: var(--accent-glow); color: var(--accent); }

  #canvas-container {
    flex: 1;
    position: relative;
    overflow: hidden;
  }

  #canvas-toolbar {
    position: absolute;
    top: 16px; right: 16px;
    display: flex;
    gap: 8px;
    z-index: 10;
  }

  .toolbar-btn {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    color: var(--text-dim);
    padding: 8px 12px;
    font-size: 12px;
    cursor: pointer;
    font-family: 'IBM Plex Sans', sans-serif;
    transition: all 0.15s;
  }
  .toolbar-btn:hover { color: var(--text); border-color: var(--accent); }

  #info-panel {
    position: absolute;
    bottom: 16px; left: 16px; right: 16px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px 20px;
    font-size: 13px;
    z-index: 10;
    display: none;
    max-height: 240px;
    overflow-y: auto;
  }

  #info-panel .info-title {
    font-weight: 600;
    color: var(--accent);
    margin-bottom: 6px;
    font-family: 'IBM Plex Mono', monospace;
  }

  #info-panel .info-expr {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    color: var(--derived);
    background: var(--surface2);
    padding: 8px 12px;
    border-radius: 6px;
    margin-top: 8px;
    word-break: break-all;
    white-space: pre-wrap;
  }

  #info-panel .info-path {
    margin-top: 8px;
    font-size: 12px;
    color: var(--text-dim);
    line-height: 1.6;
  }

  svg { width: 100%; height: 100%; }

  .node-group { cursor: pointer; }
  .node-rect { rx: 8; ry: 8; transition: filter 0.2s; }
  .node-group:hover .node-rect { filter: brightness(1.2); }

  .node-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    fill: var(--text);
    pointer-events: none;
  }

  .node-header {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    font-weight: 600;
    pointer-events: none;
  }

  .edge {
    fill: none;
    stroke-width: 1.5;
    transition: stroke 0.3s, stroke-width 0.3s, opacity 0.3s;
  }

  .edge.dimmed { stroke: var(--dim-line) !important; opacity: 0.3; stroke-width: 1; }
  .edge.highlighted { stroke: var(--highlight-line) !important; stroke-width: 2.5; opacity: 1; }

  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

  #legend {
    position: absolute;
    top: 16px; left: 16px;
    display: flex;
    gap: 16px;
    z-index: 10;
    font-size: 12px;
    color: var(--text-dim);
  }
  .legend-item { display: flex; align-items: center; gap: 6px; }
  .legend-dot { width: 10px; height: 10px; border-radius: 50%; }
  
  #parser-badge {
    position: absolute;
    bottom: 16px; right: 16px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 6px 12px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    color: var(--text-dim);
    z-index: 10;
  }
  #parser-badge .badge-accent { color: var(--accent); }
</style>
</head>
<body>

<div id="app">
  <div id="sidebar">
    <div id="sidebar-header">
      <h1>Column Lineage Explorer</h1>
      <input type="text" id="search" placeholder="Search columns..." />
    </div>
    <div id="sidebar-content"></div>
  </div>
  <div id="canvas-container">
    <div id="legend">
      <div class="legend-item"><div class="legend-dot" style="background:var(--source)"></div>Source Table</div>
      <div class="legend-item"><div class="legend-dot" style="background:var(--cte)"></div>CTE</div>
      <div class="legend-item"><div class="legend-dot" style="background:var(--output)"></div>Output Table</div>
      <div class="legend-item"><div class="legend-dot" style="background:var(--derived)"></div>Derived Column</div>
    </div>
    <div id="canvas-toolbar">
      <button class="toolbar-btn" onclick="zoomIn()">Zoom +</button>
      <button class="toolbar-btn" onclick="zoomOut()">Zoom −</button>
      <button class="toolbar-btn" onclick="resetView()">Reset</button>
      <button class="toolbar-btn" onclick="toggleAllCTEs()">Toggle CTEs</button>
    </div>
    <svg id="graph"></svg>
    <div id="info-panel"></div>
    <div id="parser-badge">parsed by <span class="badge-accent">sqlglot</span> AST</div>
  </div>
</div>

<script>
const LINEAGE_DATA = __LINEAGE_JSON__;

class LineageGraph {
  constructor(data) {
    this.data = data;
    this.nodes = [];
    this.edges = [];
    this.nodeMap = {};
    this.selectedColumn = null;
    this.transform = { x: 50, y: 50, scale: 1 };
    this.dragging = false;
    this.dragStart = { x: 0, y: 0 };
    
    this.COL_HEIGHT = 22;
    this.HEADER_HEIGHT = 32;
    this.NODE_WIDTH = 220;
    this.H_GAP = 80;
    this.V_GAP = 30;
    
    this.buildGraph();
    this.layout();
    this.render();
    this.buildSidebar();
    this.setupInteractions();
  }

  buildGraph() {
    const { source_tables, ctes, output_tables, cte_details, lineage } = this.data;
    
    source_tables.forEach(t => {
      const cols = new Set();
      this._collectSourceColumns(lineage, t, cols);
      this.addNode(t, 'source', Array.from(cols).sort());
    });
    
    ctes.forEach(c => {
      const cols = cte_details[c] ? cte_details[c].columns : [];
      this.addNode(c, 'cte', cols);
    });
    
    output_tables.forEach(o => {
      const cols = lineage[o] ? Object.keys(lineage[o]) : [];
      this.addNode(o, 'output', cols);
    });
    
    this._buildEdgesFromLineage(lineage);
  }
  
  _collectSourceColumns(lineage, tableName, cols) {
    for (const [outTable, columns] of Object.entries(lineage)) {
      for (const [colName, tree] of Object.entries(columns)) {
        this._walkTree(tree, tableName, cols);
      }
    }
  }
  
  _walkTree(node, tableName, cols) {
    if (node.table === tableName && (!node.children || node.children.length === 0)) {
      cols.add(node.column);
    }
    if (node.children) {
      node.children.forEach(c => this._walkTree(c, tableName, cols));
    }
  }
  
  _buildEdgesFromLineage(lineage) {
    const edgeSet = new Set();
    for (const [outTable, columns] of Object.entries(lineage)) {
      for (const [colName, tree] of Object.entries(columns)) {
        this._walkEdges(tree, edgeSet);
      }
    }
  }
  
  _walkEdges(node, edgeSet) {
    if (node.children) {
      node.children.forEach(child => {
        const key = `${child.table}.${child.column}->${node.table}.${node.column}`;
        if (!edgeSet.has(key)) {
          edgeSet.add(key);
          this.edges.push({
            from: { table: child.table, column: child.column },
            to: { table: node.table, column: node.column }
          });
        }
        this._walkEdges(child, edgeSet);
      });
    }
  }
  
  addNode(name, type, columns) {
    const node = {
      id: name,
      type,
      columns: columns || [],
      x: 0, y: 0,
      width: this.NODE_WIDTH,
      height: this.HEADER_HEIGHT + Math.max(columns.length, 1) * this.COL_HEIGHT + 8
    };
    this.nodes.push(node);
    this.nodeMap[name] = node;
  }
  
  layout() {
    const sources = this.nodes.filter(n => n.type === 'source');
    const ctes = this.nodes.filter(n => n.type === 'cte');
    const outputs = this.nodes.filter(n => n.type === 'output');
    
    let y = 0;
    sources.forEach(n => {
      n.x = 0;
      n.y = y;
      y += n.height + this.V_GAP;
    });
    
    const cteColSize = Math.max(1, Math.ceil(ctes.length / 2));
    ctes.forEach((n, i) => {
      const col = Math.floor(i / cteColSize);
      const row = i % cteColSize;
      n.x = (this.NODE_WIDTH + this.H_GAP) * (1 + col);
      if (row === 0) y = 0;
      n.y = y;
      y += n.height + this.V_GAP;
    });
    
    const maxCTECol = ctes.length > 0 ? Math.floor((ctes.length - 1) / cteColSize) + 1 : 1;
    y = 0;
    outputs.forEach(n => {
      n.x = (this.NODE_WIDTH + this.H_GAP) * (1 + maxCTECol);
      n.y = y;
      y += n.height + this.V_GAP;
    });
  }
  
  render() {
    const svg = document.getElementById('graph');
    svg.innerHTML = '';
    
    const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    g.id = 'graph-group';
    svg.appendChild(g);
    
    this.edges.forEach((edge) => {
      const fromNode = this.nodeMap[edge.from.table];
      const toNode = this.nodeMap[edge.to.table];
      if (!fromNode || !toNode) return;
      
      const fromColIdx = fromNode.columns.indexOf(edge.from.column);
      const toColIdx = toNode.columns.indexOf(edge.to.column);
      if (fromColIdx < 0 || toColIdx < 0) return;
      
      const x1 = fromNode.x + fromNode.width;
      const y1 = fromNode.y + this.HEADER_HEIGHT + fromColIdx * this.COL_HEIGHT + this.COL_HEIGHT / 2;
      const x2 = toNode.x;
      const y2 = toNode.y + this.HEADER_HEIGHT + toColIdx * this.COL_HEIGHT + this.COL_HEIGHT / 2;
      
      const midX = (x1 + x2) / 2;
      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('d', `M${x1},${y1} C${midX},${y1} ${midX},${y2} ${x2},${y2}`);
      path.setAttribute('class', 'edge');
      path.setAttribute('stroke', 'rgba(100,106,130,0.35)');
      path.dataset.from = `${edge.from.table}.${edge.from.column}`;
      path.dataset.to = `${edge.to.table}.${edge.to.column}`;
      g.appendChild(path);
    });
    
    this.nodes.forEach(node => {
      const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      group.setAttribute('class', 'node-group');
      group.setAttribute('transform', `translate(${node.x}, ${node.y})`);
      
      const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      rect.setAttribute('class', 'node-rect');
      rect.setAttribute('width', node.width);
      rect.setAttribute('height', node.height);
      rect.setAttribute('fill', 'var(--surface2)');
      rect.setAttribute('stroke', node.type === 'source' ? 'var(--source)' : node.type === 'cte' ? 'var(--cte)' : 'var(--output)');
      rect.setAttribute('stroke-width', '1.5');
      group.appendChild(rect);
      
      const headerBg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      headerBg.setAttribute('width', node.width);
      headerBg.setAttribute('height', this.HEADER_HEIGHT);
      headerBg.setAttribute('rx', '8');
      headerBg.setAttribute('fill', node.type === 'source' ? 'var(--source-bg)' : node.type === 'cte' ? 'var(--cte-bg)' : 'var(--output-bg)');
      group.appendChild(headerBg);
      const headerClip = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      headerClip.setAttribute('y', this.HEADER_HEIGHT - 8);
      headerClip.setAttribute('width', node.width);
      headerClip.setAttribute('height', 8);
      headerClip.setAttribute('fill', node.type === 'source' ? 'var(--source-bg)' : node.type === 'cte' ? 'var(--cte-bg)' : 'var(--output-bg)');
      group.appendChild(headerClip);
      
      const header = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      header.setAttribute('class', 'node-header');
      header.setAttribute('x', 12);
      header.setAttribute('y', 21);
      header.setAttribute('fill', node.type === 'source' ? 'var(--source)' : node.type === 'cte' ? 'var(--cte)' : 'var(--output)');
      header.textContent = node.id;
      group.appendChild(header);
      
      const badge = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      badge.setAttribute('class', 'node-label');
      badge.setAttribute('x', node.width - 12);
      badge.setAttribute('y', 21);
      badge.setAttribute('text-anchor', 'end');
      badge.setAttribute('fill', 'var(--text-dim)');
      badge.setAttribute('font-size', '10');
      badge.textContent = `${node.columns.length} cols`;
      group.appendChild(badge);
      
      node.columns.forEach((col, i) => {
        const colText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        colText.setAttribute('class', 'node-label col-label');
        colText.setAttribute('x', 14);
        colText.setAttribute('y', this.HEADER_HEIGHT + i * this.COL_HEIGHT + 15);
        colText.dataset.table = node.id;
        colText.dataset.column = col;
        const displayName = col.length > 25 ? col.substring(0, 23) + '..' : col;
        colText.textContent = displayName;
        
        const hitRect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        hitRect.setAttribute('x', 2);
        hitRect.setAttribute('y', this.HEADER_HEIGHT + i * this.COL_HEIGHT + 2);
        hitRect.setAttribute('width', node.width - 4);
        hitRect.setAttribute('height', this.COL_HEIGHT - 2);
        hitRect.setAttribute('fill', 'transparent');
        hitRect.setAttribute('rx', 4);
        hitRect.dataset.table = node.id;
        hitRect.dataset.column = col;
        hitRect.style.cursor = 'pointer';
        hitRect.addEventListener('click', (e) => { e.stopPropagation(); this.selectColumn(node.id, col); });
        hitRect.addEventListener('mouseenter', () => { hitRect.setAttribute('fill', 'rgba(108,140,255,0.08)'); });
        hitRect.addEventListener('mouseleave', () => { 
          if (!(this.selectedColumn && this.selectedColumn.table === node.id && this.selectedColumn.column === col)) {
            hitRect.setAttribute('fill', 'transparent'); 
          }
        });
        
        group.appendChild(hitRect);
        group.appendChild(colText);
      });
      
      g.appendChild(group);
    });
    
    this.applyTransform();
  }
  
  selectColumn(table, column) {
    this.selectedColumn = { table, column };
    
    const relatedEdges = new Set();
    this._traceLineage(table, column, relatedEdges, 'upstream');
    this._traceLineage(table, column, relatedEdges, 'downstream');
    
    document.querySelectorAll('.edge').forEach(e => {
      const edgeKey = `${e.dataset.from}->${e.dataset.to}`;
      if (relatedEdges.has(edgeKey)) {
        e.classList.add('highlighted');
        e.classList.remove('dimmed');
      } else {
        e.classList.add('dimmed');
        e.classList.remove('highlighted');
      }
    });
    
    this.showInfo(table, column);
    
    document.querySelectorAll('.column-item').forEach(el => {
      el.classList.toggle('active', el.dataset.table === table && el.dataset.column === column);
    });
  }
  
  _traceLineage(table, column, result, direction) {
    const key = `${table}.${column}`;
    if (result.has(key)) return;
    result.add(key);
    
    this.edges.forEach(e => {
      if (direction === 'upstream' && e.to.table === table && e.to.column === column) {
        const edgeKey = `${e.from.table}.${e.from.column}->${e.to.table}.${e.to.column}`;
        result.add(edgeKey);
        this._traceLineage(e.from.table, e.from.column, result, 'upstream');
      }
      if (direction === 'downstream' && e.from.table === table && e.from.column === column) {
        const edgeKey = `${e.from.table}.${e.from.column}->${e.to.table}.${e.to.column}`;
        result.add(edgeKey);
        this._traceLineage(e.to.table, e.to.column, result, 'downstream');
      }
    });
  }
  
  showInfo(table, column) {
    const panel = document.getElementById('info-panel');
    panel.style.display = 'block';
    
    const paths = [];
    this._findPaths(table, column, [{ table, column }], paths);
    
    let expr = this._findExpression(table, column);
    
    let html = `<div class="info-title">${table}.${column}</div>`;
    if (expr) {
      html += `<div class="info-expr">${this._escapeHtml(expr)}</div>`;
    }
    
    if (paths.length > 0) {
      html += `<div class="info-path"><strong>Lineage paths:</strong><br/>`;
      const uniquePaths = [...new Set(paths.map(p => p.map(s => `${s.table}.${s.column}`).join(' → ')))];
      uniquePaths.slice(0, 10).forEach(p => {
        html += `<div style="margin:4px 0;padding:4px 8px;background:var(--surface2);border-radius:4px;">${p}</div>`;
      });
      if (uniquePaths.length > 10) {
        html += `<div style="color:var(--text-dim);margin-top:4px;">... and ${uniquePaths.length - 10} more paths</div>`;
      }
      html += `</div>`;
    }
    
    panel.innerHTML = html;
  }
  
  _findExpression(table, column) {
    // Search through the lineage trees for a node matching this table.column
    const search = (node) => {
      if (node.table === table && node.column === column && node.expression) {
        return node.expression;
      }
      if (node.children) {
        for (const c of node.children) {
          const found = search(c);
          if (found) return found;
        }
      }
      return null;
    };
    
    for (const cols of Object.values(this.data.lineage)) {
      for (const tree of Object.values(cols)) {
        const found = search(tree);
        if (found) return found;
      }
    }
    return null;
  }
  
  _findPaths(table, column, currentPath, allPaths) {
    const upstreamEdges = this.edges.filter(e => e.to.table === table && e.to.column === column);
    
    if (upstreamEdges.length === 0) {
      if (currentPath.length > 1) {
        allPaths.push([...currentPath].reverse());
      }
      return;
    }
    
    upstreamEdges.forEach(e => {
      if (currentPath.some(s => s.table === e.from.table && s.column === e.from.column)) return;
      currentPath.push({ table: e.from.table, column: e.from.column });
      this._findPaths(e.from.table, e.from.column, currentPath, allPaths);
      currentPath.pop();
    });
  }
  
  _escapeHtml(s) {
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }
  
  clearSelection() {
    this.selectedColumn = null;
    document.querySelectorAll('.edge').forEach(e => {
      e.classList.remove('highlighted', 'dimmed');
    });
    document.getElementById('info-panel').style.display = 'none';
    document.querySelectorAll('.column-item').forEach(el => el.classList.remove('active'));
  }
  
  buildSidebar() {
    const container = document.getElementById('sidebar-content');
    
    const groups = [
      { type: 'output', nodes: this.nodes.filter(n => n.type === 'output') },
      { type: 'cte', nodes: this.nodes.filter(n => n.type === 'cte') },
      { type: 'source', nodes: this.nodes.filter(n => n.type === 'source') },
    ];
    
    groups.forEach(group => {
      group.nodes.forEach(node => {
        const div = document.createElement('div');
        div.className = `table-group ${group.type}`;
        
        const header = document.createElement('div');
        header.className = 'table-group-header';
        header.innerHTML = `<span class="dot"></span>${node.id}<span class="arrow">▼</span>`;
        header.addEventListener('click', () => div.classList.toggle('collapsed'));
        div.appendChild(header);
        
        const colList = document.createElement('div');
        colList.className = 'column-list';
        node.columns.forEach(col => {
          const item = document.createElement('div');
          item.className = 'column-item';
          item.dataset.table = node.id;
          item.dataset.column = col;
          item.textContent = col;
          item.addEventListener('click', () => this.selectColumn(node.id, col));
          colList.appendChild(item);
        });
        div.appendChild(colList);
        
        if (group.type !== 'output') div.classList.add('collapsed');
        
        container.appendChild(div);
      });
    });
    
    document.getElementById('search').addEventListener('input', (e) => {
      const q = e.target.value.toLowerCase();
      document.querySelectorAll('.column-item').forEach(item => {
        const match = item.dataset.column.includes(q) || item.dataset.table.includes(q);
        item.style.display = match ? '' : 'none';
      });
      if (q) {
        document.querySelectorAll('.table-group').forEach(g => g.classList.remove('collapsed'));
      }
    });
  }
  
  setupInteractions() {
    const svg = document.getElementById('graph');
    
    svg.addEventListener('mousedown', (e) => {
      if (e.target === svg || e.target.tagName === 'svg' || e.target.id === 'graph-group') {
        this.dragging = true;
        this.dragStart = { x: e.clientX - this.transform.x, y: e.clientY - this.transform.y };
      }
    });
    window.addEventListener('mousemove', (e) => {
      if (this.dragging) {
        this.transform.x = e.clientX - this.dragStart.x;
        this.transform.y = e.clientY - this.dragStart.y;
        this.applyTransform();
      }
    });
    window.addEventListener('mouseup', () => { this.dragging = false; });
    
    svg.addEventListener('wheel', (e) => {
      e.preventDefault();
      const delta = e.deltaY > 0 ? 0.9 : 1.1;
      this.transform.scale = Math.max(0.1, Math.min(3, this.transform.scale * delta));
      this.applyTransform();
    });
    
    svg.addEventListener('click', (e) => {
      if (e.target === svg || e.target.id === 'graph-group') {
        this.clearSelection();
      }
    });
  }
  
  applyTransform() {
    const g = document.getElementById('graph-group');
    if (g) {
      g.setAttribute('transform', `translate(${this.transform.x},${this.transform.y}) scale(${this.transform.scale})`);
    }
  }
}

let graph;

function zoomIn() {
  graph.transform.scale = Math.min(3, graph.transform.scale * 1.2);
  graph.applyTransform();
}
function zoomOut() {
  graph.transform.scale = Math.max(0.1, graph.transform.scale * 0.8);
  graph.applyTransform();
}
function resetView() {
  graph.transform = { x: 50, y: 50, scale: 1 };
  graph.applyTransform();
  graph.clearSelection();
}
function toggleAllCTEs() {
  document.querySelectorAll('.table-group.cte').forEach(g => g.classList.toggle('collapsed'));
}

document.addEventListener('DOMContentLoaded', () => {
  graph = new LineageGraph(LINEAGE_DATA);
});
</script>
</body>
</html>"""
    
    return html.replace('__LINEAGE_JSON__', lineage_json)


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Spark SQL Column Lineage Tracer (sqlglot)')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--sql-file', help='Path to SQL file')
    group.add_argument('--sql-string', help='SQL string directly')
    parser.add_argument('--output', default='lineage.html', help='Output HTML file path')
    parser.add_argument('--json', action='store_true', help='Also output raw JSON lineage')
    parser.add_argument('--dialect', default='spark', help='SQL dialect (default: spark)')
    
    args = parser.parse_args()
    
    if args.sql_file:
        with open(args.sql_file, 'r') as f:
            sql = f.read()
    else:
        sql = args.sql_string
    
    print(f"Parsing SQL with sqlglot (dialect={args.dialect})...")
    tracer = SparkSQLLineageParser(sql, dialect=args.dialect)
    
    print(f"  Sources: {sorted(tracer.source_tables)}")
    print(f"  CTEs:    {sorted(tracer.cte_names)}")
    print(f"  Outputs: {[name for name, _, _ in tracer.output_statements]}")
    
    # Count stats
    total_cols = sum(len(cols) for cols in tracer.lineage_data.values())
    failed = sum(
        1 for cols in tracer.lineage_data.values()
        for tree in cols.values()
        if 'lineage_error' in tree.get('expression', '')
    )
    print(f"  Total output columns: {total_cols}")
    if failed:
        print(f"  Columns with lineage errors: {failed}")
    
    lineage_json = tracer.get_lineage_json()
    
    if args.json:
        json_path = args.output.replace('.html', '.json')
        with open(json_path, 'w') as f:
            f.write(lineage_json)
        print(f"JSON lineage written to {json_path}")
    
    html = generate_html(lineage_json)
    with open(args.output, 'w') as f:
        f.write(html)
    
    print(f"Interactive lineage graph written to {args.output}")


if __name__ == '__main__':
    main()
