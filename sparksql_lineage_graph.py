#!/usr/bin/env python3
"""
Spark SQL Column Lineage Tracer & Graph Generator

Parses a complex Spark SQL query with CTEs and generates an interactive HTML
visualization showing column-level lineage from output tables back to source tables.

Usage:
    python spark_sql_lineage.py --sql-file query.sql --output lineage.html
    python spark_sql_lineage.py --sql-string "WITH cte AS (...) SELECT ..."
"""

import re
import json
import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional


# ──────────────────────────────────────────────────────────────
# Data Structures
# ──────────────────────────────────────────────────────────────

@dataclass
class ColumnRef:
    """A reference to a column: which table/alias it comes from and the original column name."""
    source_alias: str          # table alias or CTE name
    source_column: str         # original column name in that source
    expression: str = ""       # the SQL expression (for derived columns)
    is_derived: bool = False   # True if column is computed from expression

@dataclass
class CTEDefinition:
    """Parsed CTE or final SELECT block."""
    name: str
    columns: dict = field(default_factory=dict)        # alias -> ColumnRef
    source_tables: list = field(default_factory=list)   # list of (alias, actual_table_or_cte)
    raw_sql: str = ""

@dataclass
class LineageNode:
    """A node in the lineage graph."""
    table: str
    column: str
    expression: str = ""
    is_derived: bool = False
    children: list = field(default_factory=list)  # upstream LineageNode refs


# ──────────────────────────────────────────────────────────────
# SQL Parser (regex-based, handles common Spark SQL patterns)
# ──────────────────────────────────────────────────────────────

class SparkSQLLineageParser:
    """
    Parse Spark SQL with CTEs and extract column-level lineage.
    
    Limitations (intentional for tractability):
    - Regex-based, not a full SQL AST parser
    - Handles common patterns: direct refs, aliases, CONCAT, CASE, SUM/COUNT/AVG/MAX/MIN,
      COALESCE, NULLIF, ROUND, FLOOR, DATEDIFF, FIRST_VALUE, window functions
    - Complex nested sub-selects in FROM may need manual annotation
    """

    # Patterns for extracting column references like `alias.column`
    COL_REF_PATTERN = re.compile(r'\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b')
    
    # Pattern to match function calls
    FUNC_PATTERN = re.compile(
        r'\b(CONCAT|COALESCE|NULLIF|ROUND|FLOOR|CEIL|DATEDIFF|DATE_SUB|'
        r'SUM|COUNT|AVG|MAX|MIN|FIRST_VALUE|LAST_VALUE|IF|IFNULL|NVL|'
        r'CASE|UPPER|LOWER|TRIM|SUBSTRING|CAST|CONVERT)\b',
        re.IGNORECASE
    )

    def __init__(self, sql: str):
        self.sql = self._clean_sql(sql)
        self.ctes: dict[str, CTEDefinition] = {}
        self.output_blocks: list[CTEDefinition] = []
        self._parse()

    def _clean_sql(self, sql: str) -> str:
        """Remove comments and normalize whitespace."""
        # Remove single-line comments
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        # Remove multi-line comments
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        # Normalize whitespace
        sql = re.sub(r'\s+', ' ', sql).strip()
        return sql

    def _find_matching_paren(self, s: str, start: int) -> int:
        """Find the matching closing parenthesis."""
        depth = 0
        for i in range(start, len(s)):
            if s[i] == '(':
                depth += 1
            elif s[i] == ')':
                depth -= 1
                if depth == 0:
                    return i
        return len(s) - 1

    def _split_ctes(self):
        """Split the SQL into CTE definitions and final SELECT(s)."""
        sql = self.sql

        # Check if there's a WITH clause
        with_match = re.match(r'\bWITH\b\s+', sql, re.IGNORECASE)
        if not with_match:
            # No CTEs, entire thing is output
            self.output_blocks.append(self._parse_select_block(sql, "output_1"))
            return

        # Remove the WITH keyword
        sql_after_with = sql[with_match.end():]

        # Parse CTE blocks: name AS ( ... )
        cte_blocks = []
        remaining = sql_after_with
        
        while True:
            # Match CTE name
            cte_name_match = re.match(r'(\w+)\s+AS\s*\(', remaining, re.IGNORECASE)
            if not cte_name_match:
                break
            
            cte_name = cte_name_match.group(1).lower()
            paren_start = cte_name_match.end() - 1  # position of '('
            paren_end = self._find_matching_paren(remaining, paren_start)
            
            cte_body = remaining[paren_start + 1:paren_end].strip()
            cte_blocks.append((cte_name, cte_body))
            
            # Move past this CTE
            remaining = remaining[paren_end + 1:].strip()
            if remaining.startswith(','):
                remaining = remaining[1:].strip()

        # Whatever's left after CTEs are the output SELECT statements
        output_parts = re.split(r';\s*', remaining)
        output_parts = [p.strip() for p in output_parts if p.strip() and re.search(r'\bSELECT\b', p, re.IGNORECASE)]

        # Parse each CTE
        for cte_name, cte_body in cte_blocks:
            self.ctes[cte_name] = self._parse_select_block(cte_body, cte_name)

        # Parse output blocks
        for i, out_sql in enumerate(output_parts):
            block_name = f"output_{i+1}"
            # Try to detect INSERT INTO target name
            insert_match = re.match(r'INSERT\s+INTO\s+(\w+)', out_sql, re.IGNORECASE)
            if insert_match:
                block_name = insert_match.group(1).lower()
            self.output_blocks.append(self._parse_select_block(out_sql, block_name))

    def _parse_select_block(self, sql: str, name: str) -> CTEDefinition:
        """Parse a SELECT block to extract columns and source tables."""
        cte_def = CTEDefinition(name=name, raw_sql=sql)

        # Extract FROM / JOIN sources
        cte_def.source_tables = self._extract_sources(sql)

        # Extract SELECT columns
        cte_def.columns = self._extract_columns(sql, cte_def.source_tables)

        return cte_def

    def _extract_sources(self, sql: str) -> list:
        """Extract table/CTE references from FROM and JOIN clauses."""
        sources = []
        
        # Match FROM table alias patterns
        from_pattern = re.compile(
            r'\bFROM\s+(\w+)\s+(\w+)|\bJOIN\s+(\w+)\s+(\w+)',
            re.IGNORECASE
        )
        
        for m in from_pattern.finditer(sql):
            table = (m.group(1) or m.group(3)).lower()
            alias = (m.group(2) or m.group(4)).lower()
            # Skip keywords that look like aliases
            if alias.upper() in ('ON', 'WHERE', 'AND', 'OR', 'SET', 'INTO', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'FULL'):
                alias = table
            sources.append((alias, table))
        
        return sources

    def _extract_columns(self, sql: str, sources: list) -> dict:
        """Extract column definitions from SELECT clause."""
        columns = {}

        # Get the SELECT ... FROM portion
        select_match = re.search(r'\bSELECT\s+(.*?)\bFROM\b', sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return columns

        select_clause = select_match.group(1).strip()

        # Split by commas, but respect parentheses
        col_exprs = self._split_select_columns(select_clause)

        alias_to_table = {a: t for a, t in sources}

        for expr in col_exprs:
            expr = expr.strip()
            if not expr:
                continue

            # Check for AS alias
            alias_match = re.search(r'\bAS\s+(\w+)\s*$', expr, re.IGNORECASE)
            if alias_match:
                col_alias = alias_match.group(1).lower()
                col_expr = expr[:alias_match.start()].strip()
            else:
                # No AS - the alias is the last part
                col_expr = expr
                # Simple column ref: alias.column
                simple_ref = re.match(r'^(\w+)\.(\w+)$', expr.strip())
                if simple_ref:
                    col_alias = simple_ref.group(2).lower()
                else:
                    # Use the expression itself as alias (shouldn't happen often in well-written SQL)
                    col_alias = re.sub(r'[^a-zA-Z0-9_]', '_', expr)[:50].lower()

            # Find all column references in the expression
            refs = self._extract_column_refs(col_expr, alias_to_table)
            
            is_derived = bool(self.FUNC_PATTERN.search(col_expr)) or 'CASE' in col_expr.upper() or len(refs) > 1

            col_ref = ColumnRef(
                source_alias=refs[0][0] if refs else "",
                source_column=refs[0][1] if refs else col_alias,
                expression=col_expr if is_derived else "",
                is_derived=is_derived
            )
            # Store all upstream refs
            col_ref._all_refs = refs
            columns[col_alias] = col_ref

        return columns

    def _split_select_columns(self, select_clause: str) -> list:
        """Split SELECT columns by comma, respecting nested parentheses and CASE/END."""
        parts = []
        depth = 0
        current = []
        case_depth = 0
        
        tokens = re.split(r'(\bCASE\b|\bEND\b|[(),])', select_clause, flags=re.IGNORECASE)
        
        for token in tokens:
            upper = token.strip().upper()
            if upper == 'CASE':
                case_depth += 1
                current.append(token)
            elif upper == 'END':
                case_depth -= 1
                current.append(token)
            elif token == '(':
                depth += 1
                current.append(token)
            elif token == ')':
                depth -= 1
                current.append(token)
            elif token == ',' and depth == 0 and case_depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(token)
        
        if current:
            parts.append(''.join(current))
        
        return parts

    def _extract_column_refs(self, expr: str, alias_to_table: dict) -> list:
        """Extract all alias.column references from an expression."""
        refs = []
        for m in self.COL_REF_PATTERN.finditer(expr):
            alias = m.group(1).lower()
            col = m.group(2).lower()
            
            # Skip SQL keywords/functions that look like alias.column
            if alias.upper() in ('CURRENT_DATE', 'CURRENT_TIMESTAMP'):
                continue
                
            actual_table = alias_to_table.get(alias, alias)
            refs.append((actual_table, col))
        
        return refs

    def _parse(self):
        """Main parse entry point."""
        self._split_ctes()

    def resolve_lineage(self, output_name: str, column_name: str, 
                         visited: Optional[set] = None) -> LineageNode:
        """
        Recursively resolve lineage for a given output column back to source tables.
        Returns a tree of LineageNode.
        """
        if visited is None:
            visited = set()
        
        cache_key = (output_name, column_name)
        if cache_key in visited:
            return LineageNode(table=output_name, column=column_name, expression="[circular]")
        visited.add(cache_key)

        # Find the block (CTE or output)
        block = self.ctes.get(output_name)
        if not block:
            for ob in self.output_blocks:
                if ob.name == output_name:
                    block = ob
                    break

        if not block:
            # It's a source table (leaf node)
            return LineageNode(table=output_name, column=column_name)

        col_ref = block.columns.get(column_name)
        if not col_ref:
            # Column not found in this block - it's a pass-through or source
            return LineageNode(table=output_name, column=column_name)

        node = LineageNode(
            table=output_name,
            column=column_name,
            expression=col_ref.expression,
            is_derived=col_ref.is_derived
        )

        # Get all upstream references
        all_refs = getattr(col_ref, '_all_refs', [])
        if not all_refs and col_ref.source_alias:
            all_refs = [(col_ref.source_alias, col_ref.source_column)]

        for src_table, src_col in all_refs:
            # Check if source is a CTE (recurse) or a real table (leaf)
            if src_table in self.ctes:
                child = self.resolve_lineage(src_table, src_col, visited.copy())
            else:
                child = LineageNode(table=src_table, column=src_col)
            node.children.append(child)

        return node

    def get_all_lineage(self) -> dict:
        """Get lineage for all output columns."""
        result = {}
        for output_block in self.output_blocks:
            block_lineage = {}
            for col_name in output_block.columns:
                tree = self.resolve_lineage(output_block.name, col_name)
                block_lineage[col_name] = tree
            result[output_block.name] = block_lineage
        return result

    def lineage_to_dict(self, node: LineageNode) -> dict:
        """Convert LineageNode tree to JSON-serializable dict."""
        return {
            "table": node.table,
            "column": node.column,
            "expression": node.expression,
            "is_derived": node.is_derived,
            "children": [self.lineage_to_dict(c) for c in node.children]
        }

    def get_source_tables(self) -> set:
        """Identify all base source tables (non-CTE)."""
        all_referenced = set()
        for block in list(self.ctes.values()) + self.output_blocks:
            for alias, table in block.source_tables:
                all_referenced.add(table)
        
        cte_names = set(self.ctes.keys())
        return all_referenced - cte_names

    def get_lineage_json(self) -> str:
        """Export full lineage as JSON."""
        all_lineage = self.get_all_lineage()
        export = {}
        for output_name, columns in all_lineage.items():
            export[output_name] = {
                col: self.lineage_to_dict(tree) 
                for col, tree in columns.items()
            }
        
        meta = {
            "source_tables": sorted(self.get_source_tables()),
            "ctes": sorted(self.ctes.keys()),
            "output_tables": [ob.name for ob in self.output_blocks],
            "lineage": export,
            "cte_details": {}
        }
        
        # Add CTE column info for the graph
        for cte_name, cte_def in self.ctes.items():
            meta["cte_details"][cte_name] = {
                "columns": list(cte_def.columns.keys()),
                "sources": [(a, t) for a, t in cte_def.source_tables]
            }
        
        return json.dumps(meta, indent=2)


# ──────────────────────────────────────────────────────────────
# HTML Graph Generator
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

  /* ── Left Panel ── */
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

  .table-group {
    margin-bottom: 16px;
  }

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

  .table-group-header .dot {
    width: 8px; height: 8px; border-radius: 50%;
  }

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

  .column-list {
    padding: 4px 0 0 18px;
  }
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
  .column-item .derived-badge {
    font-size: 9px;
    background: rgba(167,139,250,0.2);
    color: var(--derived);
    padding: 1px 5px;
    border-radius: 3px;
  }

  /* ── Main Canvas ── */
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
    max-height: 200px;
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
  }

  #info-panel .info-path {
    margin-top: 8px;
    font-size: 12px;
    color: var(--text-dim);
    line-height: 1.6;
  }

  #info-panel .path-step {
    display: inline-flex;
    align-items: center;
    gap: 4px;
  }
  #info-panel .path-arrow { color: var(--text-dim); margin: 0 4px; }

  svg { width: 100%; height: 100%; }

  .node-group { cursor: pointer; }
  .node-rect {
    rx: 8; ry: 8;
    transition: filter 0.2s;
  }
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

  /* scrollbar */
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
  </div>
</div>

<script>
// ── Lineage Data (injected by Python) ──
const LINEAGE_DATA = __LINEAGE_JSON__;

// ── Graph Layout Engine ──
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
    this.showCTEs = true;
    
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
    
    // Build node for each source table
    source_tables.forEach(t => {
      // Collect all columns referenced from this source
      const cols = new Set();
      this._collectSourceColumns(lineage, t, cols);
      this.addNode(t, 'source', Array.from(cols));
    });
    
    // Build node for each CTE
    ctes.forEach(c => {
      const cols = cte_details[c] ? cte_details[c].columns : [];
      this.addNode(c, 'cte', cols);
    });
    
    // Build node for each output table
    output_tables.forEach(o => {
      const cols = lineage[o] ? Object.keys(lineage[o]) : [];
      this.addNode(o, 'output', cols);
    });
    
    // Build edges from lineage
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
    // Arrange in layers: sources -> CTEs -> outputs
    const sources = this.nodes.filter(n => n.type === 'source');
    const ctes = this.nodes.filter(n => n.type === 'cte');
    const outputs = this.nodes.filter(n => n.type === 'output');
    
    // Sources column
    let y = 0;
    sources.forEach(n => {
      n.x = 0;
      n.y = y;
      y += n.height + this.V_GAP;
    });
    
    // CTEs - arrange in multiple columns if many
    const cteColSize = Math.ceil(ctes.length / 2);
    ctes.forEach((n, i) => {
      const col = Math.floor(i / cteColSize);
      const row = i % cteColSize;
      n.x = (this.NODE_WIDTH + this.H_GAP) * (1 + col);
      // Reset y for each CTE column
      if (row === 0) y = 0;
      n.y = y;
      y += n.height + this.V_GAP;
    });
    
    // Outputs
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
    
    // Draw edges first (behind nodes)
    this.edges.forEach((edge, i) => {
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
    
    // Draw nodes
    this.nodes.forEach(node => {
      const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      group.setAttribute('class', 'node-group');
      group.setAttribute('transform', `translate(${node.x}, ${node.y})`);
      
      // Background
      const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      rect.setAttribute('class', 'node-rect');
      rect.setAttribute('width', node.width);
      rect.setAttribute('height', node.height);
      rect.setAttribute('fill', 'var(--surface2)');
      rect.setAttribute('stroke', node.type === 'source' ? 'var(--source)' : node.type === 'cte' ? 'var(--cte)' : 'var(--output)');
      rect.setAttribute('stroke-width', '1.5');
      group.appendChild(rect);
      
      // Header bar
      const headerBg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      headerBg.setAttribute('width', node.width);
      headerBg.setAttribute('height', this.HEADER_HEIGHT);
      headerBg.setAttribute('rx', '8');
      headerBg.setAttribute('fill', node.type === 'source' ? 'var(--source-bg)' : node.type === 'cte' ? 'var(--cte-bg)' : 'var(--output-bg)');
      group.appendChild(headerBg);
      // Clip bottom corners of header
      const headerClip = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      headerClip.setAttribute('y', this.HEADER_HEIGHT - 8);
      headerClip.setAttribute('width', node.width);
      headerClip.setAttribute('height', 8);
      headerClip.setAttribute('fill', node.type === 'source' ? 'var(--source-bg)' : node.type === 'cte' ? 'var(--cte-bg)' : 'var(--output-bg)');
      group.appendChild(headerClip);
      
      // Header text
      const header = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      header.setAttribute('class', 'node-header');
      header.setAttribute('x', 12);
      header.setAttribute('y', 21);
      header.setAttribute('fill', node.type === 'source' ? 'var(--source)' : node.type === 'cte' ? 'var(--cte)' : 'var(--output)');
      header.textContent = node.id;
      group.appendChild(header);
      
      // Column count badge
      const badge = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      badge.setAttribute('class', 'node-label');
      badge.setAttribute('x', node.width - 12);
      badge.setAttribute('y', 21);
      badge.setAttribute('text-anchor', 'end');
      badge.setAttribute('fill', 'var(--text-dim)');
      badge.setAttribute('font-size', '10');
      badge.textContent = `${node.columns.length} cols`;
      group.appendChild(badge);
      
      // Column labels
      node.columns.forEach((col, i) => {
        const colText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        colText.setAttribute('class', 'node-label col-label');
        colText.setAttribute('x', 14);
        colText.setAttribute('y', this.HEADER_HEIGHT + i * this.COL_HEIGHT + 15);
        colText.dataset.table = node.id;
        colText.dataset.column = col;
        const displayName = col.length > 25 ? col.substring(0, 23) + '..' : col;
        colText.textContent = displayName;
        
        // Clickable hit area
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
        hitRect.addEventListener('click', () => this.selectColumn(node.id, col));
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
    
    // Collect all related edges
    const relatedEdges = new Set();
    this._traceLineage(table, column, relatedEdges, 'upstream');
    this._traceLineage(table, column, relatedEdges, 'downstream');
    
    // Highlight/dim edges
    document.querySelectorAll('.edge').forEach(e => {
      const key = `${e.dataset.from}->${e.dataset.to}`;
      const reverseKey = `${e.dataset.to}->${e.dataset.from}`;
      if (relatedEdges.has(e.dataset.from + '->' + e.dataset.to) || 
          relatedEdges.has(key) || relatedEdges.has(reverseKey) ||
          relatedEdges.has(e.dataset.from) || relatedEdges.has(e.dataset.to)) {
        e.classList.add('highlighted');
        e.classList.remove('dimmed');
      } else {
        e.classList.add('dimmed');
        e.classList.remove('highlighted');
      }
    });
    
    // Show info panel
    this.showInfo(table, column);
    
    // Update sidebar
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
    
    // Find lineage path
    const paths = [];
    this._findPaths(table, column, [{ table, column }], paths);
    
    let expr = '';
    // Check if column is derived
    for (const [outTable, cols] of Object.entries(this.data.lineage)) {
      if (outTable === table && cols[column] && cols[column].expression) {
        expr = cols[column].expression;
        break;
      }
    }
    // Also check CTE details
    for (const [cteName, detail] of Object.entries(this.data.cte_details || {})) {
      if (cteName === table) break;
    }
    
    let html = `<div class="info-title">${table}.${column}</div>`;
    if (expr) {
      html += `<div class="info-expr">${this._escapeHtml(expr)}</div>`;
    }
    
    if (paths.length > 0) {
      html += `<div class="info-path"><strong>Lineage paths:</strong><br/>`;
      const uniquePaths = [...new Set(paths.map(p => p.map(s => `${s.table}.${s.column}`).join(' → ')))];
      uniquePaths.slice(0, 8).forEach(p => {
        html += `<div style="margin:4px 0;padding:4px 8px;background:var(--surface2);border-radius:4px;">${p}</div>`;
      });
      if (uniquePaths.length > 8) {
        html += `<div style="color:var(--text-dim);margin-top:4px;">... and ${uniquePaths.length - 8} more paths</div>`;
      }
      html += `</div>`;
    }
    
    panel.innerHTML = html;
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
      if (currentPath.some(s => s.table === e.from.table && s.column === e.from.column)) return; // avoid cycles
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
      { type: 'output', label: 'Output Tables', nodes: this.nodes.filter(n => n.type === 'output') },
      { type: 'cte', label: 'CTEs', nodes: this.nodes.filter(n => n.type === 'cte') },
      { type: 'source', label: 'Source Tables', nodes: this.nodes.filter(n => n.type === 'source') },
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
        
        // Collapse CTEs and sources by default
        if (group.type !== 'output') div.classList.add('collapsed');
        
        container.appendChild(div);
      });
    });
    
    // Search
    document.getElementById('search').addEventListener('input', (e) => {
      const q = e.target.value.toLowerCase();
      document.querySelectorAll('.column-item').forEach(item => {
        const match = item.dataset.column.includes(q) || item.dataset.table.includes(q);
        item.style.display = match ? '' : 'none';
      });
      document.querySelectorAll('.table-group').forEach(g => {
        const visible = g.querySelectorAll('.column-item[style=""]').length > 0 || 
                       g.querySelectorAll('.column-item:not([style])').length > 0;
        if (q) g.classList.remove('collapsed');
      });
    });
  }
  
  setupInteractions() {
    const svg = document.getElementById('graph');
    
    // Pan
    svg.addEventListener('mousedown', (e) => {
      if (e.target === svg || e.target.tagName === 'svg') {
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
    
    // Zoom
    svg.addEventListener('wheel', (e) => {
      e.preventDefault();
      const delta = e.deltaY > 0 ? 0.9 : 1.1;
      this.transform.scale = Math.max(0.1, Math.min(3, this.transform.scale * delta));
      this.applyTransform();
    });
    
    // Click background to clear
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

// ── Global controls ──
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

// ── Initialize ──
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
    parser = argparse.ArgumentParser(description='Spark SQL Column Lineage Tracer')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--sql-file', help='Path to SQL file')
    group.add_argument('--sql-string', help='SQL string directly')
    parser.add_argument('--output', default='lineage.html', help='Output HTML file path')
    parser.add_argument('--json', action='store_true', help='Also output raw JSON lineage')
    
    args = parser.parse_args()
    
    if args.sql_file:
        with open(args.sql_file, 'r') as f:
            sql = f.read()
    else:
        sql = args.sql_string
    
    print("Parsing SQL...")
    tracer = SparkSQLLineageParser(sql)
    
    print(f"Found {len(tracer.get_source_tables())} source tables, {len(tracer.ctes)} CTEs, {len(tracer.output_blocks)} output blocks")
    print(f"  Sources: {sorted(tracer.get_source_tables())}")
    print(f"  CTEs:    {sorted(tracer.ctes.keys())}")
    print(f"  Outputs: {[ob.name for ob in tracer.output_blocks]}")
    
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
