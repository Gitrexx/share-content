import sqlglot
from sqlglot import exp
import json


sql = """
INSERT INTO orders_enriched
SELECT
    o.order_id,
    o.amount * fx.rate                          AS amount_usd,
    UPPER(c.country)                            AS country,
    COALESCE(c.region, 'UNKNOWN')               AS region,
    DATEDIFF(current_date(), o.order_date)      AS days_since_order,
    CASE
        WHEN o.amount * fx.rate > 1000 THEN 'HIGH'
        WHEN o.amount * fx.rate > 100  THEN 'MED'
        ELSE 'LOW'
    END                                         AS amount_tier,
    o.amount * fx.rate * t.tax_rate             AS amount_after_tax,
    c.country || '-' || o.order_id              AS order_key
FROM orders o
JOIN fx_rates fx  ON o.currency = fx.currency
JOIN customers c  ON o.customer_id = c.id
JOIN tax_rates t  ON c.country = t.country
"""


def extract_source_columns(node):
    """Recursively extract all (table_alias, column) from an expression node"""
    sources = []
    for col in node.find_all(exp.Column):
        table = col.table or None
        column = col.name
        if table or column:
            sources.append({
                "table_alias": table,
                "column": column
            })
    return sources


def get_transform_type(select_expr):
    """Classify the type of transformation"""
    expr = select_expr.this if isinstance(select_expr, exp.Alias) else select_expr

    if isinstance(expr, exp.Column):
        return "passthrough"
    elif isinstance(expr, exp.Case):
        return "case_when"
    elif isinstance(expr, (exp.Anonymous, exp.Func)):
        return "function"
    elif isinstance(expr, (exp.Add, exp.Mul, exp.Sub, exp.Div)):
        return "arithmetic"
    elif isinstance(expr, exp.DPipe):
        return "concat"
    else:
        return "expression"


def extract_lineage_from_sql(sql, dialect="spark"):
    parsed = sqlglot.parse_one(sql, dialect=dialect)

    # Get the SELECT (handle INSERT INTO ... SELECT)
    select = parsed.find(exp.Select)
    if not select:
        return []

    from_clause = select.find(exp.From)
    joins = list(select.find_all(exp.Join))

    # Build alias → real table map
    alias_map = {}
    if from_clause:
        main_table = from_clause.find(exp.Table)
        if main_table:
            alias_map[main_table.alias or main_table.name] = main_table.name
    for join in joins:
        tbl = join.find(exp.Table)
        if tbl:
            alias_map[tbl.alias or tbl.name] = tbl.name

    results = []

    for select_expr in select.expressions:
        # Get output column name
        if isinstance(select_expr, exp.Alias):
            output_col = select_expr.alias
            inner_expr = select_expr.this
        elif isinstance(select_expr, exp.Column):
            output_col = select_expr.name
            inner_expr = select_expr
        else:
            output_col = str(select_expr)
            inner_expr = select_expr

        # Extract and resolve source columns
        raw_sources = extract_source_columns(inner_expr)

        resolved_sources = []
        for s in raw_sources:
            real_table = alias_map.get(s["table_alias"], s["table_alias"])
            resolved_sources.append({
                "table": real_table,
                "column": s["column"]
            })

        # Deduplicate
        seen = set()
        deduped_sources = []
        for s in resolved_sources:
            key = (s["table"], s["column"])
            if key not in seen:
                seen.add(key)
                deduped_sources.append(s)

        results.append({
            "output_column": output_col,
            "transform_type": get_transform_type(select_expr),
            "transform_expr": inner_expr.sql(dialect=dialect),
            "depends_on": deduped_sources
        })

    return results


if __name__ == "__main__":
    lineage_result = extract_lineage_from_sql(sql)
    print(json.dumps(lineage_result, indent=2))