"""
kpi_mapper.py
-------------
Maps natural language KPI descriptions to actual table names
and column names used in the template engine.
"""

import logging

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# KPI Map
# key   → natural language KPI phrase (lowercase)
# value → { table_name, kpi_col }
# ─────────────────────────────────────────────────────────────────────────────
KPI_MAP = {


}


def resolve_kpi(kpi_text: str, aggregation: str = "SUM") -> dict:
    """
    Resolves a natural language KPI description to
    { table_name, kpi_col, aggregation }.

    Resolution order:
      1. Direct match
      2. Substring match (map key inside kpi_text, or kpi_text inside map key)
      3. Fallback with warning
    """
    key = kpi_text.lower().strip()

    # 1 — Direct match
    if key in KPI_MAP:
        return {**KPI_MAP[key], "aggregation": aggregation}

    # 2 — Fuzzy substring match
    for map_key, val in KPI_MAP.items():
        if map_key in key or key in map_key:
            logger.warning("KPI fuzzy match: '%s' → '%s'", kpi_text, map_key)
            return {**val, "aggregation": aggregation}

    # 3 — Fallback
    logger.warning("KPI '%s' not found in map — defaulting to COMMON_Total_Revenue", kpi_text)
    return {
        "table_name":  "COMMON_Seg_Fct",
        "kpi_col":     "COMMON_Total_Revenue",
        "aggregation": aggregation
    }
