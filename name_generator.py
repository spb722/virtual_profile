"""
name_generator.py
-----------------
Generates consistent VP names from extracted agent JSON.
Naming conventions mirror real VP names found in the catalog.
"""

from kpi_mapper import resolve_kpi


# Short codes for Track 4 operations
_OP_SHORT = {
    "PERCENTAGE_DROP":   "PCT_DROP",
    "PERCENTAGE_CHANGE": "PCT_CHG",
    "RATIO":             "RATIO",
    "DIFFERENCE":        "DIFF",
}


def _clean_col(kpi_col: str) -> str:
    """Strip common prefixes and uppercase for VP name readability."""
    col = kpi_col
    for prefix in ("COMMON_", "Common_", "common_",
                   "RECHARGE_", "SUBSCRIPTIONS_", "BILL_PAYMENT_"):
        col = col.replace(prefix, "")
    return col.upper()


def generate_vp_name(
    track: int,
    extracted: dict,
    vp_a_name: str = None,
    vp_b_name: str = None
) -> str:
    """
    Build a VP name from the extracted agent output.

    Parameters
    ----------
    track       : track number 1-5
    extracted   : dict from the Pydantic model (.model_dump())
    vp_a_name   : resolved VP name for operand A (Track 4 only)
    vp_b_name   : resolved VP name for operand B (Track 4 only)
    """

    # ── Track 1 — Time Series ─────────────────────────────────────────────────
    if track == 1:
        tw = extracted.get("time_window", {})
        if isinstance(tw, dict):
            tw_type = tw.get("type", "")
            tw_val  = tw.get("value")
            tw_unit = tw.get("unit", "DAY") or "DAY"
        else:
            tw_type = getattr(tw, "type", "")
            tw_val  = getattr(tw, "value", None)
            tw_unit = getattr(tw, "unit", "DAY") or "DAY"

        # Multi-KPI formula path: use formula_name directly, skip KPI mapper call
        formula_name = extracted.get("formula_name")
        kpi_list     = extracted.get("kpi_list")
        if kpi_list and formula_name:
            col = formula_name.upper().replace(" ", "_")
        else:
            kpi_info = resolve_kpi(
                extracted.get("kpi", ""),
                extracted.get("aggregation", "SUM")
            )
            col = _clean_col(kpi_info["kpi_col"])

        if tw_type == "FIXED_MONTH":
            return f"M{tw_val}_{col}"
        elif tw_type == "ROLLING_WEEK":
            return f"ROLLING_W{tw_val}_{col}"
        elif tw_type == "LAST_N":
            suffix = "MONTHS" if tw_unit == "MONTH" else "DAYS"
            return f"LAST{tw_val}{suffix}_{col}"
        elif tw_type == "MTD":
            return f"MTD_{col}"
        elif tw_type == "LMTD":
            return f"LMTD_{col}"
        else:
            return f"T1_{col}"

    # ── Track 2 — Static Flag ─────────────────────────────────────────────────
    elif track == 2:
        kpi   = extracted.get("kpi", "unknown").upper().replace(" ", "_")
        state = extracted.get("expected_state", "FLAG")
        return f"{state}_{kpi}"

    # ── Track 3 — Snapshot ────────────────────────────────────────────────────
    elif track == 3:
        kpi       = extracted.get("kpi", "unknown").upper().replace(" ", "_")
        qualifier = extracted.get("qualifier", "LATEST")
        return f"{qualifier}_{kpi}"

    # ── Track 4 — Comparative (composite) ────────────────────────────────────
    elif track == 4:
        op    = extracted.get("operation", "COMP")
        short = _OP_SHORT.get(op, "COMP")
        return f"{vp_a_name}__{vp_b_name}__{short}"

    # ── Track 5 — Parameterized ───────────────────────────────────────────────
    elif track == 5:
        kpi_info = resolve_kpi(
            extracted.get("kpi", ""),
            extracted.get("aggregation", "SUM")
        )
        col   = _clean_col(kpi_info["kpi_col"])
        agg   = extracted.get("aggregation", "SUM")
        param = extracted.get("parameter_name", "X")
        unit  = extracted.get("parameter_unit", "DAY") or "DAY"
        return f"{agg}_{col}_LAST_{param}_{unit}S"

    # ── Track 6 — Join Check ─────────────────────────────────────────────────
    elif track == 6:
        kpi_info = resolve_kpi(extracted.get("kpi", ""))
        col = _clean_col(kpi_info["kpi_col"])
        join_var = extracted.get("join_var", "UNKNOWN").upper()
        return f"JOIN_{join_var}_{col}"

    return "UNKNOWN_VP"
