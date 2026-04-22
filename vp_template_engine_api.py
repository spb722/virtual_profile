"""
Virtual Profile Rule Engine — FastAPI Template Engine
======================================================
Receives structured JSON (output from track-specific extraction agents)
and returns the filled PARENT_CONDITION rule string.

Run:
    pip install fastapi uvicorn pyyaml
    uvicorn vp_template_engine_api:app --reload --port 9978

POST /resolve  →  returns filled PARENT_CONDITION
GET  /templates →  lists all template keys
GET  /health    →  health check

FIX 1 CHANGES:
  - Track2Input: added groupby_entity field + 5 new sub_types
  - Track5Input: added groupby_entity field
  - resolve_groupby_cols(): YAML-based entity → column lookup
  - _apply_groupby(): regex post-processing for COUNT_ALL
  - resolve_track2(): handlers for 5 new sub_types
  - /resolve endpoint: groupby resolution and application
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Literal, List
import re
import logging
import yaml
import os

logger = logging.getLogger(__name__)

# =============================================================================
# App Init + YAML Load
# =============================================================================

app = FastAPI(
    title="VP Rule Engine — Template API",
    description="Resolves track agent JSON into PARENT_CONDITION rule strings",
    version="1.1.0"
)

YAML_PATH = os.path.join(os.path.dirname(__file__), "vp_template_engine.yaml")

with open(YAML_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

TEMPLATES = CONFIG
COLUMN_META = CONFIG.get("column_metadata", {})


# =============================================================================
# Input Schemas (one per track)
# =============================================================================

class TimeWindow(BaseModel):
    type: Literal["ROLLING_WEEK", "FIXED_WEEK", "FIXED_MONTH", "LAST_N", "MTD", "LMTD"]
    value: Optional[int] = None
    unit: Optional[Literal["DAY", "WEEK", "MONTH"]] = None
    exact: bool = False


class Track1Input(BaseModel):
    track: Literal[1]
    table_name: str
    kpi_col: str
    aggregation: Literal["AVG", "SUM", "COUNT", "COUNT_ALL", "MAX", "MIN"]
    time_window: TimeWindow
    is_composite: bool = False
    formula: Optional[str] = None
    vp_name: Optional[str] = None
    filter_col: Optional[str] = None
    filter_val: Optional[str] = None
    filter_values: Optional[str] = None   # semicolon-joined list for IN LIST
    # Multi-KPI virtual formula: list of column names to guard (col > 0)
    null_guard_cols: Optional[List[str]] = None


class Track2Input(BaseModel):
    track: Literal[2]
    table_name: str
    sub_type: Literal[
        "subscribed",
        "not_subscribed",
        "subscribed_within_n_days",
        "not_subscribed_within_n_days",
        "subscription_threshold",
        "exists",
        "not_exists",
        "attr_check",
        "count_flag_present",
        "count_flag_absent",
        "segment_type",
        "audience_segment",
        "multi_or_notnull",
        "whitelist",
        "status_count_zero",
        # ── FIX 1: new sub_types ──────────────────────────────────────────
        "count_groupby_only",            # bare COUNT_ALL, no null guard, no date
        "count_flag_absent_today",       # date=CurrentTime + null guard + count=0
        "count_flag_present_today",      # date=CurrentTime + null guard + count>=1
        "campaign_absent_fixed_days",    # date>=CurrentTime-NDAYS + key check + count=0
        "campaign_present_fixed_days",   # date>=CurrentTime-NDAYS + action_type IN LIST + count>0
        "promo_check_fixed_days",        # date>=CurrentTime-NDAYS + flag check + groupby COUNT > 0
        "date_value_count",              # date col as ${op} ${val} + COUNT > 0
        "multi_null_date_value_count"    # multi null guards + date as ${op} ${val} + COUNT > 0

        "count_flag_present_today",      # date=CurrentTime + null guard + count>=1
        "campaign_absent_fixed_days",    # date>=CurrentTime-NDAYS + key check + count=0
        "campaign_present_fixed_days",   # date>=CurrentTime-NDAYS + action_type IN LIST + count>0
        "bonus_present_fixed_days",      # date>=CurrentTime-NDAYS + action_type IN LIST (BONUS) + count>0
        "bonus_absent_fixed_days",       # date>=CurrentTime-NDAYS + action_type IN LIST (BONUS) + count=0
        "date_value_count",              # date col as ${op} ${val} + COUNT > 0

    ]
    id_col: Optional[str] = None
    flag_col: Optional[str] = None
    count_col: Optional[str] = None
    segment_col: Optional[str] = None
    segment_val: Optional[str] = None
    status_col: Optional[str] = None
    status_val: Optional[str] = None
    rule_id_col: Optional[str] = None
    execution_counter_col: Optional[str] = None
    segment_id_col: Optional[str] = None
    col_list: Optional[List[str]] = None
    N: Optional[int] = None
    threshold: Optional[int] = None
    is_composite: bool = False
    # ── FIX 1: groupby + extra fields for new sub_types ───────────────────
    groupby_entity: Optional[str] = None
    action_type_col: Optional[str] = None     # for campaign_present_fixed_days IN LIST check
    null_guard_col: Optional[str] = None      # single extra null guard col
    null_col_1: Optional[str] = None          # first null guard col (multi)
    null_col_2: Optional[str] = None          # second null guard col (multi)
    count_threshold_op: Optional[str] = None  # "=", ">", ">=", "<"
    count_threshold_val: Optional[str] = None # "0", "1", "2"
    dedup_qualifier: Optional[str] = "none"   # "none" | "groupby_only" | "groupby_max"


class Track3Input(BaseModel):
    track: Literal[3]
    table_name: str
    sub_type: Literal[
        "snapshot_by_id",
        "snapshot_max_check",
        "snapshot_by_date_boundary",
        "snapshot_null_zero_max",
        "geo_current",
        "geo_last_n_days",
        "geo_last_n_months"
    ]
    id_col: Optional[str] = None
    value_col: Optional[str] = None
    ref_col: Optional[str] = None
    count_col: Optional[str] = None
    kpi_col: Optional[str] = None
    lon_col: Optional[str] = None
    lat_col: Optional[str] = None
    geo_name_col: Optional[str] = None
    region_col: Optional[str] = None
    msisdn_col: Optional[str] = None
    N: Optional[int] = None
    is_composite: bool = False


class Track4Input(BaseModel):
    track: Literal[4]
    operation: Literal["PERCENTAGE_DROP", "PERCENTAGE_CHANGE", "RATIO", "DIFFERENCE"]
    vp_a: str
    vp_b: str
    vp_numerator: Optional[str] = None
    vp_denominator: Optional[str] = None
    is_composite: bool = True
    table_name: Optional[str] = None


class Track5Input(BaseModel):
    track: Literal[5]
    table_name: str
    sub_type: Literal[
        "sum_x_days",
        "count_x_days",
        "virtual_sum_x_days",
        "subscription_x_days_present",
        "subscription_x_days_absent",
        "multi_param",
        "bonus_not_sent_ak",
        "promo_sent_ak",
        "promo_delivered_segment",
        "promo_not_delivered_segment",
        "bonus_not_delivered_segment",
        "bonus_nonresponder_ak"
    ]
    kpi_col: Optional[str] = None
    id_col: Optional[str] = None
    count_col: Optional[str] = None
    formula: Optional[str] = None
    vp_name: Optional[str] = None
    action_key_col: Optional[str] = None
    action_type_col: Optional[str] = None
    segment_col: Optional[str] = None
    sent_date_col: Optional[str] = None
    msisdn_col: Optional[str] = None
    aggregation: Optional[Literal["SUM", "COUNT", "AVG", "MAX", "MIN"]] = None
    is_composite: bool = False
    # ── FIX 1: groupby support ────────────────────────────────────────────
    groupby_entity: Optional[str] = None


# ── Track 6 — JOIN_CHECK ─────────────────────────────────────────────────

class Track6DateRangeInput(BaseModel):
    operator: Literal[">=", "<=", ">", "="]
    value:    Optional[int] = None
    unit:     Literal["DAYS", "MONTHS", "HOURS"] = "DAYS"


class Track6CountCheckInput(BaseModel):
    operator: Literal["=", ">", ">=", "<", "<="]
    value:    str


class Track6Input(BaseModel):
    track:          Literal[6]
    table_name:     str
    check_col:      str                                    # resolved by KPI mapper
    join_var:       str                                    # "OM_MSISDN", "HBB_imeiNumber", etc.
    date_range:     Optional[Track6DateRangeInput] = None
    count_check:    Optional[Track6CountCheckInput] = None
    groupby_entity: Optional[str] = None
    is_composite:   bool = False


# Union input for the single /resolve endpoint
from typing import Union
from pydantic import Field

class ResolveRequest(BaseModel):
    payload: Union[Track1Input, Track2Input, Track3Input, Track4Input, Track5Input, Track6Input] = Field(
        ..., discriminator="track"
    )


# =============================================================================
# Column Metadata Resolver
# =============================================================================

def _normalize_identifier(value: str) -> str:
    return value.strip().lower()


def _get_table_meta(table_name: str) -> Optional[dict]:
    meta = COLUMN_META.get(table_name)
    if meta:
        return meta
    normalized = _normalize_identifier(table_name)
    for key, value in COLUMN_META.items():
        if _normalize_identifier(key) == normalized:
            return value
    return None


def get_date_col(table_name: str) -> str:
    meta = _get_table_meta(table_name)
    if not meta:
        raise HTTPException(
            status_code=400,
            detail=f"Table '{table_name}' not found in column_metadata. Check vp_template_engine.yaml."
        )
    date_col = meta.get("date_col")
    if not date_col:
        raise HTTPException(
            status_code=400,
            detail=f"Table '{table_name}' has no date_col defined in column_metadata."
        )
    return date_col


# =============================================================================
# FIX 1: Groupby Helpers
# =============================================================================

def get_campaign_check_cols(table_name: str, sub_type: str) -> dict:
    """
    Read campaign check column names from YAML campaign_check_mappings.
    Returns a dict with sent_date_col, action_key_col, msisdn_col (or empty dict).
    """
    meta = _get_table_meta(table_name)
    if not meta:
        return {}
    return meta.get("campaign_check_mappings", {}).get(sub_type, {})


def resolve_join_col(table_name: str, join_var: str) -> Optional[str]:
    """
    Look up the actual join column for a runtime variable name.
    Uses column_metadata.{table}.join_mappings in the YAML.

    Example: resolve_join_col("AIRTEL_LIFECYCLE_CDR", "OM_MSISDN") → "LC_MSISDN"
    """
    meta = _get_table_meta(table_name)
    if not meta:
        return None
    mappings = meta.get("join_mappings", {})
    return mappings.get(join_var)


def resolve_groupby_cols(table_name: str, groupby_entity: str) -> Optional[str]:
    """
    Look up actual column name(s) for a semantic groupby entity.
    Uses column_metadata.{table}.groupby_mappings in the YAML.

    Returns the column string (e.g. "LC_MSISDN" or "RE_REFILL_ID,RE_ESB_DESCRIPTION")
    or None if no mapping found.
    """
    meta = _get_table_meta(table_name)
    if not meta:
        return None
    mappings = meta.get("groupby_mappings", {})
    return mappings.get(groupby_entity)


def _apply_groupby(condition: str, groupby_cols: str) -> str:
    """
    Set __groupby_{cols} on every COUNT_ALL(...) in the condition.

    - If COUNT_ALL has NO __groupby_ yet → append it.
    - If COUNT_ALL already HAS __groupby_ → replace it (the YAML mapping
      may specify a wider multi-column groupby than the template default).

    Examples:
      "COUNT_ALL(LC_ACTION_KEY) = 0"  +  "LC_MSISDN"
      → "COUNT_ALL(LC_ACTION_KEY)__groupby_LC_MSISDN = 0"

      "COUNT_ALL(L_AGG_MSISDN)__groupby_L_ACTION_KEY > 0"  +  "L_ACTION_KEY,L_PROMO_SENT_DATE"
      → "COUNT_ALL(L_AGG_MSISDN)__groupby_L_ACTION_KEY,L_PROMO_SENT_DATE > 0"
    """
    # First: replace any existing __groupby_XXX with the correct columns
    condition = re.sub(
        r'(COUNT_ALL\([^)]+\))__groupby_[A-Za-z0-9_,]+',
        rf'\1__groupby_{groupby_cols}',
        condition
    )
#     condition = re.sub(
# r'((?:COUNT_ALL|SUM|MAX|MIN|AVG|COUNT)\([^)]+\))__groupby_[A-Za-z0-9_,]+',
# rf'\1__groupby_{groupby_cols}',
# condition
# )
    # Then: append to any COUNT_ALL that still has no __groupby_
    condition = re.sub(
        r'(COUNT_ALL\([^)]+\))(?!__groupby_)',
        rf'\1__groupby_{groupby_cols}',
        condition
    )
#     condition = re.sub(
# r'((?:COUNT_ALL|SUM|MAX|MIN|AVG|COUNT)\([^)]+\))(?!__groupby_)',
# rf'\1__groupby_{groupby_cols}',
# condition
# )
    
    return condition


# =============================================================================
# Track 1 Resolver
# =============================================================================

def resolve_track1(p: Track1Input) -> str:
    date_col = get_date_col(p.table_name)
    tw = p.time_window
    t1 = TEMPLATES["track_1"]

    # ── Rolling Week ─────────────────────────────────────────────────────────
    if tw.type == "ROLLING_WEEK":
        rw = t1["rolling_week"]
        week_key = f"W{tw.value}"
        offsets = rw["week_offsets"].get(week_key)
        if not offsets:
            raise HTTPException(400, f"No offsets defined for {week_key}")

        if tw.value == 1:
            if p.formula and p.vp_name:
                tmpl = rw["template_w1_virtual"]
                return tmpl.replace("{date_col}", date_col) \
                            .replace("{vp_name}", p.vp_name) \
                            .replace("{formula}", p.formula) \
                            .replace("{agg}", p.aggregation)
            tmpl = rw["template_w1"]
            return tmpl.replace("{date_col}", date_col) \
                       .replace("{agg}", p.aggregation) \
                       .replace("{kpi_col}", p.kpi_col)
        else:
            start = str(offsets["start"])
            end   = str(offsets["end"])
            if p.formula and p.vp_name:
                tmpl = rw["template_w2_plus_virtual"]
                return tmpl.replace("{date_col}", date_col) \
                            .replace("{week_start}", start) \
                            .replace("{week_end}", end) \
                            .replace("{vp_name}", p.vp_name) \
                            .replace("{formula}", p.formula) \
                            .replace("{agg}", p.aggregation)
            tmpl = rw["template_w2_plus"]
            return tmpl.replace("{date_col}", date_col) \
                       .replace("{week_start}", start) \
                       .replace("{week_end}", end) \
                       .replace("{agg}", p.aggregation) \
                       .replace("{kpi_col}", p.kpi_col)

    # ── Fixed Week ────────────────────────────────────────────────────────────
    if tw.type == "FIXED_WEEK":
        fw = t1["fixed_week"]
        n = str(tw.value)
        if p.formula and p.vp_name:
            tmpl = fw["template_virtual"]
            return tmpl.replace("{date_col}", date_col) \
                       .replace("{N}", n) \
                       .replace("{agg}", p.aggregation) \
                       .replace("{vp_name}", p.vp_name) \
                       .replace("{formula}", p.formula)
        tmpl = fw["template"]
        return tmpl.replace("{date_col}", date_col) \
                   .replace("{N}", n) \
                   .replace("{agg}", p.aggregation) \
                   .replace("{kpi_col}", p.kpi_col)

    # ── Fixed Month ───────────────────────────────────────────────────────────
    if tw.type == "FIXED_MONTH":
        fm = t1["fixed_month"]
        n = str(tw.value)
        if p.filter_col and p.filter_val:
            tmpl = fm["template_with_filter"]
            return tmpl.replace("{date_col}", date_col) \
                       .replace("{N}", n) \
                       .replace("{filter_col}", p.filter_col) \
                       .replace("{filter_val}", p.filter_val) \
                       .replace("{agg}", p.aggregation) \
                       .replace("{kpi_col}", p.kpi_col)
        if p.formula and p.vp_name:
            tmpl = fm["template_virtual"]
            return tmpl.replace("{date_col}", date_col) \
                       .replace("{N}", n) \
                       .replace("{agg}", p.aggregation) \
                       .replace("{vp_name}", p.vp_name) \
                       .replace("{formula}", p.formula)
        tmpl = fm["template"]
        return tmpl.replace("{date_col}", date_col) \
                   .replace("{N}", n) \
                   .replace("{agg}", p.aggregation) \
                   .replace("{kpi_col}", p.kpi_col)

    # ── Last N ────────────────────────────────────────────────────────────────
    if tw.type == "LAST_N":
        ln = t1["last_n"]
        n = str(tw.value)

        if tw.unit == "MONTH":
            if p.aggregation == "AVG" and p.vp_name:
                tmpl = ln["template_months_avg"]
                return tmpl.replace("{date_col}", date_col) \
                           .replace("{N}", n) \
                           .replace("{vp_name}", p.vp_name) \
                           .replace("{kpi_col}", p.kpi_col)
            else:
                if p.filter_col and p.filter_val:
                    tmpl = ln["template_months_filtered_count"]
                    return tmpl.replace("{date_col}", date_col) \
                               .replace("{N}", n) \
                               .replace("{filter_col}", p.filter_col) \
                               .replace("{filter_val}", p.filter_val) \
                               .replace("{agg}", p.aggregation) \
                               .replace("{kpi_col}", p.kpi_col)
                               
                tmpl = ln["template_months_sum"]
                return tmpl.replace("{date_col}", date_col) \
                           .replace("{N}", n) \
                           .replace("{agg}", p.aggregation) \
                           .replace("{kpi_col}", p.kpi_col)

        if tw.unit == "WEEK":
            if p.aggregation == "AVG" and p.vp_name:
                tmpl = ln["template_weeks_avg"]
                return tmpl.replace("{date_col}", date_col) \
                           .replace("{N}", n) \
                           .replace("{vp_name}", p.vp_name) \
                           .replace("{kpi_col}", p.kpi_col)
            else:
                tmpl = ln["template_weeks_sum"]
                return tmpl.replace("{date_col}", date_col) \
                           .replace("{N}", n) \
                           .replace("{agg}", p.aggregation) \
                           .replace("{kpi_col}", p.kpi_col)

        if tw.unit == "DAY" and p.aggregation == "AVG" and p.vp_name:
            tmpl = ln["template_days_avg"]
            return tmpl.replace("{date_col}", date_col) \
                       .replace("{N}", n) \
                       .replace("{vp_name}", p.vp_name) \
                       .replace("{kpi_col}", p.kpi_col)

        if not date_col or date_col == "null":
            if p.formula and p.vp_name:
                tmpl = ln["template_no_date_virtual"]
                return tmpl.replace("{agg}", p.aggregation) \
                           .replace("{vp_name}", p.vp_name) \
                           .replace("{formula}", p.formula)
            tmpl = ln["template_no_date"]
            return tmpl.replace("{kpi_col}", p.kpi_col) \
                       .replace("{agg}", p.aggregation)

        if p.filter_col and p.filter_values:
            fc = TEMPLATES["track_1"]["filtered_count"]
            if p.aggregation == "COUNT_ALL":
                tmpl = fc["template_exact_date"] if getattr(tw, "exact", False) else fc["template_range"]
                return tmpl.replace("{date_col}", date_col) \
                           .replace("{N}", n) \
                           .replace("{filter_col}", p.filter_col) \
                           .replace("{filter_values}", p.filter_values) \
                           .replace("{kpi_col}", p.kpi_col)
            else:
                tmpl = fc["template_exact_date_agg"] if getattr(tw, "exact", False) else fc["template_range_agg"]
                return tmpl.replace("{filter_col}", p.filter_col) \
                           .replace("{filter_values}", p.filter_values) \
                           .replace("{date_col}", date_col) \
                           .replace("{N}", n) \
                           .replace("{agg}", p.aggregation) \
                           .replace("{kpi_col}", p.kpi_col)

        if p.filter_col and p.filter_val:
            tmpl = ln["template_days_with_filter"]
            return tmpl.replace("{date_col}", date_col) \
                       .replace("{N}", n) \
                       .replace("{filter_col}", p.filter_col) \
                       .replace("{filter_val}", p.filter_val) \
                       .replace("{agg}", p.aggregation) \
                       .replace("{kpi_col}", p.kpi_col)

        # Multi-KPI virtual formula with per-column guards
        if p.formula and p.null_guard_cols and p.vp_name:
            guard_exprs = " AND ".join(f"{col} > 0" for col in p.null_guard_cols)
            tmpl = ln["template_days_open_virtual_guarded"]
            return tmpl.replace("{date_col}", date_col) \
                       .replace("{N}", n) \
                       .replace("{guard_exprs}", guard_exprs) \
                       .replace("{agg}", p.aggregation) \
                       .replace("{vp_name}", p.vp_name) \
                       .replace("{formula}", p.formula)

        if p.formula and p.vp_name:
            tmpl = ln["template_no_date_virtual"]
            return tmpl.replace("{agg}", p.aggregation) \
                       .replace("{vp_name}", p.vp_name) \
                       .replace("{formula}", p.formula)

        tmpl = ln["template_days_open"]
        return tmpl.replace("{date_col}", date_col) \
                   .replace("{N}", n) \
                   .replace("{agg}", p.aggregation) \
                   .replace("{kpi_col}", p.kpi_col)

    # ── MTD / LMTD ────────────────────────────────────────────────────────────
    if tw.type in ("MTD", "LMTD"):
        ml = t1["mtd_lmtd"]
        if p.filter_col and p.filter_val:
            tmpl = ml["template_mtd_with_filter"]
            return tmpl.replace("{date_col}", date_col) \
                       .replace("{filter_col}", p.filter_col) \
                       .replace("{filter_val}", p.filter_val) \
                       .replace("{agg}", p.aggregation) \
                       .replace("{kpi_col}", p.kpi_col)
        tmpl = ml["template_mtd"] if tw.type == "MTD" else ml["template_lmtd"]
        return tmpl.replace("{date_col}", date_col) \
                   .replace("{agg}", p.aggregation) \
                   .replace("{kpi_col}", p.kpi_col)

    raise HTTPException(400, f"Unknown time_window type: {tw.type}")


# =============================================================================
# Track 2 Resolver
# =============================================================================

def resolve_track2(p: Track2Input) -> str:
    t2 = TEMPLATES["track_2"]
    sub = p.sub_type

    # ── Subscription sub-types ────────────────────────────────────────────────
    if sub in ("subscribed", "not_subscribed", "subscribed_within_n_days",
               "not_subscribed_within_n_days", "subscription_threshold"):
        s = t2["subscription"]
        date_col = get_date_col(p.table_name) if p.N else None

        if sub == "subscribed":
            return s["template_subscribed"].replace("{id_col}", p.id_col)
        if sub == "not_subscribed":
            return s["template_not_subscribed"].replace("{id_col}", p.id_col)
        if sub == "subscribed_within_n_days":
            return s["template_subscribed_within_n_days"] \
                    .replace("{date_col}", date_col) \
                    .replace("{N}", str(p.N)) \
                    .replace("{id_col}", p.id_col)
        if sub == "not_subscribed_within_n_days":
            return s["template_not_subscribed_within_n_days"] \
                    .replace("{date_col}", date_col) \
                    .replace("{N}", str(p.N)) \
                    .replace("{id_col}", p.id_col)
        if sub == "subscription_threshold":
            return s["template_subscription_threshold"] \
                    .replace("{date_col}", date_col) \
                    .replace("{N}", str(p.N)) \
                    .replace("{id_col}", p.id_col) \
                    .replace("{threshold}", str(p.threshold))

    # ── FIX 1: New sub_types ──────────────────────────────────────────────────

    # Bare COUNT_ALL with no null guard, no date
    # Example: CATEGORY_COUNT_CHECK → COUNT_ALL(LC_ACTION_KEY)__groupby_LC_MSISDN ${op} ${val}
    if sub == "count_groupby_only":
        return f"COUNT_ALL({p.count_col}) ${{operator}} ${{value}}"

    # Date = CurrentTime + action key check + null guard + count absent
    # Example: RECHARGE_CHECK_SSO → GASSO_SENT_DATE = CurrentTime AND GASSO_ACTION_KEY ${op} ${val}
    #          AND GASSO_SSO_MSISDN <> NULL AND COUNT_ALL(GASSO_ACTION_KEY)__groupby_GA_SSO_MSISDN = 0
    if sub == "count_flag_absent_today":
        date_col = get_date_col(p.table_name)
        parts = [
            f"{date_col} = CurrentTime",
            f"{p.flag_col} ${{operator}} ${{value}}",
        ]
        if p.null_guard_col:
            parts.append(f"{p.null_guard_col} <> NULL")
        parts.append(f"COUNT_ALL({p.count_col}) = 0")
        return " AND ".join(parts)

    # Date = CurrentTime + action key check + null guard + count present
    # Example: RECHGARGE_ACTIVATION_CHECK → GA_SENT_DATE = CurrentTime AND GA_ACTION_KEY ${op} ${val}
    #          AND GA_SSO_MSISDN <> NULL AND COUNT_ALL(GA_ACTION_KEY)__groupby_GA_SSO_MSISDN >= 1
    if sub == "count_flag_present_today":
        date_col = get_date_col(p.table_name)
        count_op  = p.count_threshold_op or ">="
        count_val = p.count_threshold_val or "1"
        parts = [
            f"{date_col} = CurrentTime",
            f"{p.flag_col} ${{operator}} ${{value}}",
        ]
        if p.null_guard_col:
            parts.append(f"{p.null_guard_col} <> NULL")
        parts.append(f"COUNT_ALL({p.count_col}) {count_op} {count_val}")
        return " AND ".join(parts)

    # Fixed-day promo presence: date window + action_type IN LIST + flag check + count > 0
    if sub == "campaign_present_fixed_days":
        date_col = get_date_col(p.table_name)
        n_days = p.N if p.N is not None else 0
        return (
            f"{date_col} >= CurrentTime-{n_days}DAYS "
            f"AND {p.action_type_col} IN LIST (Promotion;PROMOTION;promotion) "
            f"AND {p.flag_col} ${{operator}} ${{value}} "
            f"AND COUNT_ALL({p.count_col}) > 0"
        )
        # Fixed-day bonus presence: date window + action_type IN LIST + flag check + count > 0
    if sub == "bonus_present_fixed_days":
        date_col = get_date_col(p.table_name)
        n_days = p.N if p.N is not None else 0
        return (
            f"{date_col} >= CurrentTime-{n_days}DAYS "
            f"AND {p.action_type_col} IN LIST (BONUS;Bonus;bonus) "
            f"AND {p.flag_col} ${{operator}} ${{value}} "
            f"AND COUNT_ALL({p.count_col}) > 0"
        )

    # Fixed-day bonus absence: date window + action_type IN LIST (BONUS) + flag check + count = 0
    if sub == "bonus_absent_fixed_days":
        date_col = get_date_col(p.table_name)
        n_days = p.N if p.N is not None else 0
        return (
            f"{date_col} >= CurrentTime-{n_days}DAYS "
            f"AND {p.action_type_col} IN LIST (BONUS;Bonus;bonus) "
            f"AND {p.flag_col} ${{operator}} ${{value}} "
            f"AND COUNT_ALL({p.count_col}) = 0"
        )

    # Promo presence with groupby dedup — no action_type IN LIST, uses __groupby_ COUNT
    if sub == "promo_check_fixed_days":
        date_col = get_date_col(p.table_name)
        n_days = p.N if p.N is not None else 0
        count_expr = f"COUNT_ALL({p.count_col})__groupby_{p.flag_col}"
        parts = [
            f"{date_col} >= CurrentTime-{n_days}DAYS",
            f"{p.flag_col} ${{operator}} ${{value}}",
            f"{count_expr} > 0",
        ]
        if p.dedup_qualifier == "groupby_max":
            parts.append(f"And Max({date_col}) <> NULL")
        return " AND ".join(parts)

    # Fixed-day promo absence: date window + action key check + count zero
    if sub == "campaign_absent_fixed_days":
        date_col = get_date_col(p.table_name)
        n_days = p.N if p.N is not None else 0
        return (
            f"{date_col} >= CurrentTime-{n_days}DAYS "
            f"AND {p.action_type_col} IN LIST (Promotion;PROMOTION;promotion) "
            f"AND {p.flag_col} ${{operator}} ${{value}} "
            f"AND COUNT_ALL({p.count_col}) = 0"
        )

    # Date column used as comparison value + COUNT_ALL > 0
    # Example: EXPIRY_DAYS → RE_TRANS_DT ${op} ${val} AND COUNT_ALL(RE_REFILL_TYPE)__groupby_RE_REFILL_ID > 0
    if sub == "date_value_count":
        date_col = get_date_col(p.table_name)
        count_op  = p.count_threshold_op or ">"
        count_val = p.count_threshold_val or "0"
        return (
            f"{date_col} ${{operator}} ${{value}} "
            f"AND COUNT_ALL({p.count_col}) {count_op} {count_val}"
        )

    # Multiple null guards + date as comparison value + COUNT_ALL > 0
    # Example: Post_Expiry_ESB_Description_Count_1
    #   → RE_REFILL_ID <> NULL AND RE_ESB_DESCRIPTION <> NULL AND RE_TRANS_DT ${op} ${val}
    #     AND COUNT_ALL(RE_REFILL_TYPE)__groupby_RE_REFILL_ID,RE_ESB_DESCRIPTION > 0
    if sub == "multi_null_date_value_count":
        date_col = get_date_col(p.table_name)
        count_op  = p.count_threshold_op or ">"
        count_val = p.count_threshold_val or "0"
        parts = []
        if p.null_col_1:
            parts.append(f"{p.null_col_1} <> NULL")
        if p.null_col_2:
            parts.append(f"{p.null_col_2} <> NULL")
        parts.append(f"{date_col} ${{operator}} ${{value}}")
        parts.append(f"COUNT_ALL({p.count_col}) {count_op} {count_val}")
        return " AND ".join(parts)

    # ── Flag / Existence sub-types (original) ─────────────────────────────────
    f = t2["flag_check"]

    if sub == "exists":
        return f["template_exists"].replace("{flag_col}", p.flag_col)
    if sub == "not_exists":
        return f["template_not_exists"].replace("{flag_col}", p.flag_col)
    if sub == "attr_check":
        return f["template_attr_check"].replace("{flag_col}", p.flag_col)
    if sub == "count_flag_present":
        return f["template_count_flag_present"] \
                .replace("{flag_col}", p.flag_col) \
                .replace("{count_col}", p.count_col)
    if sub == "count_flag_absent":
        return f["template_count_flag_absent"] \
                .replace("{flag_col}", p.flag_col) \
                .replace("{count_col}", p.count_col)
    if sub == "segment_type":
        return f["template_segment_type"] \
                .replace("{segment_col}", p.segment_col) \
                .replace("{segment_val}", p.segment_val) \
                .replace("{count_col}", p.count_col)
    if sub == "audience_segment":
        return f["template_audience_segment"] \
                .replace("{segment_id_col}", p.segment_id_col) \
                .replace("{execution_counter_col}", p.execution_counter_col)
    if sub == "multi_or_notnull":
        if not p.col_list or len(p.col_list) < 2:
            raise HTTPException(400, "multi_or_notnull requires col_list with at least 2 columns")
        parts = [f"{col} <> NULL" for col in p.col_list]
        return " OR ".join(parts)
    if sub == "whitelist":
        return f["template_whitelist"].replace("{rule_id_col}", p.rule_id_col)
    if sub == "status_count_zero":
        return f["template_status_count_zero"] \
                .replace("{status_col}", p.status_col) \
                .replace("{status_val}", p.status_val) \
                .replace("{count_col}", p.count_col)

    raise HTTPException(400, f"Unknown track 2 sub_type: {sub}")


# =============================================================================
# Track 3 Resolver
# =============================================================================

def resolve_track3(p: Track3Input) -> str:
    t3 = TEMPLATES["track_3"]
    sub = p.sub_type

    if sub == "snapshot_by_id":
        return t3["snapshot_id"]["template_by_id"] \
                .replace("{id_col}", p.id_col) \
                .replace("{value_col}", p.value_col)
    if sub == "snapshot_max_check":
        return t3["snapshot_id"]["template_max_check"] \
                .replace("{id_col}", p.id_col) \
                .replace("{ref_col}", p.ref_col)
    if sub == "snapshot_by_date_boundary":
        date_col = get_date_col(p.table_name)
        return t3["snapshot_id"]["template_by_date_boundary"] \
                .replace("{date_col}", date_col) \
                .replace("{N}", str(p.N)) \
                .replace("{id_col}", p.id_col) \
                .replace("{count_col}", p.count_col)
    if sub == "geo_current":
        if p.region_col:
            return t3["geo_location"]["template_current_region"] \
                    .replace("{region_col}", p.region_col)
        return t3["geo_location"]["template_current"] \
                .replace("{lon_col}", p.lon_col) \
                .replace("{lat_col}", p.lat_col) \
                .replace("{geo_name_col}", p.geo_name_col)
    if sub == "geo_last_n_days":
        date_col = get_date_col(p.table_name)
        return t3["geo_location"]["template_last_n_days"] \
                .replace("{date_col}", date_col) \
                .replace("{N}", str(p.N)) \
                .replace("{region_col}", p.region_col) \
                .replace("{msisdn_col}", p.msisdn_col)
    if sub == "geo_last_n_months":
        date_col = get_date_col(p.table_name)
        return t3["geo_location"]["template_last_n_months"] \
                .replace("{date_col}", date_col) \
                .replace("{N}", str(p.N)) \
                .replace("{region_col}", p.region_col) \
                .replace("{msisdn_col}", p.msisdn_col)
    if sub == "snapshot_null_zero_max":
        date_col = get_date_col(p.table_name)
        return t3["snapshot_id"]["template_null_zero_max"] \
                .replace("{date_col}", date_col) \
                .replace("{N}", str(p.N)) \
                .replace("{kpi_col}", p.kpi_col)

    raise HTTPException(400, f"Unknown track 3 sub_type: {sub}")


# =============================================================================
# Track 4 Resolver
# =============================================================================

def resolve_track4(p: Track4Input) -> str:
    t4 = TEMPLATES["track_4"]

    if p.operation in ("PERCENTAGE_DROP", "PERCENTAGE_CHANGE"):
        return t4["pct_drop"]["template"] \
                .replace("{vp_a}", p.vp_a) \
                .replace("{vp_b}", p.vp_b)
    if p.operation == "RATIO":
        num = p.vp_numerator or p.vp_b
        den = p.vp_denominator or p.vp_a
        return t4["ratio"]["template"] \
                .replace("{vp_numerator}", num) \
                .replace("{vp_denominator}", den)
    if p.operation == "DIFFERENCE":
        return f"({p.vp_b} - {p.vp_a}) ${{operator}} ${{value}}"

    raise HTTPException(400, f"Unknown track 4 operation: {p.operation}")


# =============================================================================
# Track 5 Resolver
# =============================================================================

def resolve_track5(p: Track5Input) -> str:
    t5 = TEMPLATES["track_5"]
    sub = p.sub_type

    if sub in ("sum_x_days", "count_x_days", "virtual_sum_x_days",
               "subscription_x_days_present", "subscription_x_days_absent",
               "multi_param"):
        dw = t5["dynamic_window"]
        date_col = get_date_col(p.table_name)

        if sub == "sum_x_days":
            return dw["template_sum"] \
                    .replace("{date_col}", date_col) \
                    .replace("{agg}", p.aggregation or "SUM") \
                    .replace("{kpi_col}", p.kpi_col)
        if sub == "count_x_days":
            return dw["template_count"] \
                    .replace("{date_col}", date_col) \
                    .replace("{count_col}", p.count_col)
        if sub == "virtual_sum_x_days":
            return dw["template_virtual_sum"] \
                    .replace("{date_col}", date_col) \
                    .replace("{agg}", p.aggregation or "SUM") \
                    .replace("{vp_name}", p.vp_name) \
                    .replace("{formula}", p.formula)
        if sub == "subscription_x_days_present":
            return dw["template_subscription_x_days_present"] \
                    .replace("{date_col}", date_col) \
                    .replace("{id_col}", p.id_col)
        if sub == "subscription_x_days_absent":
            return dw["template_subscription_x_days_absent"] \
                    .replace("{date_col}", date_col) \
                    .replace("{id_col}", p.id_col)
        if sub == "multi_param":
            return dw["template_multi_param"] \
                    .replace("{date_col}", date_col) \
                    .replace("{key_col}", p.action_key_col) \
                    .replace("{count_col}", p.count_col)

    cc = t5["campaign_check"]

    if sub == "bonus_not_sent_ak":
        cols = get_campaign_check_cols(p.table_name, sub)
        sent_date_col  = p.sent_date_col  or cols.get("sent_date_col", "")
        action_key_col = p.action_key_col or cols.get("action_key_col", "")
        msisdn_col     = p.msisdn_col     or cols.get("msisdn_col", "")
        return cc["template_bonus_not_sent_ak"] \
                .replace("{sent_date_col}", sent_date_col) \
                .replace("{action_key_col}", action_key_col) \
                .replace("{msisdn_col}", msisdn_col)
    if sub == "promo_sent_ak":
        cols = get_campaign_check_cols(p.table_name, sub)
        sent_date_col  = p.sent_date_col  or cols.get("sent_date_col", "")
        action_key_col = p.action_key_col or cols.get("action_key_col", "")
        msisdn_col     = p.msisdn_col     or cols.get("msisdn_col", "")
        return cc["template_promo_sent_ak"] \
                .replace("{sent_date_col}", sent_date_col) \
                .replace("{action_key_col}", action_key_col) \
                .replace("{msisdn_col}", msisdn_col)
    if sub == "promo_delivered_segment":
        return cc["template_promo_delivered_segment"] \
                .replace("{sent_date_col}", p.sent_date_col) \
                .replace("{action_type_col}", p.action_type_col) \
                .replace("{action_key_col}", p.action_key_col) \
                .replace("{msisdn_col}", p.msisdn_col)
    if sub == "promo_not_delivered_segment":
        return cc["template_promo_not_delivered_segment"] \
                .replace("{sent_date_col}", p.sent_date_col) \
                .replace("{action_type_col}", p.action_type_col) \
                .replace("{segment_col}", p.segment_col) \
                .replace("{msisdn_col}", p.msisdn_col)
    if sub == "bonus_not_delivered_segment":
        return cc["template_bonus_not_delivered_segment"] \
                .replace("{sent_date_col}", p.sent_date_col) \
                .replace("{action_type_col}", p.action_type_col) \
                .replace("{segment_col}", p.segment_col) \
                .replace("{msisdn_col}", p.msisdn_col)
    if sub == "bonus_nonresponder_ak":
        return cc["template_bonus_nonresponder_ak"] \
                .replace("{sent_date_col}", p.sent_date_col) \
                .replace("{action_type_col}", p.action_type_col) \
                .replace("{action_key_col}", p.action_key_col) \
                .replace("{msisdn_col}", p.msisdn_col)

    raise HTTPException(400, f"Unknown track 5 sub_type: {sub}")


# =============================================================================
# Track 6 Resolver
# =============================================================================

def resolve_track6(p: Track6Input) -> str:
    """
    Assemble Track 6 JOIN_CHECK condition from building blocks.
    Resolves join_col from YAML join_mappings, date_col from column_metadata.
    """
    parts = []

    # ── Part 1: JOIN (always present) ─────────────────────────────────────
    join_col = resolve_join_col(p.table_name, p.join_var)
    if not join_col:
        raise HTTPException(
            400,
            f"No join_mapping for variable '{p.join_var}' on table '{p.table_name}'. "
            f"Add it to column_metadata.{p.table_name}.join_mappings in the YAML."
        )
    parts.append(f"{join_col} = ${p.join_var}")

    # ── Part 2: CHECK (always present) ────────────────────────────────────
    parts.append(f"{p.check_col} ${{operator}} ${{value}}")

    # ── Part 3: DATE (optional) ───────────────────────────────────────────
    if p.date_range:
        date_col = get_date_col(p.table_name)
        if p.date_range.value is not None:
            parts.append(
                f"{date_col} {p.date_range.operator} "
                f"CurrentTime-{p.date_range.value}{p.date_range.unit}"
            )
        else:
            # No offset — e.g. "<= CurrentTime"
            parts.append(f"{date_col} {p.date_range.operator} CurrentTime")

    # ── Part 4: COUNT (optional) ──────────────────────────────────────────
    if p.count_check:
        count_col = p.check_col   # count on the same column being checked
        parts.append(
            f"COUNT_ALL({count_col}) {p.count_check.operator} {p.count_check.value}"
        )

    return " AND ".join(parts)


# =============================================================================
# Routes
# =============================================================================

@app.get("/health")
def health():
    return {"status": "ok", "yaml_loaded": bool(CONFIG), "version": "1.1.0"}


@app.get("/templates")
def list_templates():
    return {
        "track_1": list(TEMPLATES["track_1"].keys()),
        "track_2": list(TEMPLATES["track_2"].keys()),
        "track_3": list(TEMPLATES["track_3"].keys()),
        "track_4": list(TEMPLATES["track_4"].keys()),
        "track_5": list(TEMPLATES["track_5"].keys()),
        "column_metadata_tables": list(COLUMN_META.keys()),
    }


@app.post("/resolve")
def resolve(request: ResolveRequest):
    p = request.payload

    if p.track == 1:
        condition = resolve_track1(p)
    elif p.track == 2:
        condition = resolve_track2(p)
    elif p.track == 3:
        condition = resolve_track3(p)
    elif p.track == 4:
        condition = resolve_track4(p)
    elif p.track == 5:
        condition = resolve_track5(p)
    elif p.track == 6:
        condition = resolve_track6(p)
    else:
        raise HTTPException(400, f"Invalid track: {p.track}")

    # Clean up extra whitespace from multiline YAML templates
    condition = " ".join(condition.split())

    # ── FIX 1: Resolve and apply groupby suffix ──────────────────────────
    groupby_entity = getattr(p, "groupby_entity", None)
    if groupby_entity:
        table_name = getattr(p, "table_name", None)
        if table_name:
            groupby_cols = resolve_groupby_cols(table_name, groupby_entity)
            if groupby_cols:
                condition = _apply_groupby(condition, groupby_cols)
            else:
                logger.warning(
                    "No groupby mapping for entity '%s' on table '%s'",
                    groupby_entity, table_name
                )

    return {
        "track": p.track,
        "parent_condition": condition,
        "note": "Append ${operator} ${value} at the end if not already present."
    }


# =============================================================================
# Sample Payloads
# =============================================================================

@app.get("/examples")
def examples():
    return {
        "track_1_fixed_month": {
            "payload": {
                "track": 1, "table_name": "COMMON_Seg_Fct",
                "kpi_col": "COMMON_Total_Revenue", "aggregation": "SUM",
                "time_window": {"type": "FIXED_MONTH", "value": 1, "unit": "MONTH"},
                "is_composite": False
            }
        },
        "track_2_count_groupby_only": {
            "payload": {
                "track": 2, "table_name": "AIRTEL_LIFECYCLE_CDR",
                "sub_type": "count_groupby_only",
                "count_col": "LC_ACTION_KEY",
                "groupby_entity": "subscriber"
            }
        },
        "track_2_count_flag_absent_today": {
            "payload": {
                "track": 2, "table_name": "GASSO",
                "sub_type": "count_flag_absent_today",
                "flag_col": "GASSO_ACTION_KEY",
                "count_col": "GASSO_ACTION_KEY",
                "null_guard_col": "GASSO_SSO_MSISDN",
                "groupby_entity": "subscriber"
            }
        },
        "track_2_date_value_count": {
            "payload": {
                "track": 2, "table_name": "AIRTEL_RECHARGE",
                "sub_type": "date_value_count",
                "count_col": "RE_REFILL_TYPE",
                "count_threshold_op": ">", "count_threshold_val": "0",
                "groupby_entity": "product"
            }
        },
        "track_5_promo_sent_groupby": {
            "payload": {
                "track": 5, "table_name": "LIFECYCLE_PROMO",
                "sub_type": "promo_sent_ak",
                "sent_date_col": "L_PROMO_SENT_DATE",
                "action_key_col": "L_ACTION_KEY",
                "msisdn_col": "L_AGG_MSISDN",
                "groupby_entity": "action_date"
            }
        },
    }
