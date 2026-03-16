"""
Virtual Profile Rule Engine — FastAPI Template Engine
======================================================
Receives structured JSON (output from track-specific extraction agents)
and returns the filled PARENT_CONDITION rule string.

Run:
    pip install fastapi uvicorn pyyaml
    uvicorn vp_template_engine_api:app --reload --port 8000

POST /resolve  →  returns filled PARENT_CONDITION
GET  /templates →  lists all template keys
GET  /health    →  health check
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Literal, List
import yaml
import os

# =============================================================================
# App Init + YAML Load
# =============================================================================

app = FastAPI(
    title="VP Rule Engine — Template API",
    description="Resolves track agent JSON into PARENT_CONDITION rule strings",
    version="1.0.0"
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
    type: Literal["ROLLING_WEEK", "FIXED_MONTH", "LAST_N", "MTD", "LMTD"]
    value: Optional[int] = None
    unit: Optional[Literal["DAY", "WEEK", "MONTH"]] = None


class Track1Input(BaseModel):
    track: Literal[1]
    table_name: str
    kpi_col: str
    aggregation: Literal["AVG", "SUM", "COUNT", "MAX", "MIN"]
    time_window: TimeWindow
    is_composite: bool = False
    # Optional: virtual formula (e.g. "col1+col2" or "col/3")
    formula: Optional[str] = None
    vp_name: Optional[str] = None
    # Optional: dimension filter (e.g. dpi_app_usage_protocol = Streaming)
    filter_col: Optional[str] = None
    filter_val: Optional[str] = None


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
        "status_count_zero"
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
    # For multi_or_notnull
    col_list: Optional[List[str]] = None
    # Time window (for within_n_days and threshold variants)
    N: Optional[int] = None
    threshold: Optional[int] = None
    is_composite: bool = False


class Track3Input(BaseModel):
    track: Literal[3]
    table_name: str
    sub_type: Literal[
        "snapshot_by_id",
        "snapshot_max_check",
        "snapshot_by_date_boundary",
        "geo_current",
        "geo_last_n_days"
    ]
    id_col: Optional[str] = None
    value_col: Optional[str] = None
    ref_col: Optional[str] = None
    count_col: Optional[str] = None
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
    vp_a: str       # base VP name (earlier period / denominator)
    vp_b: str       # comparison VP name (later period / numerator)
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


# Union input for the single /resolve endpoint
from typing import Union
from pydantic import Field

class ResolveRequest(BaseModel):
    payload: Union[Track1Input, Track2Input, Track3Input, Track4Input, Track5Input] = Field(
        ..., discriminator="track"
    )


# =============================================================================
# Column Metadata Resolver
# =============================================================================

def _normalize_identifier(value: str) -> str:
    """Normalize identifiers for case-insensitive equality checks."""
    return value.strip().lower()


def _get_table_meta(table_name: str) -> Optional[dict]:
    """Resolve table metadata with exact match first, then case-insensitive match."""
    meta = COLUMN_META.get(table_name)
    if meta:
        return meta

    normalized = _normalize_identifier(table_name)
    for key, value in COLUMN_META.items():
        if _normalize_identifier(key) == normalized:
            return value
    return None


def get_date_col(table_name: str) -> str:
    """Resolve date_col from column_metadata using table_name."""
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
            # W1 special pattern
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

        # AVG over last N months
        if tw.unit == "MONTH" and p.vp_name:
            tmpl = ln["template_months_avg"]
            return tmpl.replace("{date_col}", date_col) \
                       .replace("{N}", n) \
                       .replace("{vp_name}", p.vp_name) \
                       .replace("{kpi_col}", p.kpi_col)

        # No date filter (bare SUM)
        if not date_col or date_col == "null":
            if p.formula and p.vp_name:
                tmpl = ln["template_no_date_virtual"]
                return tmpl.replace("{agg}", p.aggregation) \
                           .replace("{vp_name}", p.vp_name) \
                           .replace("{formula}", p.formula)
            tmpl = ln["template_no_date"]
            return tmpl.replace("{kpi_col}", p.kpi_col) \
                       .replace("{agg}", p.aggregation)

        # With dimension filter
        if p.filter_col and p.filter_val:
            tmpl = ln["template_days_with_filter"]
            return tmpl.replace("{date_col}", date_col) \
                       .replace("{N}", n) \
                       .replace("{filter_col}", p.filter_col) \
                       .replace("{filter_val}", p.filter_val) \
                       .replace("{agg}", p.aggregation) \
                       .replace("{kpi_col}", p.kpi_col)

        # Closed range (explicit upper bound)
        if p.formula and p.vp_name:
            tmpl = ln["template_no_date_virtual"]
            return tmpl.replace("{agg}", p.aggregation) \
                       .replace("{vp_name}", p.vp_name) \
                       .replace("{formula}", p.formula)

        # Default: open range
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

    # ── Flag / Existence sub-types ────────────────────────────────────────────
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

    # ── Dynamic Window sub-types ──────────────────────────────────────────────
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

    # ── Campaign Check sub-types ──────────────────────────────────────────────
    cc = t5["campaign_check"]

    if sub == "bonus_not_sent_ak":
        return cc["template_bonus_not_sent_ak"] \
                .replace("{sent_date_col}", p.sent_date_col) \
                .replace("{action_key_col}", p.action_key_col) \
                .replace("{msisdn_col}", p.msisdn_col)

    if sub == "promo_sent_ak":
        return cc["template_promo_sent_ak"] \
                .replace("{sent_date_col}", p.sent_date_col) \
                .replace("{action_key_col}", p.action_key_col) \
                .replace("{msisdn_col}", p.msisdn_col)

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
# Routes
# =============================================================================

@app.get("/health")
def health():
    return {"status": "ok", "yaml_loaded": bool(CONFIG)}


@app.get("/templates")
def list_templates():
    """Return all template keys available in the YAML config."""
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
    """
    Receives track agent JSON and returns the filled PARENT_CONDITION string.

    Example input (Track 1 Fixed Month):
    {
      "payload": {
        "track": 1,
        "table_name": "COMMON_Seg_Fct",
        "kpi_col": "COMMON_Total_Revenue",
        "aggregation": "SUM",
        "time_window": { "type": "FIXED_MONTH", "value": 1, "unit": "MONTH" },
        "is_composite": false
      }
    }
    """
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
    else:
        raise HTTPException(400, f"Invalid track: {p.track}")

    # Clean up extra whitespace from multiline YAML templates
    condition = " ".join(condition.split())

    return {
        "track": p.track,
        "parent_condition": condition,
        "note": "Append ${operator} ${value} at the end if not already present."
    }


# =============================================================================
# Sample Payloads (printed on startup for quick testing)
# =============================================================================

@app.get("/examples")
def examples():
    """Returns sample request payloads for each track for quick testing."""
    return {
        "track_1_fixed_month": {
            "payload": {
                "track": 1,
                "table_name": "COMMON_Seg_Fct",
                "kpi_col": "COMMON_Total_Revenue",
                "aggregation": "SUM",
                "time_window": {"type": "FIXED_MONTH", "value": 1, "unit": "MONTH"},
                "is_composite": False
            }
        },
        "track_1_rolling_week": {
            "payload": {
                "track": 1,
                "table_name": "COMMON_Seg_Fct",
                "kpi_col": "COMMON_OG_Call_Revenue",
                "aggregation": "SUM",
                "time_window": {"type": "ROLLING_WEEK", "value": 3, "unit": "WEEK"},
                "is_composite": False
            }
        },
        "track_1_last_n_days": {
            "payload": {
                "track": 1,
                "table_name": "COMMON_Seg_Fct",
                "kpi_col": "COMMON_Data_Revenue",
                "aggregation": "SUM",
                "time_window": {"type": "LAST_N", "value": 30, "unit": "DAY"},
                "is_composite": False
            }
        },
        "track_1_mtd": {
            "payload": {
                "track": 1,
                "table_name": "COMMON_Seg_Fct",
                "kpi_col": "COMMON_Prepay_Voice_Revenue",
                "aggregation": "SUM",
                "time_window": {"type": "MTD", "value": None, "unit": None},
                "is_composite": False
            }
        },
        "track_2_subscribed": {
            "payload": {
                "track": 2,
                "table_name": "SUBSCRIPTIONS",
                "sub_type": "subscribed",
                "id_col": "SUBSCRIPTIONS_Product_Id",
                "is_composite": False
            }
        },
        "track_2_flag": {
            "payload": {
                "track": 2,
                "table_name": "BILL_PAYMENT",
                "sub_type": "count_flag_present",
                "flag_col": "BILL_IS_PAID_MSISDN_PRO",
                "count_col": "BILL_IS_PAID_MSISDN_PRO",
                "is_composite": False
            }
        },
        "track_3_geo": {
            "payload": {
                "track": 3,
                "table_name": "GEO_LOCATION_STATIC",
                "sub_type": "geo_current",
                "lon_col": "LOCATION_LONGITUDE",
                "lat_col": "LOCATION_LATITUDE",
                "geo_name_col": "GEO_LOCATION_NAME",
                "is_composite": False
            }
        },
        "track_4_pct_drop": {
            "payload": {
                "track": 4,
                "operation": "PERCENTAGE_DROP",
                "vp_a": "M1_TOTAL_REVENUE",
                "vp_b": "M2_TOTAL_REVENUE",
                "is_composite": True
            }
        },
        "track_5_dynamic": {
            "payload": {
                "track": 5,
                "table_name": "Recharge_Seg_Fact",
                "sub_type": "sum_x_days",
                "kpi_col": "RECHARGE_Denomination",
                "aggregation": "SUM",
                "is_composite": False
            }
        },
        "track_5_campaign": {
            "payload": {
                "track": 5,
                "table_name": "LOYALTY_BONUS",
                "sub_type": "bonus_not_sent_ak",
                "sent_date_col": "L_BONUS_SENT_DATE",
                "action_key_col": "L_ACTION_KEY",
                "msisdn_col": "L_AGG_MSISDN",
                "is_composite": False
            }
        }
    }
