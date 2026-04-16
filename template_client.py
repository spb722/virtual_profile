"""
template_client.py
------------------
Calls the VP Template Engine FastAPI at TEMPLATE_URL.
Builds track-specific payloads from extracted agent output.

Note: Track 4 never calls this directly —
      it composes its formula from resolved child VP names.

FIX 1 CHANGES:
  - build_track2_payload: passes groupby_entity through to template engine
  - build_track5_payload: passes groupby_entity through to template engine
  - build_track5_payload: routes parameterized subscription checks to the
    Track 5 subscription templates instead of the generic metric template

FIX 2 CHANGES:
  - build_track5_payload: routes bonus/promo campaign checks to the dedicated
    Track 5 campaign templates (bonus_not_sent_ak, promo_sent_ak, etc.)
  - _resolve_campaign_columns: reads column names from YAML campaign_check_mappings
    and passes them directly in the payload (single source of truth in YAML)
"""

import logging
import os

import requests
import yaml

from agents import Track1Output, Track2Output, Track3Output, Track5Output, Track6Output
from kpi_mapper import resolve_kpi, resolve_kpi_list

# Load YAML column metadata once at startup (same file the template engine uses)
_YAML_PATH = os.path.join(os.path.dirname(__file__), "vp_template_engine.yaml")
with open(_YAML_PATH, "r") as _f:
    _COLUMN_META = yaml.safe_load(_f).get("column_metadata", {})

logger = logging.getLogger(__name__)

TEMPLATE_URL = "http://localhost:9978/resolve"
TIMEOUT      = 15   # seconds


def call_template_engine(track: int, payload: dict) -> str:
    """
    POST to the template engine and return the filled parent_condition string.
    Returns an error string (never raises) so the caller can continue.
    """
    body = {"payload": {**payload, "track": track}}
    logger.debug("Template engine request: %s", body)
    try:
        resp = requests.post(TEMPLATE_URL, json=body, timeout=TIMEOUT)
        if not resp.ok:
            try:
                detail = resp.json().get("detail", resp.text)
            except Exception:
                detail = resp.text
            msg = f"[TEMPLATE_ENGINE_ERROR {resp.status_code}: {detail}]"
            logger.error(msg)
            return msg
        result = resp.json()
        logger.debug("Template engine response: %s", result)
        return result.get("parent_condition", "[EMPTY RESPONSE FROM TEMPLATE ENGINE]")
    except requests.exceptions.ConnectionError:
        msg = f"[TEMPLATE_ENGINE_UNREACHABLE: {TEMPLATE_URL}]"
        logger.error(msg)
        return msg
    except requests.exceptions.Timeout:
        msg = "[TEMPLATE_ENGINE_TIMEOUT]"
        logger.error(msg)
        return msg
    except Exception as e:
        msg = f"[TEMPLATE_ENGINE_ERROR: {e}]"
        logger.error(msg)
        return msg


# ─────────────────────────────────────────────────────────────────────────────
# Payload Builders — one per leaf track
# ─────────────────────────────────────────────────────────────────────────────

def build_track1_payload(extracted: Track1Output, vp_name: str = None) -> dict:
    """
    Convert Track1Output → template engine request body.
    Resolves kpi text → table_name + kpi_col via KPI mapper.
    date_col is auto-resolved inside the template engine from table_name.

    Multi-KPI path: when extracted.kpi_list is set, all KPIs are resolved in
    one batch call. A formula string and null_guard_cols list are built and
    passed to the template engine so it can render the virtual-KPI template.
    """
    tw = extracted.time_window

    # ── Multi-KPI formula path ────────────────────────────────────────────────
    if extracted.kpi_list:
        resolved_cols = resolve_kpi_list(extracted.kpi_list, extracted.aggregation)
        op            = extracted.formula_op or "+"
        formula       = op.join(r["kpi_col"] for r in resolved_cols)
        null_guard_cols = [r["kpi_col"] for r in resolved_cols]
        table_name    = resolved_cols[0]["table_name"]

        payload = {
            "table_name":      table_name,
            "kpi_col":         null_guard_cols[0],   # required by Track1Input; formula takes precedence
            "formula":         formula,
            "null_guard_cols": null_guard_cols,
            "aggregation":     extracted.aggregation,
            "time_window": {
                "type":  tw.type,
                "value": tw.value,
                "unit":  tw.unit,
            },
            "is_composite": extracted.is_composite,
        }
        if vp_name:
            payload["vp_name"] = vp_name
        return payload

    # ── Single-KPI path (unchanged) ───────────────────────────────────────────
    kpi_info = resolve_kpi(extracted.kpi, extracted.aggregation)
    payload = {
        "table_name":  kpi_info["table_name"],
        "kpi_col":     kpi_info["kpi_col"],
        "aggregation": extracted.aggregation,
        "time_window": {
            "type":  tw.type,
            "value": tw.value,
            "unit":  tw.unit
        },
        "is_composite": extracted.is_composite
    }
    if vp_name:
        payload["vp_name"] = vp_name
    if extracted.filter_col:
        try:
            filter_info = resolve_kpi(extracted.filter_col)
            payload["filter_col"] = filter_info["kpi_col"]
        except Exception:
            logger.warning("Could not resolve filter_col '%s' via KPI mapper; using raw text", extracted.filter_col)
            payload["filter_col"] = extracted.filter_col
    if extracted.filter_values:
        payload["filter_values"] = ";".join(extracted.filter_values)
    return payload


def build_track2_payload(extracted: Track2Output) -> dict:
    """
    Track 2 static flags.
    Routes subscription sub_types to id_col-based templates.
    If a time_constraint is present on a subscription check, switches to the
    timed variant (subscribed_within_n_days / not_subscribed_within_n_days).
    Passes groupby_entity through if extracted.
    """
    kpi_info = resolve_kpi(extracted.kpi)
    promo_payload = _build_track2_fixed_promo_absence_payload(extracted, kpi_info)
    if promo_payload:
        payload = promo_payload
    elif _build_track2_promo_presence_payload(extracted, kpi_info):
        payload = _build_track2_promo_presence_payload(extracted, kpi_info)
    else:
        base_sub_type = _map_state_to_subtype(extracted.expected_state)

        # ── "at most N times" threshold branch ───────────────────────────
        if extracted.threshold is not None and base_sub_type == "subscribed":
            payload = {
                "table_name":   kpi_info["table_name"],
                "sub_type":     "subscription_threshold",
                "id_col":       kpi_info["kpi_col"],
                "threshold":    extracted.threshold,
                "is_composite": extracted.is_composite,
            }
            if extracted.time_constraint:
                payload["N"] = extracted.time_constraint.value

        # ── Subscription + time window → timed variant ────────────────────
        elif base_sub_type in ("not_subscribed", "subscribed") and extracted.time_constraint:
            timed_sub_type = (
                "not_subscribed_within_n_days"
                if base_sub_type == "not_subscribed"
                else "subscribed_within_n_days"
            )
            payload = {
                "table_name":   kpi_info["table_name"],
                "sub_type":     timed_sub_type,
                "id_col":       kpi_info["kpi_col"],
                "N":            extracted.time_constraint.value,
                "is_composite": extracted.is_composite,
            }
        # ── Plain subscription (no time window, no threshold) ─────────────
        elif base_sub_type in ("not_subscribed", "subscribed"):
            payload = {
                "table_name":   kpi_info["table_name"],
                "sub_type":     base_sub_type,
                "id_col":       kpi_info["kpi_col"],
                "is_composite": extracted.is_composite,
            }
        # ── All other Track 2 sub_types — unchanged ───────────────────────
        else:
            payload = {
                "table_name":   kpi_info["table_name"],
                "sub_type":     base_sub_type,
                "flag_col":     kpi_info["kpi_col"],
                "count_col":    kpi_info["kpi_col"],
                "is_composite": extracted.is_composite,
            }

    # ── FIX 1: pass groupby_entity if present ─────────────────────────────
    if extracted.groupby_entity:
        payload["groupby_entity"] = extracted.groupby_entity
    return payload


def build_track3_payload(extracted: Track3Output) -> dict:
    """
    Build Track 3 snapshot payload.
    Routes to the correct sub_type based on what the LLM extracted.
    """
    kpi_info = resolve_kpi(extracted.kpi)
    sub_type = extracted.sub_type or "snapshot_by_id"

    if sub_type == "geo_last_n_days":
        return {
            "table_name":   kpi_info["table_name"],
            "sub_type":     "geo_last_n_days",
            "N":            extracted.N,
            "region_col":   kpi_info["kpi_col"],
            "msisdn_col":   get_msisdn_col(kpi_info["table_name"]),
            "is_composite": extracted.is_composite,
        }

    if sub_type == "geo_last_n_months":
        return {
            "table_name":   kpi_info["table_name"],
            "sub_type":     "geo_last_n_months",
            "N":            extracted.N,
            "region_col":   kpi_info["kpi_col"],
            "msisdn_col":   get_msisdn_col(kpi_info["table_name"]),
            "is_composite": extracted.is_composite,
        }

    if sub_type == "geo_current":
        return {
            "table_name":   kpi_info["table_name"],
            "sub_type":     "geo_current",
            "region_col":   kpi_info["kpi_col"],
            "is_composite": extracted.is_composite,
        }

    if sub_type == "snapshot_max_check":
        return {
            "table_name":   kpi_info["table_name"],
            "sub_type":     "snapshot_max_check",
            "id_col":       kpi_info["kpi_col"],
            "ref_col":      kpi_info["kpi_col"],
            "is_composite": extracted.is_composite,
        }

    if sub_type == "snapshot_by_date_boundary":
        return {
            "table_name":   kpi_info["table_name"],
            "sub_type":     "snapshot_by_date_boundary",
            "N":            extracted.N,
            "id_col":       kpi_info["kpi_col"],
            "count_col":    kpi_info["kpi_col"],
            "is_composite": extracted.is_composite,
        }

    if sub_type == "snapshot_null_zero_max":
        return {
            "table_name":   kpi_info["table_name"],
            "sub_type":     "snapshot_null_zero_max",
            "N":            extracted.N,
            "kpi_col":      kpi_info["kpi_col"],
            "is_composite": extracted.is_composite,
        }

    # default — snapshot_by_id
    return {
        "table_name":   kpi_info["table_name"],
        "sub_type":     "snapshot_by_id",
        "id_col":       extracted.id_col or kpi_info["kpi_col"],
        "value_col":    kpi_info["kpi_col"],
        "is_composite": extracted.is_composite,
    }


def get_msisdn_col(table_name: str) -> str:
    """
    Look up the MSISDN column for a table from YAML column_metadata.kpi_examples.
    Falls back to "MSISDN" if not found.
    """
    meta = _COLUMN_META.get(table_name, {})
    for col in meta.get("kpi_examples", []):
        if "msisdn" in col.lower():
            return col
    return "MSISDN"


def build_track5_payload(extracted: Track5Output) -> dict:
    """
    Build Track 5 parameterized payload.
    Priority order:
      1. Subscription checks   → subscription_x_days_present / absent
      2. Campaign/bonus checks → bonus_not_sent_ak / promo_sent_ak / promo_not_delivered_segment
         (column names read from YAML campaign_check_mappings and included in payload)
      3. Generic COUNT         → count_x_days
      4. Generic SUM/other     → sum_x_days
    """
    kpi_info = resolve_kpi(extracted.kpi, extracted.aggregation)
    subscription_sub_type = _infer_track5_subscription_subtype(extracted, kpi_info)

    if subscription_sub_type:
        payload = {
            "table_name":   kpi_info["table_name"],
            "sub_type":     subscription_sub_type,
            "id_col":       kpi_info["kpi_col"],
            "is_composite": extracted.is_composite
        }
    else:
        campaign_sub_type = _infer_track5_campaign_subtype(extracted, kpi_info)
        if campaign_sub_type:
            cols = _resolve_campaign_columns(kpi_info["table_name"], campaign_sub_type)
            payload = {
                "table_name":     kpi_info["table_name"],
                "sub_type":       campaign_sub_type,
                "sent_date_col":  cols["sent_date_col"],
                "action_key_col": cols["action_key_col"],
                "msisdn_col":     cols["msisdn_col"],
                "is_composite":   extracted.is_composite,
            }
        elif str(extracted.aggregation or "").upper() == "COUNT":
            payload = {
                "table_name":   kpi_info["table_name"],
                "sub_type":     "count_x_days",
                "count_col":    kpi_info["kpi_col"],
                "is_composite": extracted.is_composite
            }
        else:
            payload = {
                "table_name":   kpi_info["table_name"],
                "sub_type":     "sum_x_days",
                "kpi_col":      kpi_info["kpi_col"],
                "aggregation":  extracted.aggregation,
                "is_composite": extracted.is_composite
            }
    # ── FIX 1: pass groupby_entity if present ─────────────────────────────
    if extracted.groupby_entity:
        payload["groupby_entity"] = extracted.groupby_entity
    return payload


def build_track6_payload(extracted: Track6Output) -> dict:
    """
    Build Track 6 join-check payload.
    Resolves kpi via KPI mapper to get table_name + check_col.
    Passes join_var, date_range, count_check, groupby_entity through.
    """
    kpi_info = resolve_kpi(extracted.kpi)
    payload = {
        "table_name":   kpi_info["table_name"],
        "check_col":    kpi_info["kpi_col"],
        "join_var":     extracted.join_var,
        "is_composite": extracted.is_composite
    }
    if extracted.date_range:
        payload["date_range"] = {
            "operator": extracted.date_range.operator,
            "value":    extracted.date_range.value,
            "unit":     extracted.date_range.unit,
        }
    if extracted.count_check:
        payload["count_check"] = {
            "operator": extracted.count_check.operator,
            "value":    extracted.count_check.value,
        }
    if extracted.groupby_entity:
        payload["groupby_entity"] = extracted.groupby_entity
    return payload


# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────

def _map_state_to_subtype(expected_state: str) -> str:
    mapping = {
        "EXISTS":         "count_flag_present",
        "NOT_EXISTS":     "count_flag_absent",
        "SUBSCRIBED":     "subscribed",
        "NOT_SUBSCRIBED": "not_subscribed",
        "TRUE":           "count_flag_present",
        "FALSE":          "count_flag_absent",
        "ASSIGNED":       "attr_check",
    }
    return mapping.get(expected_state.upper(), "attr_check")


def _build_track2_fixed_promo_absence_payload(extracted: Track2Output, kpi_info: dict) -> dict | None:
    """
    Route fixed-window promo absence checks through a dedicated Track 2 branch.
    This is intentionally narrow so the rest of Track 2 keeps its existing
    behavior.
    """
    if str(kpi_info.get("table_name", "") or "").strip() != "LIFECYCLE_PROMO":
        return None
    if str(extracted.expected_state or "").upper() != "NOT_EXISTS":
        return None

    time_constraint = extracted.time_constraint
    if not time_constraint:
        return None
    if time_constraint.type == "TODAY":
        n_days = 0
    elif time_constraint.type == "LAST_N_DAYS" and time_constraint.value is not None:
        n_days = time_constraint.value
    else:
        return None

    return {
        "table_name":   kpi_info["table_name"],
        "sub_type":     "campaign_absent_fixed_days",
        "flag_col":     kpi_info["kpi_col"],
        "count_col":    "L_AGG_MSISDN",
        "N":            n_days,
        "is_composite": extracted.is_composite,
    }


def _build_track2_promo_presence_payload(extracted: Track2Output, kpi_info: dict) -> dict | None:
    """
    Route fixed-window promo PRESENCE checks (EXISTS + LIFECYCLE_PROMO/LIFECYCLE_CDR + time_constraint)
    to campaign_present_fixed_days template.
    Accepts both LIFECYCLE_PROMO and LIFECYCLE_CDR — KPI mapper may return either.
    Always forces table_name to LIFECYCLE_PROMO so template engine uses L_PROMO_SENT_DATE as date_col.
    """
    if str(kpi_info.get("table_name", "") or "").strip() not in ("LIFECYCLE_PROMO", "LIFECYCLE_CDR"):
        return None
    if str(extracted.expected_state or "").upper() != "EXISTS":
        return None
    time_constraint = extracted.time_constraint
    if not time_constraint:
        return None
    if time_constraint.type == "LAST_N_DAYS" and time_constraint.value is not None:
        n_days = time_constraint.value
    else:
        return None
    cols = _COLUMN_META.get("LIFECYCLE_PROMO", {}).get("campaign_check_mappings", {}).get("campaign_present_fixed_days", {})
    return {
        "table_name":      "LIFECYCLE_PROMO",
        "sub_type":        "campaign_present_fixed_days",
        "flag_col":        cols.get("flag_col", kpi_info["kpi_col"]),
        "action_type_col": cols.get("action_type_col", "LC_ACTION_TYPE"),
        "count_col":       cols.get("count_col", "L_AGG_MSISDN"),
        "N":               n_days,
        "is_composite":    extracted.is_composite,
    }


def _infer_track5_subscription_subtype(extracted: Track5Output, kpi_info: dict) -> str | None:
    """
    Route parameterized subscription checks to the dedicated Track 5 templates.
    This keeps the existing generic Track 5 flow unchanged for non-subscription
    metrics such as recharge revenue, bonus history, or generic counts.
    """
    if not _looks_like_subscription_target(kpi_info):
        return None

    text = " ".join(
        part.strip().lower()
        for part in (
            extracted.kpi or "",
            str(kpi_info.get("matched_condition", "") or ""),
        )
        if part
    )

    negative_markers = (
        "not subscribed",
        "non subscribed",
        "non-subscribed",
        "unsubscribed",
        "without subscription",
        "no subscription",
    )
    positive_markers = (
        "currently subscribed",
        "is subscribed",
        "are subscribed",
        "subscribed customer",
        "subscribed customers",
        "subscribed to product",
        "active subscription",
        "active product subscription",
    )

    if any(marker in text for marker in negative_markers):
        return "subscription_x_days_absent"
    if (
        ("subscription" in text or "subscribed" in text)
        and ("without" in text or " no " in f" {text} ")
    ):
        return "subscription_x_days_absent"
    if any(marker in text for marker in positive_markers):
        return "subscription_x_days_present"
    if "subscription" in text or "subscribed" in text:
        return "subscription_x_days_present"
    return None


def _looks_like_subscription_target(kpi_info: dict) -> bool:
    table_name = str(kpi_info.get("table_name", "") or "").strip().lower()
    kpi_col = str(kpi_info.get("kpi_col", "") or "").strip().lower()
    return (
        "subscription" in table_name
        or "subscription" in kpi_col
        or "product_id" in kpi_col
    )


def _infer_track5_campaign_subtype(extracted: Track5Output, kpi_info: dict) -> str | None:
    """
    Detect bonus/promo campaign patterns from the extracted KPI text and return
    the right sub_type.  Table name is used as a guard — only LIFECYCLE tables
    enter this path.
    """
    table_name = str(kpi_info.get("table_name", "") or "").strip().upper()
    if table_name not in ("LIFECYCLE_PROMO", "LIFECYCLE_BONUS"):
        return None

    text = (extracted.kpi or "").lower()

    # Bonus conditions
    if "bonus" in text:
        if "not sent" in text or "not received" in text or "not delivered" in text:
            return "bonus_not_sent_ak"
        return "promo_sent_ak"   # bonus received / sent → same template family

    # Promo conditions
    if "promo" in text:
        if "not sent" in text or "not delivered" in text:
            return "promo_not_delivered_segment"
        return "promo_sent_ak"

    return None


def _resolve_campaign_columns(table_name: str, sub_type: str) -> dict:
    """
    Pull campaign check column names from YAML column_metadata.campaign_check_mappings.
    Raises ValueError if the mapping is missing so failures are loud and obvious.
    """
    # Case-insensitive table lookup
    meta = None
    for key, val in _COLUMN_META.items():
        if key.upper() == table_name.upper():
            meta = val
            break
    if not meta:
        raise ValueError(f"Table '{table_name}' not found in column_metadata")
    cols = meta.get("campaign_check_mappings", {}).get(sub_type)
    if not cols:
        raise ValueError(
            f"No campaign_check_mapping for sub_type '{sub_type}' on table '{table_name}'"
        )
    return cols
