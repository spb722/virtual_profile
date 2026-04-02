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
"""

import logging
import requests

from agents import Track1Output, Track2Output, Track3Output, Track5Output, Track6Output
from kpi_mapper import resolve_kpi

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

def build_track1_payload(extracted: Track1Output) -> dict:
    """
    Convert Track1Output → template engine request body.
    Resolves kpi text → table_name + kpi_col via KPI mapper.
    date_col is auto-resolved inside the template engine from table_name.
    """
    kpi_info = resolve_kpi(extracted.kpi, extracted.aggregation)
    tw       = extracted.time_window
    return {
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


def build_track2_payload(extracted: Track2Output) -> dict:
    """
    Track 2 static flags.
    Now passes through groupby_entity if the agent extracted one.
    """
    kpi_info = resolve_kpi(extracted.kpi)
    payload = {
        "table_name":     kpi_info["table_name"],
        "sub_type":       _map_state_to_subtype(extracted.expected_state),
        "flag_col":       kpi_info["kpi_col"],
        "count_col":      kpi_info["kpi_col"],
        "is_composite":   extracted.is_composite
    }
    # ── FIX 1: pass groupby_entity if present ─────────────────────────────
    if extracted.groupby_entity:
        payload["groupby_entity"] = extracted.groupby_entity
    return payload


def build_track3_payload(extracted: Track3Output) -> dict:
    """
    Build Track 3 snapshot payload.
    """
    kpi_info = resolve_kpi(extracted.kpi)
    return {
        "table_name":   kpi_info["table_name"],
        "sub_type":     "snapshot_by_id",
        "value_col":    kpi_info["kpi_col"],
        "id_col":       kpi_info["kpi_col"],
        "is_composite": extracted.is_composite
    }


def build_track5_payload(extracted: Track5Output) -> dict:
    """
    Build Track 5 parameterized payload.
    Now passes through groupby_entity if the agent extracted one.
    """
    kpi_info = resolve_kpi(extracted.kpi, extracted.aggregation)
    payload = {
        "table_name":  kpi_info["table_name"],
        "sub_type":    "sum_x_days",
        "kpi_col":     kpi_info["kpi_col"],
        "aggregation": extracted.aggregation,
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
        "ASSIGNED":       "segment_type",
    }
    return mapping.get(expected_state.upper(), "attr_check")