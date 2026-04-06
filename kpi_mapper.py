"""
kpi_mapper.py
-------------
Resolves natural language KPI descriptions through the VP verification API.

The API response replaces the old hard-coded KPI_MAP by returning:
- table_name
- kpi (treated in this project as the KPI column name)
"""

import logging
import os

import requests
import urllib3

logger = logging.getLogger(__name__)

VP_VERIFY_URL = os.environ.get(
    "VP_VERIFY_URL",
    "http://localhost:5678/webhook/VP_verify",
)
VP_VERIFY_TIMEOUT = float(os.environ.get("VP_VERIFY_TIMEOUT", "15"))
VP_VERIFY_SSL_VERIFY = (
    os.environ.get("VP_VERIFY_SSL_VERIFY", "false").strip().lower()
    in {"1", "true", "yes"}
)

if not VP_VERIFY_SSL_VERIFY:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_LOOKUP_CACHE: dict[str, dict] = {}


class KPIResolutionError(RuntimeError):
    """Raised when KPI lookup fails or returns an unusable response."""


def _normalize_key(kpi_text: str) -> str:
    return kpi_text.strip().lower()


def _extract_first_match(payload: dict, kpi_text: str) -> dict:
    output = payload.get("output")
    if not isinstance(output, dict):
        raise KPIResolutionError(
            f"KPI lookup failed for '{kpi_text}': response is missing 'output'."
        )

    matches = output.get("matches")
    if not isinstance(matches, list) or not matches:
        unmatched = output.get("unmatched", [])
        logger.warning(
            "No KPI/profile found via API for '%s'. unmatched=%s",
            kpi_text,
            unmatched,
        )
        detail = f"No KPI/profile found via API for '{kpi_text}'."
        if unmatched:
            detail = f"{detail} unmatched={unmatched}"
        raise KPIResolutionError(
            detail
        )

    first = matches[0]
    if not isinstance(first, dict):
        raise KPIResolutionError(
            f"KPI lookup failed for '{kpi_text}': match entry is malformed."
        )

    table_name = first.get("table_name")
    kpi_col = first.get("kpi")

    if not table_name or not kpi_col:
        raise KPIResolutionError(
            f"KPI lookup failed for '{kpi_text}': match is missing 'table_name' or 'kpi'."
        )

    return {
        "table_name": table_name,
        "kpi_col": kpi_col,
        "datatype": first.get("datatype"),
        "matched_condition": first.get("condition"),
    }


def resolve_kpi(kpi_text: str, aggregation: str = "SUM") -> dict:
    """
    Resolve a natural language KPI description to
    { table_name, kpi_col, aggregation } using the VP verification API.
    """
    key = _normalize_key(kpi_text)
    if not key:
        raise ValueError("kpi_text cannot be empty.")

    cached = _LOOKUP_CACHE.get(key)
    if cached:
        return {**cached, "aggregation": aggregation}

    body = {
        "conditions": [kpi_text.strip()],
        "check": False,
    }
    logger.info("Resolving KPI via API: '%s'", kpi_text)

    try:
        resp = requests.post(
            VP_VERIFY_URL,
            json=body,
            timeout=VP_VERIFY_TIMEOUT,
            verify=VP_VERIFY_SSL_VERIFY,
        )
        resp.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise KPIResolutionError(
            f"KPI lookup timed out after {VP_VERIFY_TIMEOUT}s"
        ) from exc
    except requests.exceptions.SSLError as exc:
        raise KPIResolutionError(
            "KPI lookup failed SSL verification. "
            "Set VP_VERIFY_SSL_VERIFY=true if the endpoint has a trusted certificate."
        ) from exc
    except requests.exceptions.RequestException as exc:
        raise KPIResolutionError(f"KPI lookup request failed: {exc}") from exc

    try:
        payload = resp.json()
    except ValueError as exc:
        raise KPIResolutionError("KPI lookup returned non-JSON response.") from exc

    resolved = _extract_first_match(payload, kpi_text)
    _LOOKUP_CACHE[key] = resolved
    logger.info(
        "KPI resolved '%s' -> table=%s, kpi_col=%s",
        kpi_text,
        resolved["table_name"],
        resolved["kpi_col"],
    )
    return {**resolved, "aggregation": aggregation}
