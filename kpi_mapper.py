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
from typing import List

import requests
import urllib3

logger = logging.getLogger(__name__)

VP_VERIFY_URL = os.environ.get(
    "VP_VERIFY_URL",
    "https://10.0.11.179:5678/webhook/VP_verify",
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


def resolve_kpi_list(kpi_texts: List[str], aggregation: str = "SUM") -> List[dict]:
    """
    Resolve multiple natural language KPI descriptions in a single API call.

    Returns a list of { table_name, kpi_col, datatype } dicts in the same order
    as kpi_texts.

    Raises KPIResolutionError if:
    - Any KPI cannot be resolved
    - The resolved KPIs span more than one table (cross-table formulas are unsupported)
    """
    if not kpi_texts:
        raise ValueError("kpi_texts cannot be empty.")

    keys = [_normalize_key(t) for t in kpi_texts]

    # If all are already cached, skip the API call
    if all(k in _LOOKUP_CACHE for k in keys):
        results = [_LOOKUP_CACHE[k] for k in keys]
        logger.info("KPI list fully resolved from cache: %s", [r["kpi_col"] for r in results])
        _validate_same_table(results, kpi_texts)
        return [{**r, "aggregation": aggregation} for r in results]

    body = {
        "conditions": [t.strip() for t in kpi_texts],
        "check": False,
    }
    logger.info("Resolving KPI list via API: %s", kpi_texts)

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
            f"KPI list lookup timed out after {VP_VERIFY_TIMEOUT}s"
        ) from exc
    except requests.exceptions.SSLError as exc:
        raise KPIResolutionError(
            "KPI list lookup failed SSL verification. "
            "Set VP_VERIFY_SSL_VERIFY=true if the endpoint has a trusted certificate."
        ) from exc
    except requests.exceptions.RequestException as exc:
        raise KPIResolutionError(f"KPI list lookup request failed: {exc}") from exc

    try:
        payload = resp.json()
    except ValueError as exc:
        raise KPIResolutionError("KPI list lookup returned non-JSON response.") from exc

    output = payload.get("output", {})
    matches = output.get("matches", [])

    if len(matches) != len(kpi_texts):
        unmatched = output.get("unmatched", [])
        raise KPIResolutionError(
            f"KPI list lookup returned {len(matches)} matches for {len(kpi_texts)} inputs. "
            f"unmatched={unmatched}"
        )

    results = []
    for i, match in enumerate(matches):
        if not isinstance(match, dict):
            raise KPIResolutionError(f"KPI list lookup: match {i} is malformed.")
        table_name = match.get("table_name")
        kpi_col    = match.get("kpi")
        if not table_name or not kpi_col:
            raise KPIResolutionError(
                f"KPI list lookup: match {i} missing 'table_name' or 'kpi'."
            )
        resolved = {
            "table_name":        table_name,
            "kpi_col":           kpi_col,
            "datatype":          match.get("datatype"),
            "matched_condition": match.get("condition"),
        }
        _LOOKUP_CACHE[keys[i]] = resolved
        results.append(resolved)

    logger.info(
        "KPI list resolved: %s",
        [(kpi_texts[i], r["kpi_col"], r["table_name"]) for i, r in enumerate(results)],
    )

    _validate_same_table(results, kpi_texts)
    return [{**r, "aggregation": aggregation} for r in results]


def _validate_same_table(results: List[dict], kpi_texts: List[str]) -> None:
    """Raise if the resolved KPIs span more than one table."""
    tables = {r["table_name"] for r in results}
    if len(tables) > 1:
        detail = ", ".join(
            f"'{kpi_texts[i]}' → {results[i]['table_name']}"
            for i in range(len(results))
        )
        raise KPIResolutionError(
            f"Multi-KPI formula spans multiple tables ({tables}). "
            f"All KPIs in a formula must come from the same table. Detail: {detail}"
        )
