"""
main.py
-------
FastAPI entry point for the VP Resolver Pipeline.

Endpoints:
  POST   /resolve      → resolve a natural language condition into VP + template
  GET    /registry     → dump full registry (debug / audit)
  DELETE /registry     → clear registry (fresh session)
  GET    /health       → health check + registry size

Run:
  pip install fastapi uvicorn groq pydantic requests
  uvicorn main:app --host 0.0.0.0 --port 8001 --reload
"""

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from registry import VPRegistry
from resolver import resolve, ResolveResult

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Shared Registry (lives for the lifetime of the process)
# ─────────────────────────────────────────────────────────────────────────────
registry = VPRegistry()


# ─────────────────────────────────────────────────────────────────────────────
# Lifespan
# ─────────────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("VP Resolver API starting up.")
    yield
    logger.info("VP Resolver API shutting down.")


# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title        = "VP Resolver API",
    description  = (
        "Resolves natural language KPI conditions into Virtual Profile names "
        "and PARENT_CONDITION rule templates. Supports recursive resolution "
        "for comparative (Track 4) conditions."
    ),
    version      = "1.0.0",
    lifespan     = lifespan
)


# ─────────────────────────────────────────────────────────────────────────────
# Request / Response Schemas
# ─────────────────────────────────────────────────────────────────────────────

class ResolveRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {"condition": "revenue drop last 3 months"},
                {"condition": "total revenue last month"},
                {"condition": "subscriber is subscribed to product"},
                {"condition": "YouTube share within total streaming usage"},
                {"condition": "recharge amount accumulated over last X days"},
            ]
        }
    )

    condition: str


class ResolveResponse(BaseModel):
    condition:        str
    vp_name:          str
    parent_condition: str
    track:            int
    child_templates:  dict
    registry_snapshot: dict
    elapsed_ms:       float


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/resolve", response_model=ResolveResponse, summary="Resolve a condition")
def resolve_condition(request: ResolveRequest) -> ResolveResponse:
    """
    Receives a natural language KPI condition and returns:
    - `vp_name`          : Generated Virtual Profile name
    - `parent_condition` : Filled rule template string
    - `child_templates`  : All child VP templates (populated for Track 4 composites)
    - `registry_snapshot`: Full current registry state (description → vp_name)
    - `elapsed_ms`       : Total resolution time in milliseconds

    ### Example
    **Input:**
    ```json
    { "condition": "revenue drop last 3 months" }
    ```

    **Output:**
    ```json
    {
      "condition": "revenue drop last 3 months",
      "vp_name": "M3_TOTAL_REVENUE__M1_TOTAL_REVENUE__PCT_DROP",
      "parent_condition": "(M1_TOTAL_REVENUE - M3_TOTAL_REVENUE) / M3_TOTAL_REVENUE * 100 ${operator} ${value}",
      "track": 4,
      "child_templates": {
        "M3_TOTAL_REVENUE": "COMMON_Event_Date = CurrentMonth-3MONTHS AND SUM(COMMON_Total_Revenue) ${operator} ${value}",
        "M1_TOTAL_REVENUE": "COMMON_Event_Date = CurrentMonth-1MONTHS AND SUM(COMMON_Total_Revenue) ${operator} ${value}"
      },
      "registry_snapshot": { ... },
      "elapsed_ms": 3241.5
    }
    ```
    """
    condition = request.condition.strip()
    if not condition:
        raise HTTPException(status_code=400, detail="condition cannot be empty.")

    logger.info("Incoming request — condition: '%s'", condition)
    t0 = time.perf_counter()

    try:
        result: ResolveResult = resolve(condition, registry, depth=0)
    except RecursionError as e:
        logger.error("Recursion limit hit: %s", e)
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception("Unexpected error during resolution.")
        raise HTTPException(status_code=500, detail=f"Resolution failed: {e}")

    elapsed_ms = (time.perf_counter() - t0) * 1000

    # Build registry snapshot — description → vp_name only (clean for response)
    snapshot = {
        desc: entry["vp_name"]
        for desc, entry in registry.get_all().items()
    }

    logger.info(
        "Resolved '%s' → %s  (%.0f ms)",
        condition, result.vp_name, elapsed_ms
    )

    return ResolveResponse(
        condition         = condition,
        vp_name           = result.vp_name,
        parent_condition  = result.parent_condition,
        track             = result.track,
        child_templates   = result.child_templates,
        registry_snapshot = snapshot,
        elapsed_ms        = round(elapsed_ms, 2)
    )


@app.get("/registry", summary="Dump full registry")
def get_registry():
    """
    Returns the full memoization registry for the current session.
    Useful for auditing what has been resolved and cached.
    """
    return {
        "size":  registry.size(),
        "store": registry.get_all()
    }


@app.delete("/registry", summary="Clear registry")
def clear_registry():
    """
    Clears the memoization registry.
    Use this to start a fresh session without restarting the server.
    """
    size_before = registry.size()
    registry.clear()
    logger.info("Registry cleared. Had %d entries.", size_before)
    return {
        "message":        "Registry cleared successfully.",
        "cleared_entries": size_before
    }


@app.get("/health", summary="Health check")
def health():
    """Returns server status and current registry size."""
    return {
        "status":        "ok",
        "registry_size": registry.size(),
        "model":         "openai/gpt-oss-120b",
        "template_url":  "http://10.0.11.179:9978/resolve"
    }


@app.get("/", include_in_schema=False)
def root():
    return JSONResponse({
        "service": "VP Resolver API",
        "version": "1.0.0",
        "docs":    "/docs",
        "health":  "/health"
    })
