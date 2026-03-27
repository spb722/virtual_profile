"""
vp_verify_mock_api.py
---------------------
Simple mock of the VP verification API used by kpi_mapper.py.

Run:
  uvicorn vp_verify_mock_api:app --host 0.0.0.0 --port 5678 --reload
"""

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(
    title="VP Verify Mock API",
    description="Local mock of the KPI verification service",
    version="1.0.0",
)


class VerifyRequest(BaseModel):
    conditions: list[str]
    check: bool = False


MOCK_KPI_MAP = {
    "total revenue of customer": {
        "kpi": "Total_Revenue_Sum",
        "table_name": "Common_Seg_Fct",
        "datatype": "numeric",
    },
    "total revenue": {
        "kpi": "Total_Revenue_Sum",
        "table_name": "Common_Seg_Fct",
        "datatype": "numeric",
    },
    "og call revenue": {
        "kpi": "OG_Call_Revenue_Sum",
        "table_name": "Common_Seg_Fct",
        "datatype": "numeric",
    },
    "data revenue": {
        "kpi": "Data_Revenue_Sum",
        "table_name": "Common_Seg_Fct",
        "datatype": "numeric",
    },
    "recharge amount": {
        "kpi": "Recharge_Denomination_Sum",
        "table_name": "Recharge_Seg_Fact",
        "datatype": "numeric",
    },
}

DEFAULT_MATCH = {
    "kpi": "Total_Revenue_Sum",
    "table_name": "Common_Seg_Fct",
    "datatype": "numeric",
}


def _normalize(value: str) -> str:
    return value.strip().lower()


@app.get("/health")
def health():
    return {"status": "ok", "mock_entries": len(MOCK_KPI_MAP)}


@app.post("/webhook/VP_verify")
def verify(request: VerifyRequest):
    matches = []

    for condition in request.conditions:
        normalized = _normalize(condition)
        match = MOCK_KPI_MAP.get(normalized, DEFAULT_MATCH)
        matches.append(
            {
                "condition": condition,
                "kpi": match["kpi"],
                "table_name": match["table_name"],
                "datatype": match["datatype"],
            }
        )

    return {
        "output": {
            "matches": matches,
            "unmatched": [],
            "mismatch_percentage": 0,
        }
    }
