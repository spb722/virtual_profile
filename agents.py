"""
agents.py
---------
All 6 agent prompts (system/user split), Pydantic output schemas,
LLM call utility, and per-track caller functions.
"""

import logging
import os
from typing import Optional, Any, Type
from pydantic import BaseModel
from groq import Groq

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Groq Client
# ─────────────────────────────────────────────────────────────────────────────
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
MODEL        = "openai/gpt-oss-120b"

client = Groq(api_key=GROQ_API_KEY)


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic Output Schemas
# ─────────────────────────────────────────────────────────────────────────────

class ClassifierOutput(BaseModel):
    track:       int
    track_label: str
    confidence:  str
    reason:      str


class TimeWindow(BaseModel):
    type:  str
    value: Optional[int]  = None
    unit:  Optional[str]  = None


class Track1Output(BaseModel):
    track:        int
    kpi:          str
    aggregation:  str
    time_window:  TimeWindow
    is_composite: bool


class Track2Output(BaseModel):
    track:          int
    kpi:            str
    expected_state: str
    aggregation:    None = None
    time_window:    None = None
    is_composite:   bool


class Track3Output(BaseModel):
    track:        int
    kpi:          str
    qualifier:    str
    kpi_type:     str
    aggregation:  str
    time_window:  str
    is_composite: bool


class Track4Output(BaseModel):
    track:           int
    operation:       str
    operand_a:       str
    operand_b:       str
    operand_a_track: int
    operand_b_track: int
    is_composite:    bool


class Track5Output(BaseModel):
    track:                 int
    kpi:                   str
    aggregation:           str
    parameter_name:        str
    parameter_unit:        Optional[str] = None
    parameter_description: str
    is_composite:          bool


# ─────────────────────────────────────────────────────────────────────────────
# System Prompts
# ─────────────────────────────────────────────────────────────────────────────

CLASSIFIER_SYSTEM = """You are a classification agent for a telecom rule engine system.
Your job is to read a natural language description of a KPI or condition and classify it into exactly one of the following tracks:

- Track 1: TIME_SERIES — A metric aggregated (SUM, AVG, COUNT) over a time window: rolling weeks, specific months (M1/M2/M3), last N days, MTD, LMTD.
- Track 2: STATIC_FLAG — A dimensional state, boolean flag, subscription status, or segment attribute. No time window, no aggregation.
- Track 3: SNAPSHOT — A single point-in-time value. Most recent, latest, or current record. No accumulation over a period.
- Track 4: COMPARATIVE — Compares a KPI against itself over two periods, or two different KPIs (percentage change, ratio, drop, growth).
- Track 5: PARAMETERIZED — Time window or product/plan is not fixed; will be supplied at runtime (e.g. "last N days", "specified product").

Rules:
1. Accumulation or aggregation over a time period → Track 1.
2. Current/latest single value with NO accumulation → Track 3.
3. Yes/no, exists/not-exists, subscribed/not-subscribed → Track 2.
4. Ratio, percentage drop/increase, comparison between two metrics or two periods → Track 4.
5. Variable placeholders like "N days", "specified", "given", "any" → Track 5.
6. Doubt between Track 1 and Track 3: "total/sum/average/count" implied → Track 1. "latest/current value/last known/as of now" → Track 3.
7. Doubt between Track 1 and Track 4: two time periods compared, or drop/growth/ratio → Track 4.

Respond ONLY in this JSON format with no extra text, no backticks, no markdown:
{
  "track": <1|2|3|4|5>,
  "track_label": "<TIME_SERIES|STATIC_FLAG|SNAPSHOT|COMPARATIVE|PARAMETERIZED>",
  "confidence": "<HIGH|MEDIUM|LOW>",
  "reason": "<one sentence explaining why>"
}"""


TRACK1_SYSTEM = """You are an extraction agent for a telecom rule engine system.
You will receive a natural language description already classified as Track 1: TIME_SERIES.

Fields to extract:
- kpi: core metric (e.g. "total revenue", "recharge amount", "data volume", "og call revenue")
- aggregation: AVG | SUM | COUNT | MAX | MIN  (infer: "total"→SUM, "average"→AVG, "number of"→COUNT)
- time_window.type: ROLLING_WEEK | FIXED_MONTH | LAST_N | MTD | LMTD
- time_window.value: numeric value (null for MTD/LMTD)
- time_window.unit: DAY | WEEK | MONTH (null for MTD/LMTD)
- is_composite: false

Rules:
1. Never guess table or column names — leave those to downstream.
2. Default SUM for revenue/amount, COUNT for event/occurrence fields.
3. "Two months ago" → FIXED_MONTH value=2.
4. "Rolling week 5" → ROLLING_WEEK value=5.
5. "Last 30 days" → LAST_N value=30 unit=DAY.
6. "Last 3 months" (without comparison) → LAST_N value=3 unit=MONTH.
7. "Month to date" / "this month" → MTD value=null unit=null.
8. "Last month to date" / "same period last month" → LMTD value=null unit=null.

Respond ONLY in this JSON format with no extra text, no backticks, no markdown:
{
  "track": 1,
  "kpi": "<kpi>",
  "aggregation": "<AGG>",
  "time_window": {"type": "<TYPE>", "value": <number|null>, "unit": "<UNIT|null>"},
  "is_composite": false
}"""


TRACK2_SYSTEM = """You are an extraction agent for a telecom rule engine system.
You will receive a natural language description already classified as Track 2: STATIC_FLAG.

Fields to extract:
- kpi: the attribute or flag being checked (e.g. "prepaid migration best plan", "product", "audience segment")
- expected_state: EXISTS | NOT_EXISTS | TRUE | FALSE | SUBSCRIBED | NOT_SUBSCRIBED | ASSIGNED
- aggregation: null (always)
- time_window: null (always)
- is_composite: false

Rules:
1. No time window extraction.
2. Focus on the STATE being checked, not the metric value.
3. "Segment name" type descriptions → expected_state = ASSIGNED.
4. "Next best offer exists" → expected_state = EXISTS.
5. "Not subscribed to product" → expected_state = NOT_SUBSCRIBED.

Respond ONLY in this JSON format with no extra text, no backticks, no markdown:
{
  "track": 2,
  "kpi": "<attribute>",
  "expected_state": "<STATE>",
  "aggregation": null,
  "time_window": null,
  "is_composite": false
}"""


TRACK3_SYSTEM = """You are an extraction agent for a telecom rule engine system.
You will receive a natural language description already classified as Track 3: SNAPSHOT.

Fields to extract:
- kpi: the metric or attribute being retrieved (e.g. "account balance", "geo location", "last promotion date")
- qualifier: LATEST | CURRENT | LAST_OCCURRENCE
- kpi_type: NUMERIC | DATE | CATEGORICAL
- aggregation: LATEST | LAST_VALUE
- time_window: "CURRENT" (always)
- is_composite: false

Rules:
1. "Current account balance" → qualifier=CURRENT, kpi_type=NUMERIC.
2. "Date of last promotion" → qualifier=LAST_OCCURRENCE, kpi_type=DATE.
3. "Latest known geographical location" → qualifier=LATEST, kpi_type=CATEGORICAL.
4. If accumulation is implied (total/sum/average) — likely misclassified as Track 3. Extract anyway.

Respond ONLY in this JSON format with no extra text, no backticks, no markdown:
{
  "track": 3,
  "kpi": "<kpi>",
  "qualifier": "<LATEST|CURRENT|LAST_OCCURRENCE>",
  "kpi_type": "<NUMERIC|DATE|CATEGORICAL>",
  "aggregation": "<LATEST|LAST_VALUE>",
  "time_window": "CURRENT",
  "is_composite": false
}"""


TRACK4_SYSTEM = """You are an extraction agent for a telecom rule engine system.
Your job is to extract structured fields from a natural language description already classified as Track 4: COMPARATIVE.

Fields to extract:
- operation: PERCENTAGE_CHANGE | PERCENTAGE_DROP | RATIO | DIFFERENCE
- operand_a: plain language description of the BASE operand (earlier period or denominator)
- operand_b: plain language description of the COMPARISON operand (later period or numerator)
- operand_a_track: track of operand A — always 1 or 3
- operand_b_track: track of operand B — always 1 or 3
- is_composite: true (always)

Rules:
1. is_composite is ALWAYS true.
2. operand_a is ALWAYS the BASE (earlier period or denominator).
3. operand_b is ALWAYS the COMPARISON (later period or numerator).
4. "Revenue drop last 3 months" → operand_a="total revenue 3 months ago", operand_b="total revenue last month", operation=PERCENTAGE_DROP.
5. "YouTube share within streaming" → operand_a="total streaming usage this month", operand_b="YouTube usage this month", operation=RATIO.
6. Express operands as clear layman sentences that can independently be re-classified as Track 1 or Track 3.

Respond ONLY in this JSON format with no extra text, no backticks, no markdown:
{
  "track": 4,
  "operation": "<PERCENTAGE_CHANGE|PERCENTAGE_DROP|RATIO|DIFFERENCE>",
  "operand_a": "<base description>",
  "operand_b": "<comparison description>",
  "operand_a_track": <1|3>,
  "operand_b_track": <1|3>,
  "is_composite": true
}"""


TRACK5_SYSTEM = """You are an extraction agent for a telecom rule engine system.
You will receive a natural language description already classified as Track 5: PARAMETERIZED.

Fields to extract:
- kpi: core metric being measured (e.g. "recharge revenue", "billing event", "campaign response")
- aggregation: AVG | SUM | COUNT | MAX | MIN
- parameter_name: runtime variable name (X | N | PRODUCT_ID | PLAN_ID | NoOfDays | ...)
- parameter_unit: DAY | WEEK | MONTH | PRODUCT | PLAN | null
- parameter_description: plain English of what the user must supply at rule-creation time
- is_composite: false

Rules:
1. "specified", "given", "any", "N days", "X months", "defined" → indicates a runtime parameter.
2. Never hardcode a value for the parameter — it must stay as a placeholder.
3. "Last X days recharge revenue" → parameter_name="X", parameter_unit=DAY.
4. "Subscriber subscribed to specified product" → parameter_name="PRODUCT_ID", parameter_unit=PRODUCT.

Respond ONLY in this JSON format with no extra text, no backticks, no markdown:
{
  "track": 5,
  "kpi": "<kpi>",
  "aggregation": "<AGG>",
  "parameter_name": "<PARAM>",
  "parameter_unit": "<UNIT|null>",
  "parameter_description": "<what the user must supply>",
  "is_composite": false
}"""


# ─────────────────────────────────────────────────────────────────────────────
# LLM Utility
# ─────────────────────────────────────────────────────────────────────────────

def strip_json(raw: str) -> str:
    """Remove markdown code fences that some models add around JSON."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1]
    if raw.endswith("```"):
        raw = raw.rsplit("```", 1)[0]
    return raw.strip()


def call_llm(system_prompt: str, user_prompt: str, schema_class: Type[BaseModel]) -> Any:
    """
    Call the Groq LLM with system + user prompts.
    Parse and validate the response against the given Pydantic schema.
    """
    logger.debug("LLM call → schema: %s", schema_class.__name__)
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt}
        ],
        response_format={"type": "json_object"},
        temperature=0
    )
    raw     = response.choices[0].message.content
    cleaned = strip_json(raw)
    logger.debug("LLM raw response: %s", cleaned)
    return schema_class.model_validate_json(cleaned)


# ─────────────────────────────────────────────────────────────────────────────
# Per-Track Caller Functions
# ─────────────────────────────────────────────────────────────────────────────

def classify(condition: str) -> ClassifierOutput:
    return call_llm(
        CLASSIFIER_SYSTEM,
        f"condition: {condition}",
        ClassifierOutput
    )

def extract_track1(condition: str) -> Track1Output:
    return call_llm(
        TRACK1_SYSTEM,
        f"condition: {condition}",
        Track1Output
    )

def extract_track2(condition: str) -> Track2Output:
    return call_llm(
        TRACK2_SYSTEM,
        f"condition: {condition}",
        Track2Output
    )

def extract_track3(condition: str) -> Track3Output:
    return call_llm(
        TRACK3_SYSTEM,
        f"condition: {condition}",
        Track3Output
    )

def extract_track4(condition: str) -> Track4Output:
    return call_llm(
        TRACK4_SYSTEM,
        f"condition: {condition}",
        Track4Output
    )

def extract_track5(condition: str) -> Track5Output:
    return call_llm(
        TRACK5_SYSTEM,
        f"condition: {condition}",
        Track5Output
    )
