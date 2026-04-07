"""
agents.py
---------
All 6 agent prompts (system/user split), Pydantic output schemas,
LLM call utility, and per-track caller functions.

FIX 1 CHANGES:
  - Track2Output: added groupby_entity field
  - Track5Output: added groupby_entity field
  - TRACK2_SYSTEM: added groupby detection rules
  - TRACK5_SYSTEM: added groupby detection rules
"""

import logging
import os
from typing import Optional, Any, Type, Literal, List
from pydantic import BaseModel
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

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
    track:         int
    kpi:           str
    aggregation:   str
    time_window:   TimeWindow
    is_composite:  bool
    filter_col:    Optional[str]       = None
    filter_values: Optional[List[str]] = None


class Track2TimeConstraint(BaseModel):
    type:  Literal["TODAY", "LAST_N_DAYS", "LAST_N_MONTHS", "THIS_MONTH"]
    value: Optional[int] = None   # None for TODAY / THIS_MONTH


class Track2Output(BaseModel):
    track:           int
    kpi:             str
    expected_state:  str
    aggregation:     None = None
    time_window:     None = None
    time_constraint: Optional[Track2TimeConstraint] = None
    is_composite:    bool
    # ── FIX 1: groupby support ────────────────────────────────────────────
    groupby_entity:  Optional[str] = None   # "subscriber", "device", "product", "product_description"


class Track3Output(BaseModel):
    track:        int
    kpi:          str
    qualifier:    str
    kpi_type:     str
    aggregation:  str
    time_window:  str
    is_composite: bool

    # ── NEW FIELDS ────────────────────────────────────────────────────────────
    sub_type:  Optional[Literal[
                   "snapshot_by_id",
                   "snapshot_max_check",
                   "snapshot_by_date_boundary",
                   "geo_last_n_days",
                   "geo_last_n_months",
                   "geo_current"
               ]] = None

    N:          Optional[int]  = None   # numeric value from "last 30 days" / "last 2 months"
    time_unit:  Optional[Literal["DAYS", "MONTHS"]] = None
    id_col:     Optional[str]  = None   # join key when different from the KPI column


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
    # ── FIX 1: groupby support ────────────────────────────────────────────
    groupby_entity:        Optional[str] = None   # "subscriber", "action_date", "device", "product"


# ── Track 6 — JOIN_CHECK schemas ─────────────────────────────────────────

class Track6DateRange(BaseModel):
    operator: Literal[">=", "<=", ">", "="]
    value:    Optional[int] = None    # null means "CurrentTime" with no offset
    unit:     Literal["DAYS", "MONTHS", "HOURS"] = "DAYS"


class Track6CountCheck(BaseModel):
    operator: Literal["=", ">", ">=", "<", "<="]
    value:    str                      # "0", "1", "2" etc.


class Track6Output(BaseModel):
    track:           int               # always 6
    kpi:             str               # "action key", "bonus counter", etc.
    join_var:        str               # "OM_MSISDN", "HBB_imeiNumber", etc.
    date_range:      Optional[Track6DateRange] = None
    count_check:     Optional[Track6CountCheck] = None
    groupby_entity:  Optional[str] = None   # "subscriber", "device", "product"
    is_composite:    bool = False


# ─────────────────────────────────────────────────────────────────────────────
# System Prompts
# ─────────────────────────────────────────────────────────────────────────────

CLASSIFIER_SYSTEM = """You are a classification agent for a telecom rule engine system.
Your job is to read a natural language description of a KPI or condition and classify it into exactly one of the following tracks:

- Track 1: TIME_SERIES — A metric aggregated (SUM, AVG, COUNT) over a time window: rolling weeks, specific months (M1/M2/M3), last N days, MTD, LMTD.
- Track 2: STATIC_FLAG — A dimensional state, boolean flag, subscription status, segment attribute, OR count-based existence/absence check. Includes: "count of X is zero", "count of X grouped by Y", "X not sent per subscriber", "flag exists/not exists", "count per device/product". No time-series aggregation over a sliding window.
- Track 3: SNAPSHOT — A single point-in-time value. Most recent, latest, or current record. No accumulation over a period.
- Track 4: COMPARATIVE — Compares a KPI against itself over two periods, or two different KPIs (percentage change, ratio, drop, growth).
- Track 5: PARAMETERIZED — Time window or product/plan is not fixed; will be supplied at runtime (e.g. "last N days", "specified product").
- Track 6: JOIN_CHECK — A condition that joins on a runtime subscriber/device variable (e.g. "matches OM_MSISDN", "join on MSISDN", "where device ID equals HBB_imeiNumber", "MSISDN matches OM_CHECK_MSISDN"). Always involves a runtime variable like OM_MSISDN, OM_CHECK_MSISDN, HBB_imeiNumber, RE_REFILL_ID, or LT_DEVICE_ID.

Rules:
1. Accumulation or aggregation over a time period → Track 1.
2. Current/latest single value with NO accumulation → Track 3.
3. Yes/no, exists/not-exists, subscribed/not-subscribed → Track 2.
4. Ratio, percentage drop/increase, comparison between two metrics or two periods → Track 4.
5. Variable placeholders like "N days", "specified", "given", "any" → Track 5.
6. Doubt between Track 1 and Track 3: "total/sum/average/count" implied → Track 1. "latest/current value/last known/as of now" → Track 3.
7. Doubt between Track 1 and Track 4: two time periods compared, or drop/growth/ratio → Track 4.
8. "count of X is zero", "count of X grouped by", or "count per subscriber/device/product" with NO time range → Track 2 (these are existence/absence checks expressed through counts, not time-series aggregations).
9. "X not triggered/sent per subscriber", "X per device", or "no record per entity" → Track 2 (per-entity presence checks are flags, not time-series).
10. If the condition mentions matching or joining on a runtime variable name (OM_MSISDN, OM_CHECK_MSISDN, HBB_imeiNumber, RE_REFILL_ID, LT_DEVICE_ID) → Track 6, regardless of whether it also has a date range or count check.

Respond ONLY in this JSON format with no extra text, no backticks, no markdown:
{
  "track": <1|2|3|4|5|6>,
  "track_label": "<TIME_SERIES|STATIC_FLAG|SNAPSHOT|COMPARATIVE|PARAMETERIZED|JOIN_CHECK>",
  "confidence": "<HIGH|MEDIUM|LOW>",
  "reason": "<one sentence explaining why>"
}"""


TRACK1_SYSTEM = """You are an extraction agent for a telecom rule engine system.
You will receive a natural language description already classified as Track 1: TIME_SERIES.

Fields to extract:
- kpi: the core metric being measured, in plain English (e.g. "total revenue", "recharge amount", "data volume", "og call revenue"). Extract only the metric name — aggregation intent like "average" or "sum" goes into the aggregation field, not here.
- aggregation: AVG | SUM | COUNT | COUNT_ALL | MAX | MIN
  Infer from context: "total"→SUM, "average"/"mean"→AVG, "number of"→COUNT, "count all"→COUNT_ALL
- time_window.type: ROLLING_WEEK | FIXED_MONTH | LAST_N | MTD | LMTD
- time_window.value: the numeric value from the input (e.g. 30, 3, 90). null for MTD/LMTD.
- time_window.unit: the time unit mentioned in the input — DAY, WEEK, or MONTH. null for MTD/LMTD.
  "last 30 days" → unit=DAY. "last 3 months" → unit=MONTH. "rolling week 5" → unit=WEEK.
- is_composite: false
- filter_col: column being filtered (natural language, e.g. "refill ID", "action key") — null if no list filter
- filter_values: list of specific allowed values (e.g. ["MD03", "M138"]) — null if no list filter

Rules:
1. Never guess table or column names — leave those to downstream.
2. Default SUM for revenue/amount, COUNT for event/occurrence fields.
3. "Two months ago" / "month 2" / "M2" → FIXED_MONTH value=2.
4. "Rolling week 5" → ROLLING_WEEK value=5.
5. "Last 30 days" / "past 30 days" → LAST_N value=30 unit=DAY.
6. "Last 3 months" / "over the last 3 months" / "past 3 months" → LAST_N value=3 unit=MONTH.
7. "Month to date" / "this month" / "current month" → MTD value=null unit=null.
8. "Last month to date" / "same period last month" → LMTD value=null unit=null.
9. "average revenue over last 3 months" → kpi: "revenue", aggregation: AVG, time_window: {type: LAST_N, value: 3, unit: MONTH}.
   "average recharge amount over last 3 months" → kpi: "recharge amount", aggregation: AVG, time_window: {type: LAST_N, value: 3, unit: MONTH}.
   "total data revenue in the last 15 days" → kpi: "data revenue", aggregation: SUM, time_window: {type: LAST_N, value: 15, unit: DAY}.
   "total revenue last month" → kpi: "total revenue", aggregation: SUM, time_window: {type: FIXED_MONTH, value: 1}.
10. If the condition mentions filtering by a specific list of values on any column — such as a list of product IDs, refill IDs, action keys, service types, bundle codes, or any other set of named values — extract the column being filtered into filter_col and the list of values into filter_values. Use COUNT_ALL as the aggregation when a list filter is present. If no such multi-value filter is mentioned, leave both fields as null.
   Examples:
   - "count of purchases for products MD03, M138, M139" → filter_col: "product ID", filter_values: ["MD03", "M138", "M139"]
   - "bonus sent for action keys HBB_key, PROMO_key" → filter_col: "action key", filter_values: ["HBB_key", "PROMO_key"]
   - "total revenue last 30 days" → filter_col: null, filter_values: null

Respond ONLY in this JSON format with no extra text, no backticks, no markdown:
{
  "track": 1,
  "kpi": "<kpi>",
  "aggregation": "<AGG>",
  "time_window": {"type": "<TYPE>", "value": <number|null>, "unit": "<UNIT|null>"},
  "is_composite": false,
  "filter_col": null,
  "filter_values": null
}"""


TRACK2_SYSTEM = """You are an extraction agent for a telecom rule engine system.
You will receive a natural language description already classified as Track 2: STATIC_FLAG.

Fields to extract:
- kpi: the attribute or flag being checked (e.g. "prepaid migration best plan", "product", "audience segment")
- expected_state: EXISTS | NOT_EXISTS | TRUE | FALSE | SUBSCRIBED | NOT_SUBSCRIBED | ASSIGNED
- aggregation: null (always)
- time_window: null (always)
- is_composite: false
- groupby_entity: the entity to group the count by — see Groupby Detection below

Rules:
1. No time window extraction.
2. Focus on the STATE being checked, not the metric value.
3. "Segment name" type descriptions → expected_state = ASSIGNED.
4. "Next best offer exists" → expected_state = EXISTS.
5. "Not subscribed to product" → expected_state = NOT_SUBSCRIBED.
6. If the condition mentions a time constraint like 'today', 'last N days', 'last N months', or 'this month', extract it into the time_constraint field. If no time constraint is mentioned, leave it as null.

## Groupby Detection

If the count or check should be scoped per entity (per subscriber, per device, etc.), extract groupby_entity.
Look for phrases like "per subscriber", "per MSISDN", "unique per", "grouped by", "for each subscriber", "count per device".

Allowed values:
- "subscriber" — count per subscriber/MSISDN (e.g. "action key count per subscriber", "grouped by MSISDN")
- "device" — count per device/IMEI (e.g. "per device", "grouped by IMEI")
- "product" — count per product/refill ID (e.g. "grouped by product", "per refill ID")
- "product_description" — count per product and description combo (e.g. "grouped by product and description")

If no groupby intent is detected, set groupby_entity to null.

Examples:
- "Count of action keys sent, grouped by subscriber" → groupby_entity: "subscriber"
- "Refill type count per product" → groupby_entity: "product"
- "Action key count per product and description" → groupby_entity: "product_description"
- "NBO product ID is available" → groupby_entity: null (no grouping)

Respond ONLY in this JSON format with no extra text, no backticks, no markdown:
{
  "track": 2,
  "kpi": "<attribute>",
  "expected_state": "<STATE>",
  "aggregation": null,
  "time_window": null,
  "time_constraint": null,
  "is_composite": false,
  "groupby_entity": null
}"""


TRACK3_SYSTEM = """You are an extraction agent for a telecom rule engine system.
You will receive a natural language description already classified as Track 3: SNAPSHOT.

Track 3 means: a single point-in-time value — the most recent, latest, or current record
for a customer. No accumulation, no sum, no average over a period.

────────────────────────────────────────────────────────────────
FIELDS TO EXTRACT
────────────────────────────────────────────────────────────────

kpi          : the metric or attribute being checked
               (e.g. "account balance", "geo location", "HBB add-on deactivation date")

qualifier    : LATEST | CURRENT | LAST_OCCURRENCE

kpi_type     : NUMERIC | DATE | CATEGORICAL

aggregation  : LATEST | LAST_VALUE

time_window  : always "CURRENT"

is_composite : always false

sub_type     : which template to use — pick exactly one from this list:

               "snapshot_by_id"
                   → A customer record is matched by an ID or attribute value.
                   → No date window. No MAX. No COUNT.
                   → Example: "customers whose HBB add-on deactivation date >= 500"

               "snapshot_max_check"
                   → Condition uses MAX(column) to confirm the field is non-null or non-zero.
                   → Example: "customers who have a valid HBB ID and MAX(HBB_ID) <> NULL"

               "snapshot_by_date_boundary"
                   → Condition checks a fixed date offset (e.g. yesterday, N days ago)
                     combined with a COUNT = 0 or COUNT > 0 check on another column.
                   → Always uses DAYS. Never months.
                   → Example: "customers who activated add-on yesterday and have no fixed line"

               "geo_last_n_days"
                   → Geographic location check within the last N DAYS.
                   → Example: "customers detected in region >= 500 at least once in the last 30 days"

               "geo_last_n_months"
                   → Geographic location check within the last N MONTHS.
                   → Example: "customers detected in region >= 500 at least once in the last 2 months"

               "geo_current"
                   → Current location only. No time window at all.
                   → Example: "customers whose current location is region >= 500"

N            : the integer from the time window phrase.
               - "last 30 days"   → N = 30
               - "last 2 months"  → N = 2
               - "yesterday"      → N = 1
               - No time window   → N = null

time_unit    : "DAYS" or "MONTHS" — the unit that N belongs to.
               - If sub_type is geo_last_n_days or snapshot_by_date_boundary → always "DAYS"
               - If sub_type is geo_last_n_months                            → always "MONTHS"
               - If sub_type has no time window (snapshot_by_id, snapshot_max_check,
                 geo_current)                                                → null

id_col       : ONLY fill this when the condition contains a runtime variable match
               on a column that is DIFFERENT from the kpi column.
               - Example: "HBBID = $HBBID AND HBBAddon_Inact_Date >= 500"
                 → kpi = "HBB add-on deactivation date", id_col = "HBBID"
               - If there is no separate join key → id_col = null

────────────────────────────────────────────────────────────────
ROUTING RULES — read top to bottom, first match wins
────────────────────────────────────────────────────────────────

1. Contains "MAX(col)" or "MAX(col) <> NULL" or "MAX(col) > 0"
   → sub_type = "snapshot_max_check", N = null, time_unit = null

2. Contains a date = CurrentTime-NDAYS pattern AND a COUNT check on another column
   → sub_type = "snapshot_by_date_boundary"
   → N = that number, time_unit = "DAYS"

3. Contains geo / region / location AND a time window in DAYS
   → sub_type = "geo_last_n_days"
   → N = that number, time_unit = "DAYS"

4. Contains geo / region / location AND a time window in MONTHS
   → sub_type = "geo_last_n_months"
   → N = that number, time_unit = "MONTHS"

5. Contains geo / region / location AND no time window
   → sub_type = "geo_current"
   → N = null, time_unit = null

6. Contains a runtime variable on one column ($VAR) AND a separate KPI column
   → sub_type = "snapshot_by_id"
   → id_col = the column with the $VAR match
   → kpi = the other column being filtered

7. All other cases
   → sub_type = "snapshot_by_id", N = null, time_unit = null, id_col = null

────────────────────────────────────────────────────────────────
EXAMPLES
────────────────────────────────────────────────────────────────

Input : "Customers detected in region >= 500 at least once in the last 30 days"
Output: sub_type = "geo_last_n_days", N = 30, time_unit = "DAYS", id_col = null

Input : "Customers detected in region >= 500 at least once in the last 2 months"
Output: sub_type = "geo_last_n_months", N = 2, time_unit = "MONTHS", id_col = null

Input : "Customers whose current location is region >= 500"
Output: sub_type = "geo_current", N = null, time_unit = null, id_col = null

Input : "Customers with HBB add-on component >= 500 who have a valid HBB ID and MAX(HBB_ID) <> NULL"
Output: sub_type = "snapshot_max_check", N = null, time_unit = null, id_col = null

Input : "Customers who activated HBB add-on yesterday and have no active fixed line"
Output: sub_type = "snapshot_by_date_boundary", N = 1, time_unit = "DAYS", id_col = null

Input : "Customers with HBB ID matching $HBBID and HBB add-on deactivation date >= 500"
Output: sub_type = "snapshot_by_id", N = null, time_unit = null, id_col = "HBBID"

Input : "Customers whose prepaid voice revenue on event date >= 500"
Output: sub_type = "snapshot_by_id", N = null, time_unit = null, id_col = null

────────────────────────────────────────────────────────────────
OUTPUT FORMAT
────────────────────────────────────────────────────────────────

Respond ONLY in this JSON format with no extra text, no backticks, no markdown:
{
  "track":        3,
  "kpi":          "<kpi>",
  "qualifier":    "<LATEST|CURRENT|LAST_OCCURRENCE>",
  "kpi_type":     "<NUMERIC|DATE|CATEGORICAL>",
  "aggregation":  "<LATEST|LAST_VALUE>",
  "time_window":  "CURRENT",
  "is_composite": false,
  "sub_type":     "<snapshot_by_id|snapshot_max_check|snapshot_by_date_boundary|geo_last_n_days|geo_last_n_months|geo_current>",
  "N":            <integer or null>,
  "time_unit":    "<DAYS|MONTHS|null>",
  "id_col":       "<column_name or null>"
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


TRACK6_SYSTEM = """You are an extraction agent for a telecom rule engine system.
You will receive a natural language description already classified as Track 6: JOIN_CHECK.
This track handles conditions that join on a runtime subscriber/device variable.

Fields to extract:
- kpi: the attribute being checked (e.g. "action key", "bonus counter", "refill type")
- join_var: the runtime variable name being matched (e.g. "OM_MSISDN", "OM_CHECK_MSISDN", "HBB_imeiNumber", "RE_REFILL_ID", "LT_DEVICE_ID"). Extract exactly as named in the input — do not add "$" prefix.
- date_range: if a time filter is mentioned, extract operator, value, and unit. null if no date filter.
- count_check: if a count condition is mentioned (e.g. "count is zero", "at least one"), extract operator and value. null if no count condition.
- groupby_entity: if counting should be grouped per entity (e.g. "per subscriber", "per device"). null if no grouping.
- is_composite: false (always)

Rules:
1. join_var must be extracted exactly as it appears in the input (e.g. "OM_MSISDN" not "$OM_MSISDN", not "om_msisdn").
2. "within last N days" / "last N days" / "sent in N days" → date_range: {"operator": ">=", "value": N, "unit": "DAYS"}
3. "up to today" / "until now" / "up to current time" → date_range: {"operator": "<=", "value": null, "unit": "DAYS"}
4. "before N days ago" / "older than N days" → date_range: {"operator": "<=", "value": N, "unit": "DAYS"}
5. "count is zero" / "not sent" / "absent" / "no record" → count_check: {"operator": "=", "value": "0"}
6. "count greater than zero" / "at least one" / "present" / "exists" → count_check: {"operator": ">", "value": "0"}
7. "at most 2" / "less than N" → count_check: {"operator": "<", "value": "N"} or {"operator": "<=", "value": "N"}
8. "per subscriber" / "per MSISDN" / "grouped by subscriber" → groupby_entity: "subscriber"
9. "per device" / "per IMEI" / "grouped by device" → groupby_entity: "device"
10. "per product" / "per refill" / "grouped by product" → groupby_entity: "product"
11. If no date range is mentioned at all, set date_range to null.
12. If no count condition is mentioned, set count_check to null.

Respond ONLY in this JSON format with no extra text, no backticks, no markdown:
{
  "track": 6,
  "kpi": "<attribute being checked>",
  "join_var": "<runtime variable name>",
  "date_range": null,
  "count_check": null,
  "groupby_entity": null,
  "is_composite": false
}

date_range, when not null, must be a JSON object:
  {"operator": ">=", "value": 350, "unit": "DAYS"}
  {"operator": "<=", "value": null, "unit": "DAYS"}

count_check, when not null, must be a JSON object:
  {"operator": "=", "value": "0"}
  {"operator": ">", "value": "0"}
"""


TRACK5_SYSTEM = """You are an extraction agent for a telecom rule engine system.
You will receive a natural language description already classified as Track 5: PARAMETERIZED.

Fields to extract:
- kpi: core metric being measured (e.g. "recharge revenue", "billing event", "campaign response")
- aggregation: AVG | SUM | COUNT | MAX | MIN
- parameter_name: runtime variable name (X | N | PRODUCT_ID | PLAN_ID | NoOfDays | ...)
- parameter_unit: DAY | WEEK | MONTH | PRODUCT | PLAN | null
- parameter_description: plain English of what the user must supply at rule-creation time
- is_composite: false
- groupby_entity: the entity to group the count by — see Groupby Detection below

Rules:
1. "specified", "given", "any", "N days", "X months", "defined" → indicates a runtime parameter.
2. Never hardcode a value for the parameter — it must stay as a placeholder.
3. "Last X days recharge revenue" → parameter_name="X", parameter_unit=DAY.
4. "Subscriber subscribed to specified product" → parameter_name="PRODUCT_ID", parameter_unit=PRODUCT.

## Groupby Detection

If the count should be scoped per entity, extract groupby_entity.
Look for phrases like "per subscriber", "grouped by", "unique per", "for each".

Allowed values:
- "subscriber" — count per subscriber/MSISDN
- "device" — count per device/IMEI
- "product" — count per product/refill ID
- "action_date" — count per action key and sent date combo (e.g. "grouped by action key and date")
- "product_description" — count per product and description combo

If no groupby intent is detected, set groupby_entity to null.

Examples:
- "Promo sent for action key in last X days, grouped by action key and date" → groupby_entity: "action_date"
- "Bonus not sent to action key in last X days" → groupby_entity: null (no grouping)

Respond ONLY in this JSON format with no extra text, no backticks, no markdown:
{
  "track": 5,
  "kpi": "<kpi>",
  "aggregation": "<AGG>",
  "parameter_name": "<PARAM>",
  "parameter_unit": "<UNIT|null>",
  "parameter_description": "<what the user must supply>",
  "is_composite": false,
  "groupby_entity": null
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
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": schema_class.__name__,
                "schema": schema_class.model_json_schema()
            }
        },
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

def extract_track6(condition: str) -> Track6Output:
    return call_llm(
        TRACK6_SYSTEM,
        f"condition: {condition}",
        Track6Output
    )
