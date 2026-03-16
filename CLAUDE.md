# VP Rule Engine — CLAUDE.md

## What This Project Does
Converts natural language KPI descriptions into structured rule syntax strings (`PARENT_CONDITION`) used in a telecom campaign rule engine.

- Input: `"revenue drop last 3 months"`
- Output: `(M1_TOTAL_REVENUE - M3_TOTAL_REVENUE) / M3_TOTAL_REVENUE * 100 ${operator} ${value}`

---

## Tech Stack

| Component | Technology |
|---|---|
| LLM | Groq API — `openai/gpt-oss-120b` |
| Structured Output | Pydantic v2 (`model_validate_json`) |
| API Framework | FastAPI + Uvicorn |
| Template Config | YAML (`vp_template_engine.yaml`) |
| Memoization | In-memory Python dict (`VPRegistry`) |
| HTTP Client | `requests` |

---

## Two Services — Always Start Both

| Service | File | Port | Role |
|---|---|---|---|
| Template Engine API | `vp_template_engine_api.py` | `9978` | Structured JSON → filled `PARENT_CONDITION` string |
| VP Resolver API | `vp_resolver_api/main.py` | `8001` | Natural language → VP name + template (calls Template Engine internally) |

```bash
# Terminal 1 — Start Template Engine FIRST
uvicorn vp_template_engine_api:app --host 0.0.0.0 --port 9978 --reload

# Terminal 2 — Start VP Resolver
cd vp_resolver_api
uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

---

## File Structure

```
vp_resolver_api/
├── main.py              ← FastAPI app, endpoints, shared VPRegistry
├── resolver.py          ← Recursive resolution engine (the brain)
├── agents.py            ← Groq client, 6 Pydantic schemas, 6 system prompts, call_llm()
├── registry.py          ← Session-level memoization (VPRegistry class)
├── kpi_mapper.py        ← KPI_MAP (~60 phrases) + resolve_kpi()
├── name_generator.py    ← generate_vp_name() for all 5 tracks
├── template_client.py   ← HTTP caller to Template Engine + payload builders
└── requirements.txt

vp_template_engine_api.py   ← Standalone FastAPI (Template Engine)
vp_template_engine.yaml     ← 14 YAML templates across 5 tracks
```

---

## 5-Track Taxonomy

Every condition maps to exactly one track:

| Track | Label | Description | Example |
|---|---|---|---|
| 1 | TIME_SERIES | KPI aggregated over a time window | "Average recharge last 3 months" |
| 2 | STATIC_FLAG | Boolean/subscription state | "Subscriber is subscribed to product" |
| 3 | SNAPSHOT | Single point-in-time value | "Latest known geographical location" |
| 4 | COMPARATIVE | Comparison between two periods/KPIs — **recursive, composite** | "Revenue drop last 3 months" |
| 5 | PARAMETERIZED | Time window or product supplied at runtime | "Recharge amount over last X days" |

### Track 1 Sub-variants

| Type | Meaning | Example |
|---|---|---|
| `FIXED_MONTH` | Calendar month offset | "two months ago" |
| `ROLLING_WEEK` | Rolling 7-day window | "rolling week 3" |
| `LAST_N` | Last N days/months | "last 30 days" |
| `MTD` | Month to date | "this month so far" |
| `LMTD` | Last month to date | "same period last month" |

---

## VP Naming Convention

| Track | Pattern | Example |
|---|---|---|
| 1 FIXED_MONTH | `M{N}_{KPI_COL}` | `M3_TOTAL_REVENUE` |
| 1 ROLLING_WEEK | `ROLLING_W{N}_{KPI_COL}` | `ROLLING_W3_OG_CALL_REVENUE` |
| 1 LAST_N | `LAST{N}DAYS_{KPI}` or `LAST{N}MONTHS_{KPI}` | `LAST30DAYS_DATA_REVENUE` |
| 1 MTD | `MTD_{KPI_COL}` | `MTD_TOTAL_REVENUE` |
| 1 LMTD | `LMTD_{KPI_COL}` | `LMTD_TOTAL_REVENUE` |
| 2 | `{EXPECTED_STATE}_{KPI}` | `SUBSCRIBED_PRODUCT` |
| 3 | `{QUALIFIER}_{KPI}` | `LATEST_GEO_LOCATION` |
| 4 | `{VP_A}__{VP_B}__{OP_SHORT}` | `M3_TOTAL_REVENUE__M1_TOTAL_REVENUE__PCT_DROP` |
| 5 | `{AGG}_{KPI_COL}_LAST_{PARAM}_{UNIT}S` | `SUM_RECHARGE_DENOMINATION_LAST_X_DAYS` |

---

## 6 Agents (all in `agents.py`)

| Agent | Name | Input → Output |
|---|---|---|
| 0 | Classifier | Any condition → track number (1–5) + label + confidence |
| 1 | Time-Series Extractor | Track 1 condition → `Track1Output` (kpi, aggregation, time_window) |
| 2 | Static Flag Extractor | Track 2 condition → `Track2Output` (kpi, expected_state) |
| 3 | Snapshot Extractor | Track 3 condition → `Track3Output` (kpi, qualifier, kpi_type) |
| 4 | Comparative Extractor | Track 4 condition → `Track4Output` (operation, operand_a, operand_b) |
| 5 | Parameterized Extractor | Track 5 condition → `Track5Output` (kpi, aggregation, parameter_name, parameter_unit) |

- All agents: `temperature=0`, `response_format={"type": "json_object"}`
- All agents use **system/user prompt split** — system has all rules, user prompt is just `"condition: {input}"`
- All return validated Pydantic objects via `call_llm(system_prompt, user_prompt, schema_class)`

---

## Critical Design Rules — Never Violate

- `date_col` is **never passed** in any agent JSON — always auto-resolved from `column_metadata[table_name]` in the YAML
- `kpi_list` is **removed** from Track 4 schema — use `operand_a` and `operand_b` only
- Track 4 **never calls the Template Engine** — it composes the formula directly from resolved child VP names
- `operand_a` = BASE (earlier period / denominator), `operand_b` = COMPARISON (later period / numerator) — always
- Registry key = `description.lower().strip()` — normalise before checking
- `MAX_DEPTH = 6` — raise `RecursionError` beyond this; caught in `main.py` as HTTP 422
- Registry is **per-process** — resets on restart; use `DELETE /registry` to clear without restart

---

## Track 4 Formula Composition

| Operation | Trigger Words | Formula |
|---|---|---|
| `PERCENTAGE_DROP` | "drop", "decline", "fell", "decreased" | `(vp_b - vp_a) / vp_a * 100 ${operator} ${value}` |
| `PERCENTAGE_CHANGE` | "percentage change", "% change", "growth" | `(vp_b - vp_a) / vp_a * 100 ${operator} ${value}` |
| `RATIO` | "ratio of", "share of", "out of total" | `(vp_b / vp_a) * 100 ${operator} ${value}` |
| `DIFFERENCE` | "difference between", "gap between" | `(vp_b - vp_a) ${operator} ${value}` |

`${operator}` and `${value}` are **runtime placeholders** — never fill them; they stay literal in the output.

---

## KPI Mapper (`kpi_mapper.py`)

- Resolves natural language KPI text → `{table_name, kpi_col}`
- Resolution order: direct match → fuzzy substring match → fallback (`COMMON_Total_Revenue` on `COMMON_Seg_Fct`)
- To add a KPI: add lowercase key to `KPI_MAP`; multiple synonyms can map to the same column
- `date_col` is NOT in KPI_MAP — resolved separately in Template Engine from YAML `column_metadata`

---

## VP Resolver API Endpoints (`port 8001`)

| Method | Path | Description |
|---|---|---|
| POST | `/resolve` | Main — NL condition → VP name + template |
| GET | `/registry` | View full session memoization cache |
| DELETE | `/registry` | Clear registry without restart |
| GET | `/health` | Status + registry size + model + template URL |

## Template Engine API Endpoints (`port 9978`)

| Method | Path | Description |
|---|---|---|
| POST | `/resolve` | Structured JSON → filled `PARENT_CONDITION` |
| GET | `/templates` | List all template keys loaded from YAML |
| GET | `/examples` | Sample request payloads for each track |
| GET | `/health` | Health check + YAML load status |

---

## Template Engine Request Format

```json
POST /resolve
{
  "payload": {
    "track": 1,
    "table_name": "COMMON_Seg_Fct",
    "kpi_col": "COMMON_Total_Revenue",
    "aggregation": "SUM",
    "time_window": {
      "type": "FIXED_MONTH",
      "value": 3,
      "unit": "MONTH"
    },
    "is_composite": false
  }
}
```

`track` inside `payload` is the discriminator. Response always has `parent_condition` string.

---

## Key KPI Tables

| Table | KPIs |
|---|---|
| `COMMON_Seg_Fct` | Total revenue, voice, data, SMS, IDD, roam, MOU |
| `DPI_App_Usage` | Streaming, YouTube, total data usage |
| `Recharge_Seg_Fact` | Recharge amount / denomination |
| `BILL_PAYMENT` | Bill payment summary |
| `SUBSCRIPTIONS` | Product ID |
| `LOYALTY_PROMO` | Promo sent, action key |
| `LOYALTY_BONUS` | Bonus sent, MSISDN |
| `REDEEMED_POINTS` | Redeemed points |

---

## Classifier Disambiguation Rules

- Doubt Track 1 vs 3: "total/sum/average" → Track 1; "latest/current/as of now" → Track 3
- Doubt Track 1 vs 4: two periods compared, or drop/growth/ratio → Track 4
- "N days", "specified", "given", "any X" placeholder → Track 5

---

## Groq Client Config

```python
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
MODEL        = "openai/gpt-oss-120b"
TEMPLATE_URL = "http://10.0.11.179:9978/resolve"
```
