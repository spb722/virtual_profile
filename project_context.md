# Project Context

## What This Project Appears To Do

This repository is building a "virtual profile" resolver for KPI-driven rule creation.

The main user journey looks like this:

1. A user provides an English sentence describing a KPI, condition, comparison, or rule idea.
2. The system classifies that sentence into one of several predefined tracks.
3. A track-specific extractor turns the sentence into structured JSON.
4. That structured data is converted into a rule-engine `PARENT_CONDITION` template string.
5. The system also generates a deterministic VP name for the resolved profile.
6. The result is cached in a registry so the same sentence does not need to be resolved again.

At a high level, this is a translator from:

- natural language
- into structured VP metadata
- into a rule-engine condition string
- plus a generated VP name

Track 4 is special because it is composite and recursive: instead of going directly to the template engine, it first resolves child operands, then composes the final comparison formula from their VP names.

## Main Architecture

There are effectively two services in this repo:

### 1. VP Resolver API

File: `main.py`

This is the top-level FastAPI app that accepts a natural-language condition and returns:

- `vp_name`
- `parent_condition`
- `track`
- `child_templates`
- `registry_snapshot`
- elapsed time

It relies on:

- `resolver.py` for orchestration
- `agents.py` for LLM classification and extraction
- `template_client.py` for calling the template engine
- `name_generator.py` for VP names
- `registry.py` for memoization

### 2. Template Engine API

File: `vp_template_engine_api.py`

This is a second FastAPI app that receives structured payloads and emits the final `PARENT_CONDITION` string by selecting and filling YAML templates from `vp_template_engine.yaml`.

The YAML file is the core rule-template catalog. It contains:

- track-specific template families
- a routing guide
- column metadata for table/date-column lookup

## End-to-End Flow

The core flow is implemented in `resolver.py`:

1. Normalize the incoming condition and check the registry cache.
2. If not cached, classify it into a track using the classifier prompt.
3. Run the track-specific extractor prompt.
4. Branch by track:
   - Track 1, 2, 3, 5: build payload -> call template engine -> generate VP name -> cache -> return
   - Track 4: extract operands -> recursively resolve both operands -> compose formula locally -> generate composite VP name -> cache -> return

Important implementation details:

- Cache key is `description.lower().strip()`.
- Recursion depth is capped at `MAX_DEPTH = 6`.
- Track 4 stores flattened child templates from nested resolutions.
- The registry is in-memory only and process-local.

## The Track Taxonomy

The agent prompts define 5 tracks plus a classifier agent, so there are 6 agents total.

### Track 1: TIME_SERIES

Meaning:

- aggregated KPI over a time window

Examples from prompts/docs:

- total revenue last month
- average recharge amount over last 3 months
- rolling week 5 OG call revenue

Fields extracted:

- `kpi` (metric name only; aggregation intent is extracted separately)
- `aggregation`
- `time_window`
- `is_composite`
- `filter_col` and `filter_values` in the current codebase

Supported time-window types:

- `ROLLING_WEEK`
- `FIXED_MONTH`
- `LAST_N`
- `MTD`
- `LMTD`

Current recent enhancement:

- Track 1 now also supports list-based filters for cases like product IDs or action keys, using `COUNT_ALL` plus `IN LIST (...)`.
- Track 1 accepts broader time-window phrasing such as `past 30 days`, `over the last 3 months`, `current month`, and aliases like `month 2` / `M2` for fixed months.
- Track 1 examples now explicitly distinguish KPI text from aggregation intent, for example `average recharge amount over last 3 months` extracts `kpi=recharge amount` and `aggregation=AVG`.

### Track 2: STATIC_FLAG

Meaning:

- state / flag / existence / subscription / segment-like checks

Examples:

- subscriber is subscribed to product
- next best offer exists
- audience segment assigned

Fields extracted:

- `kpi`
- `expected_state`
- `time_constraint` in the current codebase
- `is_composite`

Expected states:

- `EXISTS`
- `NOT_EXISTS`
- `TRUE`
- `FALSE`
- `SUBSCRIBED`
- `NOT_SUBSCRIBED`
- `ASSIGNED`

Recent enhancement:

- The prompt and payload builder now try to capture time constraints such as:
  - `TODAY`
  - `LAST_N_DAYS`
  - `LAST_N_MONTHS`
  - `THIS_MONTH`

### Track 3: SNAPSHOT

Meaning:

- single point-in-time / latest / current / last occurrence

Examples:

- current account balance
- latest known geographic location
- date of last promotion

Fields extracted:

- `kpi`
- `qualifier`
- `kpi_type`
- `aggregation`
- `time_window`
- `is_composite`

Qualifiers include:

- `LATEST`
- `CURRENT`
- `LAST_OCCURRENCE`

### Track 4: COMPARATIVE

Meaning:

- KPI compared across periods or against another KPI

Examples:

- revenue drop last 3 months
- percentage change in revenue
- YouTube share within streaming
- difference between X and Y

Fields extracted:

- `operation`
- `operand_a`
- `operand_b`
- `operand_a_track`
- `operand_b_track`
- `is_composite=true`

Supported operations:

- `PERCENTAGE_CHANGE`
- `PERCENTAGE_DROP`
- `RATIO`
- `DIFFERENCE`

Important semantic rule in the prompt and resolver:

- `operand_a` is the base / earlier period / denominator
- `operand_b` is the comparison / later period / numerator

Track 4 is recursive and composite. It does not go through the template engine in the main resolver path. Instead, child operands are resolved first, then the final formula is composed in Python.

### Track 5: PARAMETERIZED

Meaning:

- the sentence contains runtime placeholders rather than a fixed time range or product/plan

Examples:

- recharge amount over last X days
- subscriber subscribed to specified product

Fields extracted:

- `kpi`
- `aggregation`
- `parameter_name`
- `parameter_unit`
- `parameter_description`
- `is_composite`

This track is intended for templates where something is provided later at rule-creation time, such as:

- `${X}`
- `${NoOfDays}`
- product IDs
- plan IDs

## Agents And LLM Usage

All LLM logic lives in `agents.py`.

Observed setup:

- provider: Groq
- model: `openai/gpt-oss-120b`
- `temperature=0`
- response format requested as JSON object
- Pydantic validation is used after the model response
- `load_dotenv()` is called and `GROQ_API_KEY` is read from the environment

There are 6 logical agents:

1. classifier
2. track 1 extractor
3. track 2 extractor
4. track 3 extractor
5. track 4 extractor
6. track 5 extractor

The prompt pattern is simple and consistent:

- system prompt contains all extraction rules
- user prompt is just `condition: <input>`

## Template Engine Behavior

The template engine is not LLM-based. It is deterministic.

It receives a discriminated payload with `track` and then dispatches to one of:

- `resolve_track1`
- `resolve_track2`
- `resolve_track3`
- `resolve_track4`
- `resolve_track5`

After generating the condition, it collapses extra whitespace and returns:

- `track`
- `parent_condition`
- a note reminding that `${operator} ${value}` should remain if not already present

### Track 1 Template Families

The YAML currently defines these Track 1 families:

- `rolling_week`
- `fixed_month`
- `last_n`
- `mtd_lmtd`
- `filtered_count`

The recent `filtered_count` family is new and is used for multi-value list filters with `COUNT_ALL`.

### Track 2 Template Families

The YAML currently defines:

- `subscription`
- `flag_check`

And `flag_check` now includes additional time-aware count templates such as:

- `template_count_flag_present_within_n_days`
- `template_count_flag_present_today`
- `template_count_flag_absent_within_n_days`
- `template_count_flag_absent_today`

### Track 3 Template Families

- `snapshot_id`
- `geo_location`

### Track 4 Template Families

- `pct_drop`
- `ratio`

There is also a local `DIFFERENCE` formula branch in Python.

### Track 5 Template Families

- `dynamic_window`
- `campaign_check`

This suggests Track 5 is meant to support both:

- generic "last X days" patterns
- campaign / promo / bonus history checks

## Naming Logic

VP names are built in `name_generator.py`.

Observed naming rules:

- Track 1:
  - `FIXED_MONTH` -> `M{N}_{COL}`
  - `ROLLING_WEEK` -> `ROLLING_W{N}_{COL}`
  - `LAST_N` -> `LAST{N}{DAYS|MONTHS}_{COL}`
  - `MTD` -> `MTD_{COL}`
  - `LMTD` -> `LMTD_{COL}`
- Track 2:
  - `{STATE}_{KPI}`
- Track 3:
  - `{QUALIFIER}_{KPI}`
- Track 4:
  - `{VP_A}__{VP_B}__{OP_SHORT}`
- Track 5:
  - `{AGG}_{COL}_LAST_{PARAM}_{UNIT}S`

Track 4 short codes are:

- `PERCENTAGE_DROP` -> `PCT_DROP`
- `PERCENTAGE_CHANGE` -> `PCT_CHG`
- `RATIO` -> `RATIO`
- `DIFFERENCE` -> `DIFF`

The name generator also strips some common column prefixes such as:

- `COMMON_`
- `RECHARGE_`
- `SUBSCRIPTIONS_`
- `BILL_PAYMENT_`

## Comparative Formula Logic

Track 4 formulas are composed in `resolver.py`:

- percentage drop / change:
  - `({vp_b} - {vp_a}) / {vp_a} * 100 ${operator} ${value}`
- ratio:
  - `({vp_b} / {vp_a}) * 100 ${operator} ${value}`
- difference:
  - `({vp_b} - {vp_a}) ${operator} ${value}`

So the base/earlier/denominator side is always `vp_a`.

## Registry / Memoization

The registry is a simple in-memory dictionary.

It stores:

- `vp_name`
- `parent_condition`
- `child_templates`
- any extra metadata passed during save, such as `track`

There are endpoints to:

- inspect the registry
- clear the registry

This is a session/process cache, not persistent storage.

## API Endpoints

### Resolver API (`main.py`)

- `POST /resolve`
- `GET /registry`
- `DELETE /registry`
- `GET /health`
- `GET /`

### Template Engine API (`vp_template_engine_api.py`)

- `POST /resolve`
- `GET /templates`
- `GET /examples`
- `GET /health`

## How To Run Both Applications

From the repository root:

1. Install dependencies:

```bash
pip install -r requirements.txt
pip install pyyaml python-dotenv
```

2. Make sure `.env` contains a valid Groq key:

```bash
GROQ_API_KEY=your_key_here
```

3. Start the template engine first:

```bash
uvicorn vp_template_engine_api:app --host 0.0.0.0 --port 9978 --reload
```

4. Start the main resolver API second in another terminal:

```bash
uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

Why this order:

- the main resolver calls the template engine for Tracks 1, 2, 3, and 5
- if the template engine is not up first, the main API cannot produce final templates

Quick health checks:

```bash
curl http://localhost:9978/health
curl http://localhost:8001/health
```

Quick resolver test:

```bash
curl -X POST http://localhost:8001/resolve \
  -H "Content-Type: application/json" \
  -d '{"condition":"total revenue last month"}'
```

## Column Metadata And Tables

The YAML's `column_metadata` section is effectively the data dictionary for template generation.

Observed table keys include:

- `COMMON_Seg_Fct`
- `Recharge_Seg_Fact`
- `DPI_App_Usage`
- `DPI_Geo_Location`
- `Subscriptions`
- `BILL_PAYMENT`
- `BILL_EVENT`
- `LIFECYCLE_PROMO`
- `LIFECYCLE_BONUS`
- `LIFECYCLE_CDR`
- `MANUAL_SEGMENT_GROUP`
- `AUDIENCE_SEGMENT_CDR`
- `Profile_Cdr_group`
- `Instant_cdr_group`
- `CAMPAIGN_WHITELIST`
- `360_PROFILE`
- `REDEEMED_POINTS`
- `HBB`
- `GEO_LOCATION_STATIC`
- `AUDIENCE_SEGMENT`

This metadata is used mainly to resolve the correct `date_col` from `table_name`.

The template engine does case-insensitive matching for table names, which helps with naming inconsistencies like `Subscriptions` vs `SUBSCRIPTIONS`.

## What Seems Intended Versus What Is Fully Implemented

This is the most important part of my understanding: the repository has a clear intended design, but some pieces are still placeholders or incomplete.

### 1. `CLAUDE.md` is useful, but not fully authoritative

It describes the intended architecture well, but a few things no longer match the live code:

- it says `kpi_mapper.py` contains about 60 KPI phrases in a local map, but the current code now resolves KPIs through the external VP verification API
- it refers to a `vp_resolver_api/` subdirectory, but the active files are in the repo root
- it uses `http://10.0.11.179:9978/resolve` as the template URL, while `template_client.py` currently points to `http://localhost:9978/resolve`
- `vp_template_engine_api.py` still has a run comment mentioning port `8000`, while the client and docs imply `9978`

My read is that the design doc captures the intended system, while the code reflects a local/in-progress version.

### 2. KPI resolution is now an external dependency rather than a local map

`kpi_mapper.py` no longer uses a hard-coded `KPI_MAP`. It now calls the VP verification API and treats the response as the KPI resolver for the rest of the pipeline.

The current behavior is:

- send the extracted KPI text to the VP verification API
- take the first returned match
- use `table_name` directly
- treat the returned `kpi` value as the internal `kpi_col`
- cache lookups in memory for repeated use during the process lifetime

This means KPI resolution is more dynamic than before, but the success of Tracks 1, 2, 3, and 5 now depends on:

- the VP verification API being reachable
- the API returning a match for the extracted KPI text
- the returned `kpi` naming being compatible with downstream template and naming expectations

If no match is returned, the main API now surfaces a clear `422` error naming the missing KPI text.

### 3. Track 1 looks the most mature

Track 1 has:

- detailed prompt rules
- clear payload building
- strong YAML support
- naming support
- recent list-filter enhancement

This looks like the most complete leaf-track path right now.

### 4. Track 4 is conceptually strong and clearly implemented

Track 4 has:

- strong prompt guidance
- recursion
- child VP resolution
- local formula composition
- composite VP naming

This is one of the clearest parts of the system design.

### 5. Track 2 appears partially implemented

The prompt, YAML, and client imply support for:

- subscriptions
- existence checks
- flags
- segment checks
- time-constrained flags

But the current payload builder does not appear to populate all fields needed by every Track 2 template branch.

Examples:

- subscription templates need `id_col`, but the builder currently only sets `flag_col` and `count_col`
- `ASSIGNED -> segment_type` would need `segment_col` and `segment_val`, but the builder does not populate those

So Track 2 support exists structurally, but not every subtype looks wired end-to-end yet.

### 6. Track 3 also looks only partially wired

The Track 3 extractor captures:

- qualifier
- type
- latest/current semantics

But the payload builder currently always emits:

- `sub_type = snapshot_by_id`
- `value_col = resolved kpi col`
- `id_col = resolved kpi col`

That suggests the richer Track 3 YAML families, especially geo-related ones, are not yet fully connected to the extraction output.

### 7. Track 5 is broader in YAML than in the resolver

The prompts and YAML suggest Track 5 should cover:

- generic runtime windows like last X days
- product/plan parameterization
- campaign / promo / bonus checks
- multi-parameter cases

But the current `build_track5_payload()` always emits:

- `sub_type = sum_x_days`

So the current resolver path only seems to actively support a narrow subset of the Track 5 design.

### 8. Recent work is actively extending the system

The repo currently has uncommitted changes in:

- `agents.py`
- `template_client.py`
- `vp_template_engine_api.py`
- `vp_template_engine.yaml`

Those changes add:

- Track 1 list-filter extraction and template support
- Track 2 time-constraint extraction and additional template branches

So the project is actively evolving rather than static.

## Setup / Dependency Notes

`requirements.txt` currently includes:

- `fastapi`
- `uvicorn[standard]`
- `groq`
- `pydantic`
- `requests`

But the code also imports:

- `dotenv` / `load_dotenv()` in `agents.py`
- `yaml` in `vp_template_engine_api.py`

So the current environment must also have packages compatible with:

- `python-dotenv`
- `PyYAML`

Those are currently not listed in `requirements.txt`.

There is now also an external runtime dependency on the VP verification API used by `kpi_mapper.py`:

- default URL: `https://10.0.11.179:5678/webhook/VP_verify/webhook/VP_verify`
- configurable with `VP_VERIFY_URL`
- timeout configurable with `VP_VERIFY_TIMEOUT`
- SSL verification configurable with `VP_VERIFY_SSL_VERIFY`

## Current Repository Snapshot Notes

Observed repo characteristics:

- no automated tests are present
- there is a duplicate file `main (1).py` that appears to be the same as `main.py`
- the working tree is dirty, which suggests in-progress local development
- import-level sanity checks work in the current environment

## My Current Mental Model Of The System

If I had to explain the project in one compact paragraph:

This is an LLM-assisted rule authoring pipeline for telecom-style virtual profiles. The LLM is used only to classify the user's sentence and extract structured track-specific fields. Once the structure is known, the rest of the system is deterministic: map KPI names to real columns, choose the correct YAML rule template, fill the placeholders, generate a VP name, and cache the result. Comparative profiles are recursive compositions of simpler profiles. The overall design is solid, but the current repository snapshot still has some unfinished wiring, especially around KPI mapping and the less mature Track 2/3/5 branches.

## Questions / Uncertainties I Still Have

These are the main things I still cannot know just from the code:

1. What the full KPI catalog behind the VP verification API is, and how complete its coverage is for the extractors' KPI phrases.
2. Which Track 2, 3, and 5 subtypes are already working in production versus only planned in code/YAML.
3. Whether the correct template engine target is localhost or `10.0.11.179`.
4. Whether `main (1).py` is disposable or intentionally kept as a fallback copy.
5. Whether the eventual source of truth is the current code, `CLAUDE.md`, or some external VP catalog / JSON export mentioned in the YAML comments.

## Practical Conclusion

The project already has a clear architecture:

- classify the sentence
- extract structured fields for the chosen track
- turn that into a template-engine payload
- generate a VP name
- return and cache the result

The parts I currently trust most are:

- the high-level resolver flow
- Track 1 time-series handling
- Track 4 comparative recursion
- the YAML-based template engine concept

The parts I would treat as still under construction are:

- full KPI coverage and error handling around the external KPI verification API
- full subtype coverage for Tracks 2, 3, and 5
- environment/config consistency across ports and template URLs
