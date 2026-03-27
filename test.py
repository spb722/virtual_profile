"""
test_structured_output.py
-------------------------
Hello World test for Groq's json_schema structured output.
Tests whether passing a Pydantic schema in response_format
forces the LLM to return the exact shape we need.

Run:
    GROQ_API_KEY=your_key python3 test_structured_output.py
"""

import json
import os
from typing import Optional, Literal
from pydantic import BaseModel
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
MODEL = "openai/gpt-oss-120b"


# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Define a simple nested schema (mimics the time_constraint problem)
# ─────────────────────────────────────────────────────────────────────────────

class TimeConstraint(BaseModel):
    type: Literal["TODAY", "LAST_N_DAYS", "LAST_N_MONTHS", "THIS_MONTH"]
    value: Optional[int] = None


class HelloOutput(BaseModel):
    name: str
    category: Literal["GREETING", "FAREWELL", "QUESTION"]
    time_constraint: Optional[TimeConstraint] = None
    is_urgent: bool


# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Build the response_format with json_schema
# ─────────────────────────────────────────────────────────────────────────────

schema = HelloOutput.model_json_schema()

response_format_OLD = {
    "type": "json_object"
}

response_format_NEW = {
    "type": "json_schema",
    "json_schema": {
        "name": "HelloOutput",
        "schema": schema
    }
}

print("=" * 60)
print("JSON Schema being sent to Groq:")
print("=" * 60)
print(json.dumps(schema, indent=2))
print()

# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Test with OLD format (json_object — no enforcement)
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 60)
print("TEST A: OLD format (json_object, no schema enforcement)")
print("=" * 60)

try:
    resp_old = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": "Extract structured data. Return JSON only."},
            {"role": "user", "content": "Tell me about: 'Hey, check this today!'"}
        ],
        response_format=response_format_OLD,
        temperature=0
    )
    raw_old = resp_old.choices[0].message.content
    print(f"Raw response: {raw_old}")

    # Try to validate with Pydantic
    try:
        parsed_old = HelloOutput.model_validate_json(raw_old)
        print(f"Pydantic parse: SUCCESS → {parsed_old.model_dump()}")
    except Exception as e:
        print(f"Pydantic parse: FAILED → {e}")
except Exception as e:
    print(f"API call failed: {e}")

print()

# ─────────────────────────────────────────────────────────────────────────────
# Step 4: Test with NEW format (json_schema — enforced)
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 60)
print("TEST B: NEW format (json_schema, schema enforced)")
print("=" * 60)

try:
    resp_new = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": "Extract structured data. Return JSON only."},
            {"role": "user", "content": "Tell me about: 'Hey, check this today!'"}
        ],
        response_format=response_format_NEW,
        temperature=0
    )
    raw_new = resp_new.choices[0].message.content
    print(f"Raw response: {raw_new}")

    # Try to validate with Pydantic
    try:
        parsed_new = HelloOutput.model_validate_json(raw_new)
        print(f"Pydantic parse: SUCCESS → {parsed_new.model_dump()}")
    except Exception as e:
        print(f"Pydantic parse: FAILED → {e}")
except Exception as e:
    print(f"API call failed: {e}")

print()

# ─────────────────────────────────────────────────────────────────────────────
# Step 5: The real test — does it force time_constraint to be an object?
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 60)
print("TEST C: The 'today' problem — does schema force correct shape?")
print("=" * 60)

try:
    resp_today = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": "Extract structured data from the input. Return JSON only."},
            {"role": "user", "content": "Tell me about: 'Urgent meeting today!'"}
        ],
        response_format=response_format_NEW,
        temperature=0
    )
    raw_today = resp_today.choices[0].message.content
    print(f"Raw response: {raw_today}")

    parsed_today = HelloOutput.model_validate_json(raw_today)
    print(f"Pydantic parse: SUCCESS")
    print(f"  time_constraint type: {type(parsed_today.time_constraint)}")
    if parsed_today.time_constraint:
        print(f"  time_constraint.type: {parsed_today.time_constraint.type}")
        print(f"  time_constraint.value: {parsed_today.time_constraint.value}")
    else:
        print(f"  time_constraint: null")
except Exception as e:
    print(f"FAILED: {e}")

print()
print("=" * 60)
print("DONE — compare Test A vs B vs C")
print("=" * 60)