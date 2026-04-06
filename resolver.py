"""
resolver.py
-----------
Core recursive + memoized VP resolution engine.

Flow for every condition:
  1. Registry check  → cache hit? return immediately
  2. Classify        → which track?
  3. Extract         → track-specific agent
  4. Branch
       Tracks 1/2/3/5 → LEAF  → build payload → call template engine → name → save → return
       Track 4        → COMPOSITE → recurse into operand_a and operand_b → compose formula → save → return
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Dict

from registry import VPRegistry
from agents import (
    classify,
    extract_track1, extract_track2, extract_track3,
    extract_track4, extract_track5, extract_track6,
)
from name_generator import generate_vp_name
from template_client import (
    call_template_engine,
    build_track1_payload,
    build_track2_payload,
    build_track3_payload,
    build_track5_payload,
    build_track6_payload,
)

logger = logging.getLogger(__name__)

MAX_DEPTH = 6   # safety guard against infinite recursion


# ─────────────────────────────────────────────────────────────────────────────
# Result dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ResolveResult:
    vp_name:          str
    parent_condition: str
    track:            int
    depth:            int
    child_templates:  Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "vp_name":          self.vp_name,
            "parent_condition": self.parent_condition,
            "track":            self.track,
            "depth":            self.depth,
            "child_templates":  self.child_templates,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Track 4 formula composer
# ─────────────────────────────────────────────────────────────────────────────

def _compose_track4_formula(operation: str, vp_a: str, vp_b: str) -> str:
    """
    Build the PARENT_CONDITION formula for Track 4.
    operand_a (vp_a) is always the BASE  (earlier period / denominator).
    operand_b (vp_b) is always the COMPARISON (later period / numerator).
    """
    if operation in ("PERCENTAGE_DROP", "PERCENTAGE_CHANGE"):
        return (
            f"({vp_b} - {vp_a}) / {vp_a} * 100 ${{operator}} ${{value}}"
        )
    elif operation == "RATIO":
        return (
            f"({vp_b} / {vp_a}) * 100 ${{operator}} ${{value}}"
        )
    elif operation == "DIFFERENCE":
        return (
            f"({vp_b} - {vp_a}) ${{operator}} ${{value}}"
        )
    else:
        # Fallback generic formula
        return (
            f"({vp_b} / {vp_a}) ${{operator}} ${{value}}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Core resolver
# ─────────────────────────────────────────────────────────────────────────────

def resolve(
    condition: str,
    registry: VPRegistry,
    depth: int = 0
) -> ResolveResult:
    """
    Recursively resolve a natural language condition into a VP name
    and its PARENT_CONDITION template.

    Parameters
    ----------
    condition : natural language KPI/condition description
    registry  : shared VPRegistry instance (memoization store)
    depth     : current recursion depth (starts at 0)

    Returns
    -------
    ResolveResult
    """
    indent = "  " * depth
    logger.info("%s→ resolve [depth=%d]: '%s'", indent, depth, condition)

    # ── Guard ─────────────────────────────────────────────────────────────────
    if depth > MAX_DEPTH:
        raise RecursionError(
            f"Max recursion depth ({MAX_DEPTH}) exceeded for condition: '{condition}'"
        )

    # ── STEP 1: Registry check (memoization) ──────────────────────────────────
    cached = registry.check(condition)
    if cached:
        logger.info("%s  ✓ Cache hit → %s", indent, cached["vp_name"])
        return ResolveResult(
            vp_name          = cached["vp_name"],
            parent_condition = cached["parent_condition"],
            track            = cached.get("track", 0),
            depth            = depth,
            child_templates  = cached.get("child_templates", {})
        )

    # ── STEP 2: Classify ──────────────────────────────────────────────────────
    clf = classify(condition)
    track = clf.track
    logger.info(
        "%s  Track %d (%s) [%s] — %s",
        indent, track, clf.track_label, clf.confidence, clf.reason
    )

    # ── STEP 3: Branch by track ───────────────────────────────────────────────

    # ── Track 1 — TIME SERIES (leaf) ──────────────────────────────────────────
    if track == 1:
        extracted = extract_track1(condition)
        logger.info("%s  Extracted Track 1: %s", indent, json.dumps(extracted.model_dump()))
        vp_name   = generate_vp_name(1, extracted.model_dump())
        payload   = build_track1_payload(extracted, vp_name=vp_name)
        template  = call_template_engine(1, payload)

        result = ResolveResult(
            vp_name          = vp_name,
            parent_condition = template,
            track            = 1,
            depth            = depth,
            child_templates  = {}
        )
        registry.save(condition, vp_name, template, extra={"track": 1})
        logger.info("%s  VP_OUTPUT %s", indent, json.dumps({"condition": condition, "track": 1, "vp_name": vp_name, "parent_condition": template}))
        return result

    # ── Track 2 — STATIC FLAG (leaf) ──────────────────────────────────────────
    elif track == 2:
        extracted = extract_track2(condition)
        payload   = build_track2_payload(extracted)
        template  = call_template_engine(2, payload)
        vp_name   = generate_vp_name(2, extracted.model_dump())

        result = ResolveResult(
            vp_name          = vp_name,
            parent_condition = template,
            track            = 2,
            depth            = depth,
            child_templates  = {}
        )
        registry.save(condition, vp_name, template, extra={"track": 2})
        logger.info("%s  VP_OUTPUT %s", indent, json.dumps({"condition": condition, "track": 2, "vp_name": vp_name, "parent_condition": template}))
        return result

    # ── Track 3 — SNAPSHOT (leaf) ─────────────────────────────────────────────
    elif track == 3:
        extracted = extract_track3(condition)
        payload   = build_track3_payload(extracted)
        template  = call_template_engine(3, payload)
        vp_name   = generate_vp_name(3, extracted.model_dump())

        result = ResolveResult(
            vp_name          = vp_name,
            parent_condition = template,
            track            = 3,
            depth            = depth,
            child_templates  = {}
        )
        registry.save(condition, vp_name, template, extra={"track": 3})
        logger.info("%s  VP_OUTPUT %s", indent, json.dumps({"condition": condition, "track": 3, "vp_name": vp_name, "parent_condition": template}))
        return result

    # ── Track 4 — COMPARATIVE (composite — recursive) ─────────────────────────
    elif track == 4:
        extracted = extract_track4(condition)
        logger.info(
            "%s  Decomposing Track 4:\n%s    operand_a [Track %d]: %s\n%s    operand_b [Track %d]: %s",
            indent,
            indent, extracted.operand_a_track, extracted.operand_a,
            indent, extracted.operand_b_track, extracted.operand_b
        )

        # Recursive resolution of both operands
        result_a = resolve(extracted.operand_a, registry, depth + 1)
        result_b = resolve(extracted.operand_b, registry, depth + 1)

        vp_a = result_a.vp_name
        vp_b = result_b.vp_name

        # Compose Track 4 formula
        template = _compose_track4_formula(extracted.operation, vp_a, vp_b)

        # Generate composite VP name
        vp_name  = generate_vp_name(4, extracted.model_dump(), vp_a, vp_b)

        # Collect all child templates (flatten nested children too)
        child_templates = {
            vp_a: result_a.parent_condition,
            vp_b: result_b.parent_condition,
            **result_a.child_templates,
            **result_b.child_templates,
        }

        result = ResolveResult(
            vp_name          = vp_name,
            parent_condition = template,
            track            = 4,
            depth            = depth,
            child_templates  = child_templates
        )
        registry.save(condition, vp_name, template,
                      child_templates=child_templates, extra={"track": 4})
        logger.info("%s  VP_OUTPUT %s", indent, json.dumps({"condition": condition, "track": 4, "vp_name": vp_name, "parent_condition": template, "child_vps": [vp_a, vp_b]}))
        return result

    # ── Track 5 — PARAMETERIZED (leaf) ────────────────────────────────────────
    elif track == 5:
        extracted = extract_track5(condition)
        logger.info("%s  Track5 extracted: %s", indent, extracted.model_dump_json())
        payload   = build_track5_payload(extracted)
        template  = call_template_engine(5, payload)
        vp_name   = generate_vp_name(5, extracted.model_dump())

        result = ResolveResult(
            vp_name          = vp_name,
            parent_condition = template,
            track            = 5,
            depth            = depth,
            child_templates  = {}
        )
        registry.save(condition, vp_name, template, extra={"track": 5})
        logger.info("%s  VP_OUTPUT %s", indent, json.dumps({"condition": condition, "track": 5, "vp_name": vp_name, "parent_condition": template}))
        return result

    # ── Track 6 — JOIN_CHECK (leaf) ───────────────────────────────────────────
    elif track == 6:
        extracted = extract_track6(condition)
        logger.info("%s  Extracted Track 6: %s", indent, json.dumps(extracted.model_dump()))
        payload   = build_track6_payload(extracted)
        template  = call_template_engine(6, payload)
        vp_name   = generate_vp_name(6, extracted.model_dump())

        result = ResolveResult(
            vp_name          = vp_name,
            parent_condition = template,
            track            = 6,
            depth            = depth,
            child_templates  = {}
        )
        registry.save(condition, vp_name, template, extra={"track": 6})
        logger.info("%s  VP_OUTPUT %s", indent, json.dumps({"condition": condition, "track": 6, "vp_name": vp_name, "parent_condition": template}))
        return result

    else:
        raise ValueError(f"Unknown track number: {track} for condition: '{condition}'")
