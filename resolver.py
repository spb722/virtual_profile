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

MAX_DEPTH = 6


# ─────────────────────────────────────────────────────────────────────────────
# ✅ NEW: Concrete substitution helper
# ─────────────────────────────────────────────────────────────────────────────

def apply_concrete_values(template: str, extracted) -> str:
    """
    Replace ${operator} and ${value} with concrete values if present.
    """
    if (
        hasattr(extracted, "concrete_operator") and
        hasattr(extracted, "concrete_value") and
        extracted.concrete_operator and
        extracted.concrete_value
    ):
        if "${operator}" in template:
            template = template.replace("${operator}", extracted.concrete_operator)
        if "${value}" in template:
            template = template.replace("${value}", extracted.concrete_value)

    return template


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
    if operation in ("PERCENTAGE_DROP", "PERCENTAGE_CHANGE"):
        return f"({vp_b} - {vp_a}) / {vp_a} * 100 ${{operator}} ${{value}}"
    elif operation == "RATIO":
        return f"({vp_b} / {vp_a}) * 100 ${{operator}} ${{value}}"
    elif operation == "DIFFERENCE":
        return f"({vp_b} - {vp_a}) ${{operator}} ${{value}}"
    else:
        return f"({vp_b} / {vp_a}) ${{operator}} ${{value}}"


# ─────────────────────────────────────────────────────────────────────────────
# Core resolver
# ─────────────────────────────────────────────────────────────────────────────

def resolve(condition: str, registry: VPRegistry, depth: int = 0) -> ResolveResult:
    indent = "  " * depth
    logger.info("%s→ resolve [depth=%d]: '%s'", indent, depth, condition)

    if depth > MAX_DEPTH:
        raise RecursionError(
            f"Max recursion depth ({MAX_DEPTH}) exceeded for condition: '{condition}'"
        )

    # ── Cache check ──────────────────────────────────────────────────────────
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

    # ── Classification ───────────────────────────────────────────────────────
    clf = classify(condition)
    track = clf.track

    logger.info(
        "%s  Track %d (%s) [%s] — %s",
        indent, track, clf.track_label, clf.confidence, clf.reason
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Track 1
    # ─────────────────────────────────────────────────────────────────────────
    if track == 1:
        extracted = extract_track1(condition)

        vp_name   = generate_vp_name(1, extracted.model_dump())
        payload   = build_track1_payload(extracted, vp_name=vp_name)
        template  = call_template_engine(1, payload)

        # ✅ APPLY FIX
        template = apply_concrete_values(template, extracted)

        result = ResolveResult(vp_name, template, 1, depth, {})
        registry.save(condition, vp_name, template, extra={"track": 1})

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # Track 2
    # ─────────────────────────────────────────────────────────────────────────
    elif track == 2:
        extracted = extract_track2(condition)

        payload   = build_track2_payload(extracted)
        template  = call_template_engine(2, payload)

        # ✅ APPLY FIX
        template = apply_concrete_values(template, extracted)

        vp_name   = generate_vp_name(2, extracted.model_dump())

        result = ResolveResult(vp_name, template, 2, depth, {})
        registry.save(condition, vp_name, template, extra={"track": 2})

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # Track 3
    # ─────────────────────────────────────────────────────────────────────────
    elif track == 3:
        extracted = extract_track3(condition)

        payload   = build_track3_payload(extracted)
        template  = call_template_engine(3, payload)

        # ✅ APPLY FIX
        template = apply_concrete_values(template, extracted)

        vp_name   = generate_vp_name(3, extracted.model_dump())

        result = ResolveResult(vp_name, template, 3, depth, {})
        registry.save(condition, vp_name, template, extra={"track": 3})

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # Track 4 (NO CHANGE)
    # ─────────────────────────────────────────────────────────────────────────
    elif track == 4:
        extracted = extract_track4(condition)

        result_a = resolve(extracted.operand_a, registry, depth + 1)
        result_b = resolve(extracted.operand_b, registry, depth + 1)

        vp_a = result_a.vp_name
        vp_b = result_b.vp_name

        template = _compose_track4_formula(extracted.operation, vp_a, vp_b)
        vp_name  = generate_vp_name(4, extracted.model_dump(), vp_a, vp_b)

        child_templates = {
            vp_a: result_a.parent_condition,
            vp_b: result_b.parent_condition,
            **result_a.child_templates,
            **result_b.child_templates,
        }

        result = ResolveResult(vp_name, template, 4, depth, child_templates)

        registry.save(condition, vp_name, template,
                      child_templates=child_templates, extra={"track": 4})

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # Track 5
    # ─────────────────────────────────────────────────────────────────────────
    elif track == 5:
        extracted = extract_track5(condition)

        payload   = build_track5_payload(extracted)
        template  = call_template_engine(5, payload)

        # ✅ APPLY FIX
        template = apply_concrete_values(template, extracted)

        vp_name   = generate_vp_name(5, extracted.model_dump())

        result = ResolveResult(vp_name, template, 5, depth, {})
        registry.save(condition, vp_name, template, extra={"track": 5})

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # Track 6
    # ─────────────────────────────────────────────────────────────────────────
    elif track == 6:
        extracted = extract_track6(condition)

        payload   = build_track6_payload(extracted)
        template  = call_template_engine(6, payload)

        # ✅ APPLY FIX
        template = apply_concrete_values(template, extracted)

        vp_name   = generate_vp_name(6, extracted.model_dump())

        result = ResolveResult(vp_name, template, 6, depth, {})
        registry.save(condition, vp_name, template, extra={"track": 6})

        return result

    else:
        raise ValueError(f"Unknown track number: {track}")