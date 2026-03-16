"""
registry.py
-----------
Session-level memoization cache for resolved Virtual Profiles.
Prevents duplicate LLM calls for the same condition description.
"""

from typing import Optional


class VPRegistry:

    def __init__(self):
        self.store: dict = {}

    def check(self, description: str) -> Optional[dict]:
        """Return cached entry if exists, else None."""
        return self.store.get(self._key(description))

    def save(
        self,
        description: str,
        vp_name: str,
        template: str,
        child_templates: dict = None,
        extra: dict = None
    ) -> None:
        """Save a resolved VP to the cache."""
        entry = {
            "vp_name":          vp_name,
            "parent_condition": template,
            "child_templates":  child_templates or {}
        }
        if extra:
            entry.update(extra)
        self.store[self._key(description)] = entry

    def get_all(self) -> dict:
        """Return the full registry — used for debug/audit endpoint."""
        return self.store

    def clear(self) -> None:
        """Reset the registry — called via DELETE /registry."""
        self.store = {}

    def size(self) -> int:
        return len(self.store)

    @staticmethod
    def _key(description: str) -> str:
        """Normalise description to a consistent lookup key."""
        return description.lower().strip()
