"""
Goal Comparator — Compares current state against goal state.
Returns list of gaps (what is not yet satisfied).
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class GoalComparator:
    def __init__(self, goal_state: dict):
        self.goal_state = goal_state

    def find_gaps(self, current_state: dict) -> list[dict]:
        """
        Return list of gaps between current state and goal state.
        Each gap: {field, expected, actual}
        """
        gaps = []

        for key, expected in self.goal_state.items():
            actual = current_state.get(key)

            if not self._satisfies(actual, expected):
                gaps.append({
                    "field": key,
                    "expected": expected,
                    "actual": actual,
                })
                logger.debug(f"GAP: {key} | expected={expected} | actual={actual}")

        return gaps

    def _satisfies(self, actual: Any, expected: Any) -> bool:
        """Flexible comparison supporting bool, version prefix, int."""
        if expected is None:
            return True  # No constraint

        if isinstance(expected, bool):
            return actual == expected

        if isinstance(expected, int):
            if actual is None:
                return False
            return int(actual) >= expected

        if isinstance(expected, str):
            if actual is None:
                return False
            # Allow prefix matching for versions e.g. "11" matches "11.0.18"
            return str(actual).startswith(str(expected))

        return actual == expected