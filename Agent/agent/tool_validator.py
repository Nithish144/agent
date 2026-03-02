"""
Tool Validator — Safety layer that validates every LLM tool decision
before execution. Blocks destructive or unknown operations.
"""

import logging
from tools.registry import TOOL_REGISTRY, DESTRUCTIVE_TOOLS

logger = logging.getLogger(__name__)


class ToolValidator:
    def validate(self, decision: dict) -> tuple[bool, str]:
        """
        Returns (is_valid, reason).
        Blocks unknown tools, destructive tools, malformed decisions.
        """
        tool = decision.get("tool")
        arguments = decision.get("arguments", {})

        # Must have a tool name
        if not tool:
            return False, "No tool specified in decision."

        # Tool must be in registry
        if tool not in TOOL_REGISTRY:
            return False, f"Unknown tool: '{tool}'. Not in registry."

        # Block destructive tools (require human approval)
        if tool in DESTRUCTIVE_TOOLS:
            return False, (
                f"Tool '{tool}' is destructive and requires explicit human approval. "
                "Set HUMAN_APPROVED=true in arguments to proceed."
            )

        # Check required arguments
        tool_def = TOOL_REGISTRY[tool]
        required_args = tool_def.get("required_args", [])
        for arg in required_args:
            if arg not in arguments:
                return False, f"Tool '{tool}' missing required argument: '{arg}'."

        # Validate argument values
        valid, msg = self._validate_args(tool, arguments, tool_def)
        if not valid:
            return False, msg

        return True, "OK"

    def _validate_args(self, tool: str, arguments: dict, tool_def: dict) -> tuple[bool, str]:
        """Validate argument values against allowed values."""
        allowed = tool_def.get("allowed_args", {})

        for arg, value in arguments.items():
            if arg in allowed:
                allowed_values = allowed[arg]
                if value not in allowed_values:
                    return False, (
                        f"Argument '{arg}={value}' not allowed for tool '{tool}'. "
                        f"Allowed: {allowed_values}"
                    )

        return True, "OK"