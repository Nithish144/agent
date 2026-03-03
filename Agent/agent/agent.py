"""
Hadoop AI Agent — Goal-Based Infrastructure Agent
Core agent loop: Detect → Compare → Reason → Validate → Execute → Repeat
"""
import json
import time
import logging
from typing import Optional
from agent.state_detector import StateDetector
from agent.goal_comparator import GoalComparator
from agent.llm_reasoner import LLMReasoner
from agent.tool_validator import ToolValidator
from tools.executor import ToolExecutor
from config.goal_state import GOAL_STATE
from config.settings import Settings

logger = logging.getLogger(__name__)


class HadoopAgent:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.state_detector = StateDetector()
        self.goal_comparator = GoalComparator(GOAL_STATE)
        self.llm_reasoner = LLMReasoner(settings)
        self.tool_validator = ToolValidator()
        self.tool_executor = ToolExecutor(dry_run=settings.dry_run)
        self.action_log = []
        self.max_iterations = settings.max_iterations
        # Track last tool result for daemon_error injection
        self._last_result: dict = {}

    # ------------------------------------------------------------------
    # Consecutive-failure counter
    # ------------------------------------------------------------------
    def _count_consecutive_tool_failures(self, tool_name: str, failure_key: str = "namenode_running") -> int:
        """
        Count how many times `tool_name` was called most recently without
        achieving `failure_key` == True.  Resets as soon as a different tool
        or a successful result appears.
        """
        count = 0
        for entry in reversed(self.action_log):
            if entry.get("tool") != tool_name:
                break
            result = entry.get("result", {})
            if result.get(failure_key) is True:
                break
            count += 1
        return count

    # ------------------------------------------------------------------
    # Daemon-error guard — injected into LLM context before each call
    # ------------------------------------------------------------------
    def _daemon_error_override(self) -> Optional[str]:
        """
        Returns a hard override instruction string when a daemon failure
        pattern is detected, or None when normal decision flow should apply.
        Called before the LLM is asked to decide.
        """
        daemon_error = self._last_result.get("daemon_error")
        ssh_ready = self._last_result.get("ssh_ready")
        ssh_fix = self._last_result.get("ssh_fix", "")

        # RULE D3: SSH not ready
        if ssh_ready is False:
            return (
                "OVERRIDE — SSH not ready. "
                "You MUST call request_human_approval with this reason:\n"
                f"{ssh_fix}"
            )

        # RULE D1: daemon crashed (logs harvested)
        if daemon_error:
            return (
                "OVERRIDE — start_hdfs returned daemon_error. "
                "DO NOT call start_hdfs again.\n"
                f"daemon_error content:\n{daemon_error}\n"
                "Diagnose the error above and call the correct fix tool per DAEMON FAILURE RULES."
            )

        # RULE D2: repeated failures with no harvested error
        consecutive = self._count_consecutive_tool_failures("start_hdfs", "namenode_running")
        if consecutive >= 2:
            return (
                f"OVERRIDE — start_hdfs has failed {consecutive} times in a row "
                "with no daemon_error. "
                "You MUST call analyze_logs now. DO NOT call start_hdfs again."
            )

        return None

    # ------------------------------------------------------------------
    # Main agent loop
    # ------------------------------------------------------------------
    def run(self) -> dict:
        """Main agent loop."""
        logger.info("🚀 Hadoop AI Agent starting...")

        # Bootstrap: write HDFS daemon user vars to hadoop-env.sh so that
        # start-dfs.sh / stop-dfs.sh work from any terminal without errors,
        # even when the cluster is already healthy and start_hdfs is never
        # called by the agent in this run.
        # Uses getpwuid(getuid()) — correct user even when run under sudo,
        # immune to SUDO_USER pointing to an unrelated account (e.g. kc-internal).
        try:
            self.tool_executor._write_daemon_users_to_hadoop_env()
        except Exception as e:
            logger.warning(f"Could not bootstrap daemon user vars: {e}")

        iteration = 0
        while iteration < self.max_iterations:
            iteration += 1
            logger.info(f"\n{'=' * 50}")
            logger.info(f"🔄 Iteration {iteration}/{self.max_iterations}")

            # Step 1: Detect current state
            current_state = self.state_detector.collect()
            logger.info(f"📊 Current State: {json.dumps(current_state, indent=2)}")

            # Step 2: Compare with goal state
            gaps = self.goal_comparator.find_gaps(current_state)
            logger.info(f"🎯 Gaps Found: {gaps}")

            # Step 3: Check if goal achieved
            if not gaps:
                logger.info("✅ GOAL STATE ACHIEVED — Agent stopping.")
                return {"status": "success", "iterations": iteration, "log": self.action_log}

            # Step 4: Check for daemon failure overrides BEFORE asking the LLM.
            # This prevents the model from blindly re-picking start_hdfs.
            override_instruction = self._daemon_error_override()
            if override_instruction:
                logger.warning(f"🛑 Daemon override active: {override_instruction[:120]}...")

            # Step 5: LLM reasoning — pick best tool
            decision = self.llm_reasoner.decide(
                current_state,
                gaps,
                override_instruction=override_instruction,
            )
            logger.info(f"🧠 LLM Decision: {json.dumps(decision, indent=2)}")

            if not decision or "tool" not in decision:
                logger.error("❌ LLM returned invalid decision. Stopping.")
                return {"status": "error", "reason": "invalid_llm_decision", "log": self.action_log}

            # Step 6: Hard-block: never let the LLM retry start_hdfs when an
            # override is active — catches it even if the LLM ignores the prompt.
            tool_name = decision["tool"]
            if override_instruction and tool_name == "start_hdfs":
                logger.error(
                    "🚫 LLM ignored daemon override and chose start_hdfs again. "
                    "Forcing analyze_logs."
                )
                decision["tool"] = "analyze_logs"
                decision["arguments"] = {}
                decision["reasoning"] = "[agent override] forced analyze_logs after repeated start_hdfs failure"
                tool_name = "analyze_logs"

            # Step 7: Validate tool call
            is_valid, reason = self.tool_validator.validate(decision)
            if not is_valid:
                logger.error(f"🚫 Tool validation failed: {reason}")
                self.action_log.append({"iteration": iteration, "action": "BLOCKED", "reason": reason})
                continue

            # Step 8: Execute tool
            arguments = decision.get("arguments", {})
            logger.info(f"⚙️  Executing tool: {tool_name} with args {arguments}")
            result = self.tool_executor.execute(tool_name, arguments)
            logger.info(f"📋 Tool Result: {result}")

            # Step 9: Persist last result so override logic can inspect it next iteration
            self._last_result = result

            # Step 10: Log action
            self.action_log.append({
                "iteration": iteration,
                "state_gaps": gaps,
                "reasoning": decision.get("reasoning"),
                "tool": tool_name,
                "arguments": arguments,
                "result": result,
            })

            # Brief pause before re-checking state
            time.sleep(self.settings.loop_delay_seconds)

        logger.warning("⚠️  Max iterations reached without achieving goal state.")
        return {"status": "max_iterations_reached", "log": self.action_log}
