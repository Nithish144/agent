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
        self._last_result: dict = {}
        # Track consecutive analyze_logs calls that returned 0 errors
        self._analyze_logs_zero_count = 0

    # ------------------------------------------------------------------
    # Consecutive-failure counter
    # ------------------------------------------------------------------
    def _count_consecutive_tool_failures(self, tool_name: str, failure_key: str = "namenode_running") -> int:
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
    # Daemon-error guard
    # ------------------------------------------------------------------
    def _daemon_error_override(self) -> Optional[str]:
        daemon_error = self._last_result.get("daemon_error")
        ssh_ready    = self._last_result.get("ssh_ready")
        ssh_fix      = self._last_result.get("ssh_fix", "")

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

        # RULE D2: repeated start_hdfs failures with no harvested error
        consecutive = self._count_consecutive_tool_failures("start_hdfs", "namenode_running")
        if consecutive >= 2:
            return (
                f"OVERRIDE — start_hdfs has failed {consecutive} times in a row "
                "with no daemon_error. "
                "You MUST call analyze_logs now. DO NOT call start_hdfs again."
            )

        # RULE D4: analyze_logs returned 0 errors 2+ times in a row but
        # critical_log_errors is still True — the log detector is seeing
        # stale/benign errors (e.g. SIGTERM from a previous shutdown).
        # Suppress the critical_log_errors gap so the agent stops looping.
        if self._analyze_logs_zero_count >= 2:
            return (
                "OVERRIDE — analyze_logs has returned 0 errors "
                f"{self._analyze_logs_zero_count} times in a row. "
                "The remaining critical_log_errors are stale pre-startup entries "
                "(e.g. SIGTERM signals from previous shutdown) — NOT real problems. "
                "All daemons are running. "
                "IGNORE the critical_log_errors gap completely. "
                "If all other goals are met, declare SUCCESS. "
                "DO NOT call analyze_logs again."
            )

        return None

    # ------------------------------------------------------------------
    # Main agent loop
    # ------------------------------------------------------------------
    def run(self) -> dict:
        logger.info("🚀 Hadoop AI Agent starting...")

        # Bootstrap: write daemon user vars so start/stop scripts work
        # from any terminal without manual exports — even if cluster is
        # already healthy and start_hdfs is never called this run.
        try:
            self.tool_executor._write_daemon_users_to_hadoop_env()
            hadoop_home = self.tool_executor._hh()
            java_home   = self.tool_executor._resolve_java_home() if hasattr(
                self.tool_executor, '_resolve_java_home') else None
            if java_home:
                self.tool_executor._write_profile_d(hadoop_home, java_home)
        except Exception as e:
            logger.warning(f"Could not bootstrap env vars: {e}")

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

            # Step 3: If ONLY remaining gap is critical_log_errors AND
            # analyze_logs has confirmed 0 errors 2+ times → treat as done.
            if self._analyze_logs_zero_count >= 2:
                real_gaps = [g for g in gaps if g.get("field") != "critical_log_errors"]
                if not real_gaps:
                    logger.info(
                        "✅ GOAL STATE ACHIEVED — only stale log errors remain "
                        f"(analyze_logs returned 0 errors {self._analyze_logs_zero_count}x). "
                        "Agent stopping."
                    )
                    return {"status": "success", "iterations": iteration, "log": self.action_log}

            # Step 4: Check if goal achieved
            if not gaps:
                logger.info("✅ GOAL STATE ACHIEVED — Agent stopping.")
                return {"status": "success", "iterations": iteration, "log": self.action_log}

            # Step 5: Daemon failure / loop override
            override_instruction = self._daemon_error_override()
            if override_instruction:
                logger.warning(f"🛑 Override active: {override_instruction[:120]}...")

            # Step 6: LLM reasoning
            decision = self.llm_reasoner.decide(
                current_state,
                gaps,
                override_instruction=override_instruction,
            )
            logger.info(f"🧠 LLM Decision: {json.dumps(decision, indent=2)}")

            if not decision or "tool" not in decision:
                logger.error("❌ LLM returned invalid decision. Stopping.")
                return {"status": "error", "reason": "invalid_llm_decision", "log": self.action_log}

            tool_name = decision["tool"]

            # Step 7: Hard-block — never retry start_hdfs when override active
            if override_instruction and tool_name == "start_hdfs":
                logger.error("🚫 LLM ignored override and chose start_hdfs. Forcing analyze_logs.")
                decision["tool"]      = "analyze_logs"
                decision["arguments"] = {}
                decision["reasoning"] = "[agent override] forced analyze_logs after repeated start_hdfs failure"
                tool_name = "analyze_logs"

            # Step 8: Validate
            is_valid, reason = self.tool_validator.validate(decision)
            if not is_valid:
                logger.error(f"🚫 Tool validation failed: {reason}")
                self.action_log.append({"iteration": iteration, "action": "BLOCKED", "reason": reason})
                continue

            # Step 9: Execute
            arguments = decision.get("arguments", {})
            logger.info(f"⚙️  Executing tool: {tool_name} with args {arguments}")
            result = self.tool_executor.execute(tool_name, arguments)
            logger.info(f"📋 Tool Result: {result}")

            # Step 10: Track analyze_logs zero-error streak
            if tool_name == "analyze_logs":
                if result.get("errors_found", 1) == 0:
                    self._analyze_logs_zero_count += 1
                    logger.info(
                        f"ℹ️  analyze_logs returned 0 errors "
                        f"({self._analyze_logs_zero_count} consecutive time(s))"
                    )
                else:
                    self._analyze_logs_zero_count = 0  # reset on real errors
            else:
                # Reset counter when any other tool runs
                self._analyze_logs_zero_count = 0

            # Step 11: Persist last result
            self._last_result = result

            # Step 12: Log action
            self.action_log.append({
                "iteration":  iteration,
                "state_gaps": gaps,
                "reasoning":  decision.get("reasoning"),
                "tool":       tool_name,
                "arguments":  arguments,
                "result":     result,
            })

            time.sleep(self.settings.loop_delay_seconds)

        logger.warning("⚠️  Max iterations reached without achieving goal state.")
        return {"status": "max_iterations_reached", "log": self.action_log}
