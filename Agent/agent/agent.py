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

    def run(self) -> dict:
        """Main agent loop."""
        logger.info("🚀 Hadoop AI Agent starting...")
        iteration = 0

        while iteration < self.max_iterations:
            iteration += 1
            logger.info(f"\n{'='*50}")
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

            # Step 4: LLM reasoning — pick best tool
            decision = self.llm_reasoner.decide(current_state, gaps)
            logger.info(f"🧠 LLM Decision: {json.dumps(decision, indent=2)}")

            if not decision or "tool" not in decision:
                logger.error("❌ LLM returned invalid decision. Stopping.")
                return {"status": "error", "reason": "invalid_llm_decision", "log": self.action_log}

            # Step 5: Validate tool call
            is_valid, reason = self.tool_validator.validate(decision)
            if not is_valid:
                logger.error(f"🚫 Tool validation failed: {reason}")
                self.action_log.append({"iteration": iteration, "action": "BLOCKED", "reason": reason})
                continue

            # Step 6: Execute tool
            tool_name = decision["tool"]
            arguments = decision.get("arguments", {})
            logger.info(f"⚙️  Executing tool: {tool_name} with args {arguments}")

            result = self.tool_executor.execute(tool_name, arguments)
            logger.info(f"📋 Tool Result: {result}")

            # Step 7: Log action
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