"""
Tests for the Hadoop AI Agent.
Run with: pytest tests/ -v
"""

import pytest
from unittest.mock import MagicMock, patch
from agent.goal_comparator import GoalComparator
from agent.tool_validator import ToolValidator
from config.goal_state import GOAL_STATE


# ── GoalComparator Tests ────────────────────────────────────────────────────

class TestGoalComparator:
    def setup_method(self):
        self.comparator = GoalComparator(GOAL_STATE)

    def test_no_gaps_when_all_satisfied(self):
        state = {
            "java_installed": True,
            "java_version": "11.0.18",
            "hadoop_installed": True,
            "hadoop_version": "3.3.6",
            "java_home_configured": True,
            "namenode_running": True,
            "datanode_running": True,
            "replication_factor": 3,
            "hdfs_safemode": False,
            "critical_log_errors": False,
        }
        gaps = self.comparator.find_gaps(state)
        assert gaps == []

    def test_detects_java_not_installed(self):
        state = {"java_installed": False}
        gaps = self.comparator.find_gaps(state)
        fields = [g["field"] for g in gaps]
        assert "java_installed" in fields

    def test_detects_namenode_not_running(self):
        state = {"namenode_running": False}
        gaps = self.comparator.find_gaps(state)
        fields = [g["field"] for g in gaps]
        assert "namenode_running" in fields

    def test_version_prefix_match(self):
        # "11.0.18" should satisfy goal "11"
        from agent.goal_comparator import GoalComparator
        comp = GoalComparator({"java_version": "11"})
        gaps = comp.find_gaps({"java_version": "11.0.18"})
        assert gaps == []

    def test_replication_factor_below_goal(self):
        from agent.goal_comparator import GoalComparator
        comp = GoalComparator({"replication_factor": 3})
        gaps = comp.find_gaps({"replication_factor": 1})
        assert len(gaps) == 1
        assert gaps[0]["field"] == "replication_factor"


# ── ToolValidator Tests ─────────────────────────────────────────────────────

class TestToolValidator:
    def setup_method(self):
        self.validator = ToolValidator()

    def test_valid_tool_passes(self):
        decision = {"tool": "install_java", "arguments": {"version": "11"}}
        valid, reason = self.validator.validate(decision)
        assert valid is True

    def test_unknown_tool_blocked(self):
        decision = {"tool": "rm_rf_everything", "arguments": {}}
        valid, reason = self.validator.validate(decision)
        assert valid is False
        assert "Unknown tool" in reason

    def test_destructive_tool_blocked(self):
        decision = {"tool": "format_namenode", "arguments": {}}
        valid, reason = self.validator.validate(decision)
        assert valid is False
        assert "destructive" in reason.lower()

    def test_missing_required_arg_blocked(self):
        decision = {"tool": "install_java", "arguments": {}}  # missing "version"
        valid, reason = self.validator.validate(decision)
        assert valid is False
        assert "version" in reason

    def test_invalid_arg_value_blocked(self):
        decision = {"tool": "install_java", "arguments": {"version": "6"}}  # not allowed
        valid, reason = self.validator.validate(decision)
        assert valid is False

    def test_start_hdfs_needs_no_args(self):
        decision = {"tool": "start_hdfs", "arguments": {}}
        valid, reason = self.validator.validate(decision)
        assert valid is True

    def test_empty_decision_blocked(self):
        valid, reason = self.validator.validate({})
        assert valid is False


# ── Agent Integration Test (dry run) ────────────────────────────────────────

class TestAgentDryRun:
    def test_agent_runs_without_executing_tools(self):
        from agent.agent import HadoopAgent
        from config.settings import Settings

        # Mock LLM to return a deterministic decision
        settings = Settings(openrouter_api_key="fake-key", dry_run=True, max_iterations=3)
        agent = HadoopAgent(settings)

        # Mock state: java not installed
        agent.state_detector.collect = MagicMock(return_value={
            "java_installed": False,
            "java_version": None,
            "hadoop_installed": False,
            "hadoop_version": None,
            "java_home_configured": False,
            "namenode_running": False,
            "datanode_running": False,
            "replication_factor": None,
            "hdfs_safemode": False,
            "critical_log_errors": False,
        })

        # Mock LLM to return install_java decision
        agent.llm_reasoner.decide = MagicMock(return_value={
            "reasoning": "Java not installed.",
            "tool": "install_java",
            "arguments": {"version": "11"},
        })

        result = agent.run()
        assert result["status"] in ("max_iterations_reached", "success", "error")
        assert len(result["log"]) > 0