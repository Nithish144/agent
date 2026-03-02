"""
State Detector — Collects the real current state of the Hadoop cluster.
In production: runs actual shell commands / API checks.
In simulation: returns mocked state for safe testing.
"""

import subprocess
import shutil
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class StateDetector:
    def collect(self) -> dict:
        """Collect full cluster state snapshot."""
        return {
            "java_installed": self._check_java(),
            "java_version": self._get_java_version(),
            "hadoop_installed": self._check_hadoop(),
            "hadoop_version": self._get_hadoop_version(),
            "java_home_configured": self._check_java_home(),
            "namenode_running": self._check_process("NameNode"),
            "datanode_running": self._check_process("DataNode"),
            "replication_factor": self._get_replication_factor(),
            "hdfs_safemode": self._check_safemode(),
            "critical_log_errors": self._check_log_errors(),
            "disk_usage_percent": self._get_disk_usage(),
        }

    def _check_java(self) -> bool:
        return shutil.which("java") is not None

    def _get_java_version(self) -> Optional[str]:
        try:
            result = subprocess.run(
                ["java", "-version"], capture_output=True, text=True, timeout=5
            )
            output = result.stderr or result.stdout
            if "version" in output:
                return output.split('"')[1] if '"' in output else None
        except Exception:
            pass
        return None

    def _check_hadoop(self) -> bool:
        return shutil.which("hadoop") is not None

    def _get_hadoop_version(self) -> Optional[str]:
        try:
            result = subprocess.run(
                ["hadoop", "version"], capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                first_line = result.stdout.splitlines()[0]
                return first_line.split()[-1] if first_line else None
        except Exception:
            pass
        return None

    def _check_java_home(self) -> bool:
        import os
        java_home = os.environ.get("JAVA_HOME", "")
        return bool(java_home) and shutil.which(f"{java_home}/bin/java") is not None

    def _check_process(self, process_name: str) -> bool:
        try:
            result = subprocess.run(
                ["jps"], capture_output=True, text=True, timeout=5
            )
            return process_name in result.stdout
        except Exception:
            return False

    def _get_replication_factor(self) -> Optional[int]:
        try:
            result = subprocess.run(
                ["hdfs", "dfsadmin", "-report"], capture_output=True, text=True, timeout=10
            )
            for line in result.stdout.splitlines():
                if "Replication" in line:
                    parts = line.split(":")
                    if len(parts) > 1:
                        return int(parts[1].strip())
        except Exception:
            pass
        return None

    def _check_safemode(self) -> bool:
        try:
            result = subprocess.run(
                ["hdfs", "dfsadmin", "-safemode", "get"],
                capture_output=True, text=True, timeout=10
            )
            return "ON" in result.stdout
        except Exception:
            return False

    def _check_log_errors(self) -> bool:
        """Check Hadoop logs for FATAL/ERROR entries."""
        import os
        log_dirs = [
            "/usr/local/hadoop/logs",
            "/opt/hadoop/logs",
            os.environ.get("HADOOP_LOG_DIR", ""),
        ]
        for log_dir in log_dirs:
            if not log_dir or not os.path.isdir(log_dir):
                continue
            for fname in os.listdir(log_dir):
                if not fname.endswith(".log"):
                    continue
                fpath = os.path.join(log_dir, fname)
                try:
                    with open(fpath, "r", errors="ignore") as f:
                        # Check last 200 lines only
                        lines = f.readlines()[-200:]
                    for line in lines:
                        if "FATAL" in line or "ERROR" in line:
                            return True
                except Exception:
                    pass
        return False

    def _get_disk_usage(self) -> Optional[int]:
        try:
            result = subprocess.run(
                ["df", "-h", "/"], capture_output=True, text=True, timeout=5
            )
            lines = result.stdout.splitlines()
            if len(lines) > 1:
                parts = lines[1].split()
                if len(parts) >= 5:
                    return int(parts[4].replace("%", ""))
        except Exception:
            pass
        return None