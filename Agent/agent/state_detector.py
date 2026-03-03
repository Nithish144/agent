"""
State Detector — Collects the real current state of the Hadoop cluster.
In production: runs actual shell commands / API checks.
In simulation: returns mocked state for safe testing.
"""

import os
import subprocess
import shutil
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

HADOOP_HOME = os.environ.get("HADOOP_HOME", "/usr/local/hadoop")


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
        java_home = os.environ.get("JAVA_HOME", "")
        return bool(java_home) and os.path.isfile(os.path.join(java_home, "bin", "java"))

    def _check_process(self, process_name: str) -> bool:
        """
        Per-line exact suffix match — avoids 'NameNode' matching 'SecondaryNameNode'.
        """
        try:
            result = subprocess.run(
                ["jps"], capture_output=True, text=True, timeout=5
            )
            return any(
                line.strip().endswith(process_name)
                for line in result.stdout.splitlines()
            )
        except Exception:
            return False

    def _get_replication_factor(self) -> Optional[int]:
        """
        Read replication factor directly from hdfs-site.xml.

        The old approach used `hdfs dfsadmin -report` which only returns
        replication data when live blocks exist — always null on a fresh cluster.
        Reading the config file is instant, reliable, and works before any data
        is written to HDFS.
        """
        xml_path = os.path.join(HADOOP_HOME, "etc", "hadoop", "hdfs-site.xml")
        try:
            if os.path.exists(xml_path):
                tree = ET.parse(xml_path)
                root = tree.getroot()
                for prop in root.findall("property"):
                    name_el = prop.find("name")
                    if name_el is not None and name_el.text == "dfs.replication":
                        value_el = prop.find("value")
                        if value_el is not None and value_el.text:
                            return int(value_el.text.strip())
        except Exception as e:
            logger.warning(f"Could not read replication factor from {xml_path}: {e}")
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
        """
        Check Hadoop logs for FATAL/ERROR entries written in the last 5 minutes only.

        The old approach scanned the last 200 lines with no time filter — old errors
        from failed startup attempts kept triggering this indefinitely, even after
        the cluster was healthy. Now only recent lines count.
        """
        log_dirs = [
            os.path.join(HADOOP_HOME, "logs"),
            "/opt/hadoop/logs",
            os.environ.get("HADOOP_LOG_DIR", ""),
        ]
        cutoff = datetime.now() - timedelta(minutes=5)

        for log_dir in log_dirs:
            if not log_dir or not os.path.isdir(log_dir):
                continue
            for fname in os.listdir(log_dir):
                if not fname.endswith(".log"):
                    continue
                fpath = os.path.join(log_dir, fname)
                try:
                    with open(fpath, "r", errors="ignore") as f:
                        lines = f.readlines()[-200:]
                    for line in lines:
                        if "FATAL" not in line and "ERROR" not in line:
                            continue
                        # Parse Hadoop log timestamp: 2026-03-03 08:15:24,346
                        try:
                            ts_str = line[:23]
                            ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")
                            if ts >= cutoff:
                                logger.debug(f"Recent error in {fname}: {line.strip()[:120]}")
                                return True
                        except ValueError:
                            # Line has no parseable timestamp — skip it
                            pass
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
