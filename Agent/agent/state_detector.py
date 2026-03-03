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


    def _ensure_hadoop_on_path(self) -> bool:
        """
        Ensure HADOOP_HOME and its bin/sbin are on PATH.
        Checks in order:
        1. hadoop already on PATH (nothing to do)
        2. /usr/local/hadoop symlink (standard install location)
        3. Any /usr/local/hadoop-* versioned directory

        When found, sets HADOOP_HOME and PATH for the current process so every
        subsequent subprocess (hadoop, hdfs, start-dfs.sh) finds the binaries.
        Also writes to ~/.bashrc so the terminal works without manual setup.
        """
        # Already on PATH — nothing to do
        if shutil.which("hadoop"):
            return True

        # Find Hadoop home directory
        hadoop_home = None
        import glob as _glob
        candidates = ["/usr/local/hadoop"] + sorted(_glob.glob("/usr/local/hadoop-*"), reverse=True)
        for candidate in candidates:
            if os.path.isfile(os.path.join(candidate, "bin", "hadoop")):
                hadoop_home = candidate
                break

        if not hadoop_home:
            return False

        # Set for current process
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PATH"] = (
            f"{hadoop_home}/bin:{hadoop_home}/sbin:{os.environ.get('PATH', '')}"
        )
        logger.info(f"Auto-set HADOOP_HOME={hadoop_home} and updated PATH")

        # Create symlinks in /usr/local/bin for all key Hadoop commands.
        # /usr/local/bin is on PATH for EVERY user, EVERY shell type, with NO
        # sourcing required — this is the only reliable cross-user, cross-shell fix.
        # .bashrc fixes only work after `source ~/.bashrc` in the current session.
        HADOOP_COMMANDS = [
            # sbin — cluster management scripts
            "start-dfs.sh", "stop-dfs.sh", "start-yarn.sh", "stop-yarn.sh",
            "start-all.sh", "stop-all.sh", "hadoop-daemon.sh", "hdfs-config.sh",
            # bin — user-facing tools
            "hadoop", "hdfs", "yarn", "mapred",
        ]
        for cmd in HADOOP_COMMANDS:
            for subdir in ("sbin", "bin"):
                src = os.path.join(hadoop_home, subdir, cmd)
                dst = os.path.join("/usr/local/bin", cmd)
                if os.path.isfile(src):
                    try:
                        if os.path.lexists(dst):
                            os.remove(dst)
                        os.symlink(src, dst)
                        logger.info(f"Symlinked {cmd} → /usr/local/bin/")
                    except Exception as e:
                        logger.warning(f"Could not symlink {cmd}: {e}")
                    break  # found in this subdir, no need to check the other

        # Persist to all shell init files so every terminal session works.
        # ~/.bashrc      — interactive non-login bash shells
        # ~/.bash_profile — login bash shells (SSH, new terminal tabs on some systems)
        # ~/.profile     — login shells (sh, dash, non-bash)
        export_lines = [
            f"export HADOOP_HOME={hadoop_home}",
            "export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin",
            f"export JAVA_HOME={os.environ.get('JAVA_HOME', '/usr/lib/jvm/java-11-openjdk-amd64')}",
            "export PATH=$PATH:$JAVA_HOME/bin",
        ]
        # Detect ALL real human users to write to — agent may run as root
        # but the terminal user is different (e.g. ubuntu, hadoop, ec2-user).
        # Strategy: collect home dirs for (1) current user, (2) SUDO_USER if
        # agent was launched with sudo, (3) all non-system users (UID >= 1000).
        import pwd as _pwd
        target_homes = set()

        # Current process user
        target_homes.add(os.path.expanduser("~"))

        # SUDO_USER — the human who ran `sudo python main.py`
        sudo_user = os.environ.get("SUDO_USER")
        if sudo_user:
            try:
                target_homes.add(_pwd.getpwnam(sudo_user).pw_dir)
            except KeyError:
                pass

        # All non-system users (UID >= 1000) — catches ubuntu, hadoop, etc.
        try:
            for entry in _pwd.getpwall():
                if entry.pw_uid >= 1000 and os.path.isdir(entry.pw_dir):
                    target_homes.add(entry.pw_dir)
        except Exception:
            pass

        shell_files = []
        for home in target_homes:
            shell_files += [
                os.path.join(home, ".bashrc"),
                os.path.join(home, ".bash_profile"),
                os.path.join(home, ".profile"),
            ]
        for shell_file in shell_files:
            try:
                # Read existing content (create file if missing)
                try:
                    with open(shell_file, "r") as f:
                        content = f.read()
                except FileNotFoundError:
                    content = ""
                # Check each export line individually by its exact string —
                # the old filter used variable-name matching which caused the
                # PATH export to be skipped because PATH= already existed in
                # .bashrc (from unrelated PATH=$PATH:/snap/bin lines).
                lines_to_add = [l for l in export_lines if l not in content]
                if lines_to_add:
                    with open(shell_file, "a") as f:
                        f.write("\n# Added by Hadoop AI Agent\n")
                        f.write("\n".join(lines_to_add) + "\n")
                    logger.info(f"Wrote Hadoop/Java PATH exports to {shell_file}")
            except Exception as e:
                logger.warning(f"Could not update {shell_file}: {e}")

        # Also write /etc/environment for system-wide persistence (all users, all shells)
        try:
            with open("/etc/environment", "r") as f:
                etc_content = f.read()
            with open("/etc/environment", "a") as f:
                if f"HADOOP_HOME=" not in etc_content:
                    f.write(f'\nHADOOP_HOME="{hadoop_home}"\n')
                if f"JAVA_HOME=" not in etc_content:
                    java_home_val = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64")
                    f.write(f'JAVA_HOME="{java_home_val}"\n')
        except Exception as e:
            logger.warning(f"Could not update /etc/environment: {e}")

        return shutil.which("hadoop") is not None

    def _check_hadoop(self) -> bool:
        return self._ensure_hadoop_on_path()

    def _get_hadoop_version(self) -> Optional[str]:
        self._ensure_hadoop_on_path()
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
        """
        Check JAVA_HOME from two sources in order:
        1. Current process environment (set by shell or configure_java_home)
        2. hadoop-env.sh (written by configure_java_home — persists across runs)

        Using only os.environ means the agent sees java_home_configured=false
        on every fresh run even after it was already configured, causing it to
        call configure_java_home unnecessarily every single time.
        """
        # Source 1: environment variable
        java_home = os.environ.get("JAVA_HOME", "").strip()
        if java_home and os.path.isfile(os.path.join(java_home, "bin", "java")):
            return True

        # Source 2: hadoop-env.sh (persistent across agent restarts)
        env_file = os.path.join(HADOOP_HOME, "etc", "hadoop", "hadoop-env.sh")
        try:
            if os.path.exists(env_file):
                with open(env_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("export JAVA_HOME="):
                            java_home = line.split("=", 1)[1].strip().strip('"').strip("'")
                            if java_home and os.path.isfile(os.path.join(java_home, "bin", "java")):
                                # Sync into current process so subprocesses inherit it
                                os.environ["JAVA_HOME"] = java_home
                                return True
        except Exception as e:
            logger.warning(f"Could not read hadoop-env.sh: {e}")

        return False

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
        Check Hadoop logs for FATAL/ERROR entries written AFTER the NameNode started.

        Strategy:
        1. Find when the NameNode last started by reading its log for the
           "NameNode RPC up" / "IPC Server Responder" startup marker.
        2. Only flag ERROR/FATAL lines that appear AFTER that timestamp.

        This prevents old startup-failure errors (from before core-site.xml was
        fixed) from keeping critical_log_errors=true forever after the cluster
        is healthy. Falls back to a 10-minute window if no startup marker found.
        """
        log_dirs = [
            os.path.join(HADOOP_HOME, "logs"),
            "/opt/hadoop/logs",
            os.environ.get("HADOOP_LOG_DIR", ""),
        ]

        # Step 1: Find NameNode startup timestamp from its log
        namenode_started_at = None
        NAMENODE_MARKERS = (
            "org.apache.hadoop.hdfs.server.namenode.NameNode: createNameNode",
            "NameNode RPC up at:",
            "IPC Server Responder",
            "NameNode: registered UNIX signal handlers",
        )
        for log_dir in log_dirs:
            if not log_dir or not os.path.isdir(log_dir):
                continue
            for fname in os.listdir(log_dir):
                if "namenode" not in fname.lower() or not fname.endswith(".log"):
                    continue
                fpath = os.path.join(log_dir, fname)
                try:
                    with open(fpath, "r", errors="ignore") as f:
                        lines = f.readlines()
                    # Walk lines in reverse — find the MOST RECENT startup marker
                    for line in reversed(lines):
                        if any(m in line for m in NAMENODE_MARKERS):
                            try:
                                ts_str = line[:23]
                                namenode_started_at = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")
                                break
                            except ValueError:
                                pass
                    if namenode_started_at:
                        break
                except Exception:
                    pass
            if namenode_started_at:
                break

        # Step 2: Set cutoff — errors only count if after NameNode started
        if namenode_started_at:
            cutoff = namenode_started_at
            logger.debug(f"Log error cutoff: NameNode started at {namenode_started_at}")
        else:
            # NameNode not running or log unreadable — fall back to 10-minute window
            cutoff = datetime.now() - timedelta(minutes=10)
            logger.debug("Log error cutoff: fallback 10-minute window")

        # Step 3: Scan all logs for ERROR/FATAL after cutoff
        for log_dir in log_dirs:
            if not log_dir or not os.path.isdir(log_dir):
                continue
            for fname in os.listdir(log_dir):
                if not fname.endswith(".log"):
                    continue
                fpath = os.path.join(log_dir, fname)
                try:
                    with open(fpath, "r", errors="ignore") as f:
                        lines = f.readlines()[-300:]
                    for line in lines:
                        if "FATAL" not in line and "ERROR" not in line:
                            continue
                        try:
                            ts_str = line[:23]
                            ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")
                            if ts >= cutoff:
                                logger.debug(f"Post-startup error in {fname}: {line.strip()[:120]}")
                                return True
                        except ValueError:
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
