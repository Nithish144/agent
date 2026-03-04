"""
State Detector — Collects the real current state of the Hadoop cluster.
"""

import os
import subprocess
import shutil
import logging
import glob
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# Only bin commands are safe to symlink — sbin scripts use relative paths
# (../libexec/hdfs-config.sh) that break when called from /usr/local/bin/.
HADOOP_BIN_SYMLINKS = ["hadoop", "hdfs", "yarn", "mapred"]


def _resolve_hadoop_home() -> str:
    """
    Dynamically resolve HADOOP_HOME at runtime.
    Priority:
      1. HADOOP_HOME env var (if valid)
      2. /usr/local/hadoop symlink
      3. Latest versioned /usr/local/hadoop-* directory
    """
    env_home = os.environ.get("HADOOP_HOME", "")
    if env_home and os.path.isfile(os.path.join(env_home, "bin", "hadoop")):
        return env_home
    if os.path.isfile("/usr/local/hadoop/bin/hadoop"):
        return "/usr/local/hadoop"
    candidates = sorted(glob.glob("/usr/local/hadoop-*"), reverse=True)
    for c in candidates:
        if os.path.isfile(os.path.join(c, "bin", "hadoop")):
            return c
    return "/usr/local/hadoop"


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

    def _get_current_user(self) -> str:
        import pwd
        try:
            return pwd.getpwuid(os.getuid()).pw_name
        except Exception:
            return "root"

    def _write_profile_d(self, hadoop_home: str):
        """
        Write /etc/profile.d/hadoop.sh with HADOOP_HOME, JAVA_HOME, PATH,
        and ALL daemon user vars (HDFS + YARN).

        Also appends a source line to every user's .bashrc so non-login
        interactive terminals (Ubuntu default) pick it up automatically.
        No manual sourcing ever needed after this runs.
        """
        import pwd as _pwd

        profile_file = "/etc/profile.d/hadoop.sh"
        java_home = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64")
        current_user = self._get_current_user()

        profile_content = (
            "# Hadoop environment — written by Hadoop AI Agent\n"
            f"export HADOOP_HOME={hadoop_home}\n"
            f"export JAVA_HOME={java_home}\n"
            f"export PATH=$PATH:{hadoop_home}/bin:{hadoop_home}/sbin:$JAVA_HOME/bin\n"
            f"export HDFS_NAMENODE_USER={current_user}\n"
            f"export HDFS_DATANODE_USER={current_user}\n"
            f"export HDFS_SECONDARYNAMENODE_USER={current_user}\n"
            f"export YARN_RESOURCEMANAGER_USER={current_user}\n"
            f"export YARN_NODEMANAGER_USER={current_user}\n"
        )

        try:
            existing = ""
            if os.path.exists(profile_file):
                with open(profile_file, "r") as f:
                    existing = f.read()
            if existing.strip() != profile_content.strip():
                with open(profile_file, "w") as f:
                    f.write(profile_content)
                os.chmod(profile_file, 0o644)
                logger.info(f"Wrote Hadoop PATH + daemon users to {profile_file}")

            # Export into current process immediately
            os.environ["HDFS_NAMENODE_USER"] = current_user
            os.environ["HDFS_DATANODE_USER"] = current_user
            os.environ["HDFS_SECONDARYNAMENODE_USER"] = current_user
            os.environ["YARN_RESOURCEMANAGER_USER"] = current_user
            os.environ["YARN_NODEMANAGER_USER"] = current_user
            sbin = os.path.join(hadoop_home, "sbin")
            if sbin not in os.environ.get("PATH", ""):
                os.environ["PATH"] = f"{sbin}:{os.environ.get('PATH', '')}"

        except Exception as e:
            logger.warning(f"Could not write {profile_file}: {e}")

        # Add source line to .bashrc for every user so non-login shells work
        source_line = f"\n# Added by Hadoop AI Agent\n[ -f {profile_file} ] && source {profile_file}\n"
        target_homes = set()
        target_homes.add(os.path.expanduser("~"))
        sudo_user = os.environ.get("SUDO_USER")
        if sudo_user:
            try:
                target_homes.add(_pwd.getpwnam(sudo_user).pw_dir)
            except KeyError:
                pass
        try:
            for entry in _pwd.getpwall():
                if entry.pw_uid >= 1000 and os.path.isdir(entry.pw_dir):
                    target_homes.add(entry.pw_dir)
        except Exception:
            pass

        for home in target_homes:
            bashrc = os.path.join(home, ".bashrc")
            try:
                try:
                    with open(bashrc, "r") as f:
                        content = f.read()
                except FileNotFoundError:
                    content = ""
                if profile_file not in content:
                    with open(bashrc, "a") as f:
                        f.write(source_line)
                    logger.info(f"Added profile.d source to {bashrc}")
            except Exception as e:
                logger.warning(f"Could not update {bashrc}: {e}")

    def _create_bin_symlinks(self, hadoop_home: str):
        """Symlink bin commands into /usr/local/bin (safe — no relative paths)."""
        for cmd in HADOOP_BIN_SYMLINKS:
            src = os.path.join(hadoop_home, "bin", cmd)
            dst = os.path.join("/usr/local/bin", cmd)
            if not os.path.isfile(src):
                continue
            try:
                if os.path.islink(dst) or os.path.exists(dst):
                    if os.path.realpath(dst) == os.path.realpath(src):
                        continue
                    os.remove(dst)
                os.symlink(src, dst)
                logger.info(f"Symlinked {cmd} → /usr/local/bin/")
            except Exception as e:
                logger.warning(f"Could not symlink {cmd}: {e}")

    def _ensure_hadoop_on_path(self) -> bool:
        """
        Resolve HADOOP_HOME dynamically, set env, write profile.d and bin symlinks.
        Called unconditionally on every collect() — fully self-healing.
        """
        hadoop_home = _resolve_hadoop_home()
        if not os.path.isfile(os.path.join(hadoop_home, "bin", "hadoop")):
            return False

        os.environ["HADOOP_HOME"] = hadoop_home
        if hadoop_home + "/bin" not in os.environ.get("PATH", ""):
            os.environ["PATH"] = (
                f"{hadoop_home}/bin:{hadoop_home}/sbin:{os.environ.get('PATH', '')}"
            )
            logger.info(f"Auto-set HADOOP_HOME={hadoop_home} and updated PATH")

        # Always run — idempotent, self-healing
        self._write_profile_d(hadoop_home)
        self._create_bin_symlinks(hadoop_home)

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
        java_home = os.environ.get("JAVA_HOME", "").strip()
        if java_home and os.path.isfile(os.path.join(java_home, "bin", "java")):
            return True
        hadoop_home = _resolve_hadoop_home()
        env_file = os.path.join(hadoop_home, "etc", "hadoop", "hadoop-env.sh")
        try:
            if os.path.exists(env_file):
                with open(env_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("export JAVA_HOME="):
                            java_home = line.split("=", 1)[1].strip().strip('"').strip("'")
                            if java_home and os.path.isfile(os.path.join(java_home, "bin", "java")):
                                os.environ["JAVA_HOME"] = java_home
                                return True
        except Exception as e:
            logger.warning(f"Could not read hadoop-env.sh: {e}")
        return False

    def _check_process(self, process_name: str) -> bool:
        """Per-line exact suffix match — avoids 'NameNode' matching 'SecondaryNameNode'."""
        try:
            result = subprocess.run(["jps"], capture_output=True, text=True, timeout=5)
            return any(
                line.strip().endswith(process_name)
                for line in result.stdout.splitlines()
            )
        except Exception:
            return False

    def _get_replication_factor(self) -> Optional[int]:
        hadoop_home = _resolve_hadoop_home()
        xml_path = os.path.join(hadoop_home, "etc", "hadoop", "hdfs-site.xml")
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
            logger.warning(f"Could not read replication factor: {e}")
        return None

    def _check_safemode(self) -> bool:
        try:
            result = subprocess.run(
                ["hdfs", "dfsadmin", "-safemode", "get"],
                capture_output=True, text=True, timeout=10,
            )
            return "ON" in result.stdout
        except Exception:
            return False

    def _check_log_errors(self) -> bool:
        hadoop_home = _resolve_hadoop_home()
        log_dirs = [
            os.path.join(hadoop_home, "logs"),
            "/opt/hadoop/logs",
            os.environ.get("HADOOP_LOG_DIR", ""),
        ]

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
                    for line in reversed(lines):
                        if any(m in line for m in NAMENODE_MARKERS):
                            try:
                                namenode_started_at = datetime.strptime(
                                    line[:23], "%Y-%m-%d %H:%M:%S,%f"
                                )
                                break
                            except ValueError:
                                pass
                    if namenode_started_at:
                        break
                except Exception:
                    pass
            if namenode_started_at:
                break

        cutoff = namenode_started_at if namenode_started_at else (
            datetime.now() - timedelta(minutes=10)
        )

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
                            ts = datetime.strptime(line[:23], "%Y-%m-%d %H:%M:%S,%f")
                            if ts >= cutoff:
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
