"""
State Detector — Collects the real current state of the Hadoop cluster.
Dynamically resolves HADOOP_HOME — never uses hardcoded paths.
"""

import os
import subprocess
import shutil
import logging
import glob
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

HADOOP_BIN_SYMLINKS = ["hadoop", "hdfs", "yarn", "mapred"]
HADOOP_SBIN_SYMLINKS = [
    "start-dfs.sh", "stop-dfs.sh", "start-yarn.sh", "stop-yarn.sh",
    "start-all.sh", "stop-all.sh", "hadoop-daemon.sh", "hadoop-daemons.sh",
    "workers.sh", "refresh-namenodes.sh", "mr-jobhistory-daemon.sh",
    "yarn-daemon.sh", "yarn-daemons.sh", "httpfs.sh", "kms.sh",
    "start-balancer.sh", "stop-balancer.sh", "distribute-exclude.sh",
    "start-secure-dns.sh", "stop-secure-dns.sh",
]


def _resolve_hadoop_home() -> str:
    env = os.environ.get("HADOOP_HOME", "").strip()
    if env and os.path.isfile(os.path.join(env, "bin", "hadoop")):
        return env
    if os.path.isfile("/usr/local/hadoop/bin/hadoop"):
        real = os.path.realpath("/usr/local/hadoop")
        return real if os.path.isfile(os.path.join(real, "bin", "hadoop")) else "/usr/local/hadoop"
    for c in sorted(glob.glob("/usr/local/hadoop-*"), reverse=True):
        if os.path.isfile(os.path.join(c, "bin", "hadoop")):
            return c
    return "/usr/local/hadoop"


def _resolve_java_home() -> str:
    try:
        java_bin = subprocess.run(["which", "java"], capture_output=True, text=True).stdout.strip()
        if java_bin:
            real = subprocess.run(["readlink", "-f", java_bin], capture_output=True, text=True).stdout.strip()
            return os.path.dirname(os.path.dirname(real))
    except Exception:
        pass
    for c in ["/usr/lib/jvm/java-11-openjdk-amd64", "/usr/lib/jvm/java-11-openjdk-arm64",
              "/usr/lib/jvm/java-21-openjdk-amd64"]:
        if os.path.isfile(os.path.join(c, "bin", "java")):
            return c
    return "/usr/lib/jvm/java-11-openjdk-amd64"


class StateDetector:

    def collect(self) -> dict:
        return {
            "java_installed":        self._check_java(),
            "java_version":          self._get_java_version(),
            "hadoop_installed":      self._check_hadoop(),
            "hadoop_version":        self._get_hadoop_version(),
            "java_home_configured":  self._check_java_home(),
            "namenode_running":      self._check_process("NameNode"),
            "datanode_running":      self._check_process("DataNode"),
            "replication_factor":    self._get_replication_factor(),
            "hdfs_safemode":         self._check_safemode(),
            "critical_log_errors":   self._check_log_errors(),
            "disk_usage_percent":    self._get_disk_usage(),
        }

    # ── java ──────────────────────────────────────────────────────────────────

    def _check_java(self) -> bool:
        # Must actually RUN java — file existing is not enough (partial installs)
        java_candidates = [
            shutil.which("java"),
            "/usr/bin/java",
            "/usr/lib/jvm/java-11-openjdk-amd64/bin/java",
            "/usr/lib/jvm/java-17-openjdk-amd64/bin/java",
            "/usr/lib/jvm/java-21-openjdk-amd64/bin/java",
        ]
        for p in java_candidates:
            if not p:
                continue
            if not (os.path.isfile(p) or os.path.islink(p)):
                continue
            try:
                # Actually execute java — if it runs, it is installed
                rc = subprocess.run(
                    [p, "-version"],
                    capture_output=True, text=True, timeout=10
                ).returncode
                if rc == 0:
                    real = os.path.realpath(p)
                    java_home = os.path.dirname(os.path.dirname(real))
                    os.environ["JAVA_HOME"] = java_home
                    bin_dir = os.path.dirname(p)
                    if bin_dir not in os.environ.get("PATH", ""):
                        os.environ["PATH"] = bin_dir + ":" + os.environ.get("PATH", "")
                    return True
            except Exception:
                continue
        return False

    def _get_java_version(self) -> Optional[str]:
        try:
            java_bin = shutil.which("java") or "/usr/lib/jvm/java-11-openjdk-amd64/bin/java"
            out = subprocess.run([java_bin, "-version"], capture_output=True,
                                 text=True, timeout=5).stderr
            if '"' in out:
                return out.split('"')[1]
        except Exception:
            pass
        return None

    # ── hadoop ────────────────────────────────────────────────────────────────

    def _get_current_user(self) -> str:
        import pwd as _pwd
        try:
            return _pwd.getpwuid(os.getuid()).pw_name
        except Exception:
            return "root"

    def _write_profile_d(self, hadoop_home: str, java_home: str):
        import pwd as _pwd
        profile_file = "/etc/profile.d/hadoop.sh"
        user = self._get_current_user()

        content = (
            "# Hadoop AI Agent — auto-generated\n"
            f"export HADOOP_HOME={hadoop_home}\n"
            f"export JAVA_HOME={java_home}\n"
            f"export PATH=$PATH:{hadoop_home}/bin:{hadoop_home}/sbin:{java_home}/bin\n"
            f"export HDFS_NAMENODE_USER={user}\n"
            f"export HDFS_DATANODE_USER={user}\n"
            f"export HDFS_SECONDARYNAMENODE_USER={user}\n"
            f"export YARN_RESOURCEMANAGER_USER={user}\n"
            f"export YARN_NODEMANAGER_USER={user}\n"
        )

        try:
            if os.getuid() == 0:
                open(profile_file, "w").write(content)
                os.chmod(profile_file, 0o644)
            else:
                import tempfile
                tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False)
                tmp.write(content)
                tmp.flush()
                tmp.close()
                subprocess.run(["sudo", "-n", "cp", tmp.name, profile_file], check=True)
                subprocess.run(["sudo", "-n", "chmod", "644", profile_file], check=True)
                os.unlink(tmp.name)
            logger.info(f"Wrote {profile_file} (HADOOP_HOME={hadoop_home})")
        except Exception as e:
            logger.warning(f"Could not write {profile_file}: {e}")

        for key, val in [
            ("HADOOP_HOME", hadoop_home), ("JAVA_HOME", java_home),
            ("HDFS_NAMENODE_USER", user), ("HDFS_DATANODE_USER", user),
            ("HDFS_SECONDARYNAMENODE_USER", user),
            ("YARN_RESOURCEMANAGER_USER", user), ("YARN_NODEMANAGER_USER", user),
        ]:
            os.environ[key] = val
        if hadoop_home + "/sbin" not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{hadoop_home}/bin:{hadoop_home}/sbin:{os.environ.get('PATH','')}"

        source_line = f"\n# Hadoop AI Agent\n[ -f {profile_file} ] && source {profile_file}\n"
        homes = {os.path.expanduser("~"), "/root", "/home/ubuntu"}
        try:
            for entry in _pwd.getpwall():
                if entry.pw_uid >= 1000 and os.path.isdir(entry.pw_dir):
                    homes.add(entry.pw_dir)
        except Exception:
            pass
        for home in homes:
            bashrc = os.path.join(home, ".bashrc")
            try:
                existing = open(bashrc).read() if os.path.isfile(bashrc) else ""
                if profile_file not in existing:
                    open(bashrc, "a").write(source_line)
                    logger.info(f"Patched {bashrc}")
            except Exception as e:
                try:
                    subprocess.run(
                        ["sudo", "-n", "bash", "-c",
                         f"grep -q '{profile_file}' {bashrc} 2>/dev/null || "
                         f"echo '\\n# Hadoop AI Agent\\n[ -f {profile_file} ] && source {profile_file}' >> {bashrc}"],
                        capture_output=True, text=True, timeout=10)
                    logger.info(f"Patched {bashrc} via sudo")
                except Exception:
                    logger.warning(f"Could not patch {bashrc}: {e}")

    def _sudo_ln(self, src: str, dst: str, name: str):
        """Symlink src -> dst using sudo ln -sfn (works for both root and non-root)."""
        try:
            cmd = ["sudo", "-n", "ln", "-sfn", src, dst] if os.getuid() != 0 else ["ln", "-sfn", src, dst]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if r.returncode == 0:
                logger.info(f"Symlinked {name} -> /usr/local/bin/")
            else:
                logger.warning(f"Could not symlink {name}: {r.stderr.strip()}")
        except Exception as e:
            logger.warning(f"Could not symlink {name}: {e}")

    def _create_bin_symlinks(self, hadoop_home: str):
        # bin/ commands
        for cmd in HADOOP_BIN_SYMLINKS:
            src = os.path.join(hadoop_home, "bin", cmd)
            if not os.path.isfile(src):
                continue
            dst = f"/usr/local/bin/{cmd}"
            # skip if already pointing to same target
            if os.path.islink(dst) and os.path.realpath(dst) == os.path.realpath(src):
                continue
            self._sudo_ln(src, dst, cmd)

        # sbin/ scripts — explicit list + auto-discover anything else
        sbin_dir = os.path.join(hadoop_home, "sbin")
        sbin_names = set(HADOOP_SBIN_SYMLINKS)
        if os.path.isdir(sbin_dir):
            sbin_names.update(os.listdir(sbin_dir))
        for cmd in sorted(sbin_names):
            src = os.path.join(sbin_dir, cmd)
            if not os.path.isfile(src):
                continue
            dst = f"/usr/local/bin/{cmd}"
            if os.path.islink(dst) and os.path.realpath(dst) == os.path.realpath(src):
                continue
            self._sudo_ln(src, dst, cmd)

    def _ensure_hadoop_on_path(self) -> bool:
        hadoop_home = _resolve_hadoop_home()
        if not os.path.isfile(os.path.join(hadoop_home, "bin", "hadoop")):
            return False
        java_home = _resolve_java_home()
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["JAVA_HOME"]   = java_home
        if hadoop_home + "/bin" not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{hadoop_home}/bin:{hadoop_home}/sbin:{os.environ.get('PATH','')}"
            logger.info(f"Auto-set HADOOP_HOME={hadoop_home} and updated PATH")

        # Only write profile_d + symlinks once per process — not every iteration
        marker = getattr(self, "_profile_written", None)
        if marker != hadoop_home:
            self._write_profile_d(hadoop_home, java_home)
            self._create_bin_symlinks(hadoop_home)
            self._profile_written = hadoop_home

        return shutil.which("hadoop") is not None

    def _check_hadoop(self) -> bool:
        return self._ensure_hadoop_on_path()

    def _get_hadoop_version(self) -> Optional[str]:
        self._ensure_hadoop_on_path()
        hadoop_home = _resolve_hadoop_home()
        try:
            hadoop_bin = os.path.join(hadoop_home, "bin", "hadoop")
            out = subprocess.run(
                [hadoop_bin, "version"],
                capture_output=True, text=True, timeout=10,
                env={**os.environ, "JAVA_HOME": _resolve_java_home(), "HADOOP_HOME": hadoop_home}
            ).stdout
            for line in out.splitlines():
                if line.strip().startswith("Hadoop "):
                    return line.strip().split()[-1]
        except Exception:
            pass
        try:
            common_dir = os.path.join(hadoop_home, "share", "hadoop", "common")
            if os.path.isdir(common_dir):
                for fname in os.listdir(common_dir):
                    if fname.startswith("hadoop-common-") and fname.endswith(".jar"):
                        ver = fname.replace("hadoop-common-", "").replace(".jar", "")
                        if ver:
                            return ver
        except Exception:
            pass
        try:
            import re as _re
            m = _re.search(r"hadoop-(\d+\.\d+\.\d+)", hadoop_home)
            if m:
                return m.group(1)
        except Exception:
            pass
        return None

    def _check_java_home(self) -> bool:
        java_home = os.environ.get("JAVA_HOME", "").strip()
        if java_home and os.path.isfile(os.path.join(java_home, "bin", "java")):
            return True
        env_file = os.path.join(_resolve_hadoop_home(), "etc", "hadoop", "hadoop-env.sh")
        try:
            for line in open(env_file):
                line = line.strip()
                if line.startswith("export JAVA_HOME="):
                    jh = line.split("=", 1)[1].strip().strip('"').strip("'")
                    if jh and os.path.isfile(os.path.join(jh, "bin", "java")):
                        os.environ["JAVA_HOME"] = jh
                        return True
        except Exception:
            pass
        return False

    # ── processes ─────────────────────────────────────────────────────────────

    def _check_process(self, process_name: str) -> bool:
        for attempt in range(2):
            try:
                out = subprocess.run(["jps"], capture_output=True, text=True, timeout=10).stdout
                if any(l.strip().endswith(process_name) for l in out.splitlines()):
                    return True
                if attempt == 0:
                    import time as _time
                    _time.sleep(2)
            except Exception:
                pass
        return False

    # ── config ────────────────────────────────────────────────────────────────

    def _get_replication_factor(self) -> Optional[int]:
        xml_path = os.path.join(_resolve_hadoop_home(), "etc", "hadoop", "hdfs-site.xml")
        try:
            if os.path.exists(xml_path):
                for prop in ET.parse(xml_path).getroot().findall("property"):
                    n = prop.find("name")
                    if n is not None and n.text == "dfs.replication":
                        v = prop.find("value")
                        if v is not None and v.text:
                            return int(v.text.strip())
        except Exception:
            pass
        return None

    def _check_safemode(self) -> bool:
        try:
            out = subprocess.run(["hdfs", "dfsadmin", "-safemode", "get"],
                                 capture_output=True, text=True, timeout=10).stdout
            return "ON" in out
        except Exception:
            return False

    # ── logs ──────────────────────────────────────────────────────────────────

    def _check_log_errors(self) -> bool:
        # If NameNode is already up, old errors in logs are stale — ignore them
        try:
            jps_out = subprocess.run(["jps"], capture_output=True, text=True, timeout=5).stdout
            if "NameNode" in jps_out and "DataNode" in jps_out:
                return False
        except Exception:
            pass

        hadoop_home = _resolve_hadoop_home()
        log_dirs = [os.path.join(hadoop_home, "logs"),
                    os.environ.get("HADOOP_LOG_DIR", "")]
        namenode_start_time = None
        NAMENODE_START_MARKERS = [
            "NameNode RPC up at:",
            "IPC Server Responder",
            "createNameNode",
        ]
        for log_dir in log_dirs:
            if not log_dir or not os.path.isdir(log_dir):
                continue
            for fname in os.listdir(log_dir):
                if "namenode" not in fname.lower() or not fname.endswith(".log"):
                    continue
                try:
                    for line in open(os.path.join(log_dir, fname), errors="ignore"):
                        if any(m in line for m in NAMENODE_START_MARKERS):
                            try:
                                namenode_start_time = datetime.strptime(line[:23], "%Y-%m-%d %H:%M:%S,%f")
                            except ValueError:
                                pass
                except Exception:
                    pass
        cutoff = namenode_start_time if namenode_start_time else (datetime.now() - timedelta(minutes=3))
        IGNORE_PATTERNS = [
            "RECEIVED SIGNAL 15: SIGTERM",
            "RECEIVED SIGNAL 2: SIGINT",
            "file:/// has no authority",
            "No services to connect",
            "missing NameNode address",
        ]
        for log_dir in log_dirs:
            if not log_dir or not os.path.isdir(log_dir):
                continue
            for fname in os.listdir(log_dir):
                if not fname.endswith(".log"):
                    continue
                try:
                    for line in open(os.path.join(log_dir, fname), errors="ignore").readlines()[-300:]:
                        if "FATAL" not in line and "ERROR" not in line:
                            continue
                        if any(p in line for p in IGNORE_PATTERNS):
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

    # ── disk ──────────────────────────────────────────────────────────────────

    def _get_disk_usage(self) -> Optional[int]:
        try:
            lines = subprocess.run(["df", "-h", "/"], capture_output=True,
                                   text=True, timeout=5).stdout.splitlines()
            if len(lines) > 1:
                parts = lines[1].split()
                if len(parts) >= 5:
                    return int(parts[4].replace("%", ""))
        except Exception:
            pass
        return None
