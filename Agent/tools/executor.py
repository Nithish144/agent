"""
Tool Executor — Safe predefined tool implementations.
Windows-aware. Never runs arbitrary LLM shell commands.
"""

import subprocess
import logging
import os
import sys
import pwd
import shutil
import time
import glob
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)
IS_WINDOWS = sys.platform == "win32"


def _resolve_hadoop_home() -> str:
    """
    Dynamically resolve HADOOP_HOME at runtime.
    Priority:
      1. HADOOP_HOME env var (if valid)
      2. /usr/local/hadoop symlink
      3. Latest versioned /usr/local/hadoop-* directory
    Never falls back to a hardcoded wrong path.
    """
    # 1. Env var
    env_home = os.environ.get("HADOOP_HOME", "")
    if env_home and os.path.isfile(os.path.join(env_home, "bin", "hadoop")):
        return env_home

    # 2. Symlink
    if os.path.isfile("/usr/local/hadoop/bin/hadoop"):
        return "/usr/local/hadoop"

    # 3. Versioned dirs — pick latest
    candidates = sorted(glob.glob("/usr/local/hadoop-*"), reverse=True)
    for c in candidates:
        if os.path.isfile(os.path.join(c, "bin", "hadoop")):
            return c

    # 4. Last resort — return symlink path even if missing (agent will install)
    return "/usr/local/hadoop"


# Resolved once at import time; also re-resolved inside methods that need it
HADOOP_HOME = _resolve_hadoop_home()


class ToolExecutor:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self._dispatch = {
            "install_java": self._install_java,
            "install_hadoop": self._install_hadoop,
            "configure_java_home": self._configure_java_home,
            "configure_hdfs_site": self._configure_hdfs_site,
            "configure_core_site": self._configure_core_site,
            "start_hdfs": self._start_hdfs,
            "stop_hdfs": self._stop_hdfs,
            "restart_namenode": self._restart_namenode,
            "restart_datanode": self._restart_datanode,
            "leave_safemode": self._leave_safemode,
            "check_hdfs_health": self._check_hdfs_health,
            "analyze_logs": self._analyze_logs,
            "check_disk_space": self._check_disk_space,
            "request_human_approval": self._request_human_approval,
            "format_namenode": self._format_namenode,
        }

    def _hadoop_home(self) -> str:
        """Always resolve fresh — handles the case where Hadoop was just installed."""
        return _resolve_hadoop_home()

    def execute(self, tool_name: str, arguments: dict) -> dict:
        if tool_name not in self._dispatch:
            return {"success": False, "error": f"No executor for tool: {tool_name}"}
        if self.dry_run:
            logger.info(f"[DRY RUN] Would execute: {tool_name}({arguments})")
            return {"success": True, "dry_run": True, "tool": tool_name, "arguments": arguments}
        try:
            result = self._dispatch[tool_name](arguments)
            return {"success": True, **result}
        except Exception as e:
            logger.error(f"Tool execution error [{tool_name}]: {e}")
            return {"success": False, "error": str(e)}

    def _run(self, cmd: list, timeout: int = 60) -> dict:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return {
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }

    def _update_xml_property(self, filepath: str, name: str, value: str):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        if not os.path.exists(filepath):
            root = ET.Element("configuration")
            tree = ET.ElementTree(root)
        else:
            tree = ET.parse(filepath)
            root = tree.getroot()
        for prop in root.findall("property"):
            n = prop.find("name")
            if n is not None and n.text == name:
                prop.find("value").text = value
                tree.write(filepath)
                return
        prop = ET.SubElement(root, "property")
        ET.SubElement(prop, "name").text = name
        ET.SubElement(prop, "value").text = value
        tree.write(filepath, xml_declaration=True, encoding="UTF-8")

    def _get_current_user(self) -> str:
        try:
            return pwd.getpwuid(os.getuid()).pw_name
        except Exception:
            return "root"

    def _install_java(self, args: dict) -> dict:
        version = args["version"]
        if IS_WINDOWS:
            return {"status": "manual_required", "message": f"Download OpenJDK {version} from https://adoptium.net"}

        if shutil.which("java"):
            logger.info("Java already installed, updating JAVA_HOME only")
            try:
                java_path = subprocess.run(["which", "java"], capture_output=True, text=True).stdout.strip()
                real_path = subprocess.run(["readlink", "-f", java_path], capture_output=True, text=True).stdout.strip()
                java_home = os.path.dirname(os.path.dirname(real_path))
                os.environ["JAVA_HOME"] = java_home
                os.environ["PATH"] = java_home + "/bin:" + os.environ.get("PATH", "")
            except Exception:
                pass
            return {"installed": "java (already present)", "returncode": 0, "stdout": "", "stderr": ""}

        logger.info(f"Installing OpenJDK {version}...")
        self._run(["apt-get", "update", "-qq"], timeout=120)
        result = self._run(["apt-get", "install", "-y", f"openjdk-{version}-jdk"], timeout=300)
        if result["returncode"] == 0:
            try:
                java_path = subprocess.run(["which", "java"], capture_output=True, text=True).stdout.strip()
                real_path = subprocess.run(["readlink", "-f", java_path], capture_output=True, text=True).stdout.strip()
                java_home = os.path.dirname(os.path.dirname(real_path))
                os.environ["JAVA_HOME"] = java_home
                os.environ["PATH"] = java_home + "/bin:" + os.environ.get("PATH", "")
                logger.info(f"JAVA_HOME auto-set to {java_home}")
            except Exception as e:
                logger.warning(f"Could not auto-set JAVA_HOME: {e}")
        return {"installed": f"openjdk-{version}-jdk", **result}

    def _install_hadoop(self, args: dict) -> dict:
        version = args["version"]
        if IS_WINDOWS:
            return {"status": "manual_required", "message": f"Download hadoop-{version} from https://hadoop.apache.org/releases.html"}

        hadoop_dir = f"/usr/local/hadoop-{version}"
        if os.path.isdir(hadoop_dir):
            logger.info(f"Hadoop {version} already extracted at {hadoop_dir}, creating symlink only")
            link = self._run(["ln", "-sfn", hadoop_dir, "/usr/local/hadoop"])
            os.environ["PATH"] = f"/usr/local/hadoop/bin:{os.environ.get('PATH', '')}"
            os.environ["HADOOP_HOME"] = hadoop_dir
            return {"version": version, "status": "already_exists", "symlink": link}

        url = f"https://downloads.apache.org/hadoop/common/hadoop-{version}/hadoop-{version}.tar.gz"
        tarfile = f"/tmp/hadoop-{version}.tar.gz"
        logger.info(f"Downloading Hadoop {version}...")
        dl = self._run(["wget", "-q", "--show-progress", "-O", tarfile, url], timeout=600)
        if dl["returncode"] != 0:
            return {"error": "Download failed", **dl}
        logger.info("Extracting Hadoop...")
        extract = self._run(["tar", "-xzf", tarfile, "-C", "/usr/local/"], timeout=120)
        link = self._run(["ln", "-sfn", hadoop_dir, "/usr/local/hadoop"])
        os.environ["PATH"] = f"/usr/local/hadoop/bin:{os.environ.get('PATH', '')}"
        os.environ["HADOOP_HOME"] = hadoop_dir
        try:
            os.remove(tarfile)
        except Exception:
            pass
        return {"version": version, "extract": extract, "symlink": link}

    def _configure_java_home(self, args: dict) -> dict:
        if IS_WINDOWS:
            return {"status": "manual_required", "message": "Set JAVA_HOME in System Environment Variables"}
        java_home = self._auto_detect_java_home()
        os.environ["JAVA_HOME"] = java_home

        hadoop_home = self._hadoop_home()
        env_file = os.path.join(hadoop_home, "etc", "hadoop", "hadoop-env.sh")
        if os.path.exists(env_file):
            with open(env_file, "r") as f:
                content = f.read()
            import re
            if "export JAVA_HOME" in content:
                content = re.sub(r"#?\s*export JAVA_HOME=.*", f"export JAVA_HOME={java_home}", content)
            else:
                content += f"\nexport JAVA_HOME={java_home}\n"
            with open(env_file, "w") as f:
                f.write(content)

        try:
            with open("/etc/environment", "a") as f:
                f.write(f'\nJAVA_HOME="{java_home}"\n')
        except Exception:
            pass

        logger.info(f"JAVA_HOME set to {java_home}")
        return {"java_home": java_home, "configured_in": env_file}

    def _configure_hdfs_site(self, args: dict) -> dict:
        replication = str(args["replication_factor"])
        hadoop_home = self._hadoop_home()
        filepath = os.path.join(hadoop_home, "etc", "hadoop", "hdfs-site.xml")
        self._update_xml_property(filepath, "dfs.replication", replication)
        self._update_xml_property(filepath, "dfs.namenode.name.dir", "file:///data/namenode")
        self._update_xml_property(filepath, "dfs.datanode.data.dir", "file:///data/datanode")
        os.makedirs("/data/namenode", exist_ok=True)
        os.makedirs("/data/datanode", exist_ok=True)
        return {"updated": filepath, "replication_factor": replication}

    def _configure_core_site(self, args: dict) -> dict:
        hadoop_home = self._hadoop_home()
        filepath = os.path.join(hadoop_home, "etc", "hadoop", "core-site.xml")
        self._update_xml_property(filepath, "fs.defaultFS", "hdfs://localhost:9000")
        return {"updated": filepath}

    def _auto_detect_java_home(self) -> str:
        try:
            java_bin = subprocess.run(["which", "java"], capture_output=True, text=True).stdout.strip()
            if java_bin:
                real = subprocess.run(["readlink", "-f", java_bin], capture_output=True, text=True).stdout.strip()
                return os.path.dirname(os.path.dirname(real))
        except Exception:
            pass
        for candidate in [
            "/usr/lib/jvm/java-11-openjdk-amd64",
            "/usr/lib/jvm/java-11-openjdk-arm64",
            "/usr/lib/jvm/temurin-11-amd64",
            "/usr/lib/jvm/java-21-openjdk-amd64",
        ]:
            if os.path.isfile(os.path.join(candidate, "bin", "java")):
                return candidate
        return "/usr/lib/jvm/java-11-openjdk-amd64"

    def _write_java_home_to_hadoop_env(self, java_home: str):
        import re
        hadoop_home = self._hadoop_home()
        env_file = os.path.join(hadoop_home, "etc", "hadoop", "hadoop-env.sh")
        if not os.path.exists(env_file):
            logger.warning(f"hadoop-env.sh not found at {env_file}")
            return
        with open(env_file, "r") as f:
            content = f.read()
        content = re.sub(r"\n?#?\s*export JAVA_HOME=.*", "", content)
        content = content.rstrip() + f"\nexport JAVA_HOME={java_home}\n"
        with open(env_file, "w") as f:
            f.write(content)
        logger.info(f"Wrote JAVA_HOME={java_home} into hadoop-env.sh")

    def _write_daemon_users_to_hadoop_env(self):
        """
        Persist ALL daemon user vars into hadoop-env.sh so start/stop scripts
        work from any terminal without manual exports.
        Covers HDFS + YARN daemon users.
        Uses getpwuid(getuid()) for accurate user detection.
        Uses regex substitution to overwrite any wrong previous values.
        """
        import re

        hadoop_home = self._hadoop_home()
        env_file = os.path.join(hadoop_home, "etc", "hadoop", "hadoop-env.sh")
        if not os.path.exists(env_file):
            logger.warning(f"hadoop-env.sh not found at {env_file}, skipping daemon user config")
            return

        current_user = self._get_current_user()
        logger.info(f"Writing HDFS daemon user vars for user: {current_user}")

        with open(env_file, "r") as f:
            content = f.read()

        daemon_vars = {
            "HDFS_NAMENODE_USER": current_user,
            "HDFS_DATANODE_USER": current_user,
            "HDFS_SECONDARYNAMENODE_USER": current_user,
            "YARN_RESOURCEMANAGER_USER": current_user,
            "YARN_NODEMANAGER_USER": current_user,
        }

        for key, val in daemon_vars.items():
            if re.search(rf"export {key}=", content):
                content = re.sub(rf"export {key}=\S+", f"export {key}={val}", content)
            else:
                content = content.rstrip() + f"\nexport {key}={val}\n"

        with open(env_file, "w") as f:
            f.write(content)

        logger.info(f"Wrote HDFS daemon user vars to hadoop-env.sh (user={current_user})")

    def _start_hdfs(self, args: dict) -> dict:
        hadoop_home = self._hadoop_home()

        # Step 1: Auto-detect JAVA_HOME, write to hadoop-env.sh + daemon users
        java_home = self._auto_detect_java_home()
        logger.info(f"Auto-detected JAVA_HOME: {java_home}")
        self._write_java_home_to_hadoop_env(java_home)
        self._write_daemon_users_to_hadoop_env()
        os.environ["JAVA_HOME"] = java_home

        # Step 2: SSH pre-flight
        ssh_ok = False
        ssh_error = ""
        try:
            ssh_check = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                 "-o", "StrictHostKeyChecking=no", "localhost", "echo", "ssh_ok"],
                capture_output=True, text=True, timeout=10,
            )
            ssh_ok = ssh_check.returncode == 0 and "ssh_ok" in ssh_check.stdout
            if not ssh_ok:
                ssh_error = ssh_check.stderr.strip() or "SSH returned non-zero"
                logger.warning(f"Passwordless SSH not configured: {ssh_error}")
        except Exception as e:
            ssh_error = str(e)
            logger.warning(f"SSH pre-flight check failed: {e}")

        if not ssh_ok:
            return {
                "returncode": 1, "stdout": "", "stderr": f"SSH pre-flight failed: {ssh_error}",
                "namenode_running": False, "datanode_running": False,
                "secondary_namenode_running": False, "jps_output": "",
                "ssh_ready": False, "daemon_error": None,
                "ssh_fix": (
                    "Run these commands to enable passwordless SSH:\n"
                    "  sudo apt install openssh-server -y\n"
                    "  sudo service ssh start\n"
                    "  ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa\n"
                    "  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys\n"
                    "  chmod 600 ~/.ssh/authorized_keys"
                ),
            }

        # Step 3: Format NameNode if first time
        namenode_current = "/tmp/hadoop-root/dfs/name/current"
        if not os.path.isdir(namenode_current):
            logger.info("Formatting NameNode for first time...")
            env_fmt = os.environ.copy()
            env_fmt["JAVA_HOME"] = java_home
            fmt = subprocess.run(
                [os.path.join(hadoop_home, "bin", "hdfs"), "namenode", "-format", "-force"],
                capture_output=True, text=True, timeout=60, env=env_fmt,
            )
            logger.info(f"Format returncode: {fmt.returncode}")

        # Step 4: Start HDFS
        current_user = self._get_current_user()
        script = os.path.join(hadoop_home, "sbin", "start-dfs.sh")
        env = os.environ.copy()
        env["JAVA_HOME"] = java_home
        env["HADOOP_HOME"] = hadoop_home
        env["PATH"] = f"{java_home}/bin:{hadoop_home}/bin:{hadoop_home}/sbin:{env.get('PATH', '')}"
        env["HDFS_NAMENODE_USER"] = current_user
        env["HDFS_DATANODE_USER"] = current_user
        env["HDFS_SECONDARYNAMENODE_USER"] = current_user
        env["YARN_RESOURCEMANAGER_USER"] = current_user
        env["YARN_NODEMANAGER_USER"] = current_user

        result = subprocess.run([script], capture_output=True, text=True, timeout=90, env=env)

        # Step 5: Wait for async daemons
        time.sleep(5)

        # Step 6: Verify via jps
        jps_bin = shutil.which("jps") or os.path.join(java_home, "bin", "jps")
        try:
            jps_proc = subprocess.run([jps_bin], capture_output=True, text=True, timeout=15, env=env)
            jps_output = jps_proc.stdout
        except Exception as e:
            logger.warning(f"jps check failed: {e}")
            jps_output = ""

        jps_lines = [line.strip() for line in jps_output.splitlines()]
        namenode_running  = any(line.endswith("NameNode") for line in jps_lines)
        datanode_running  = any(line.endswith("DataNode") for line in jps_lines)
        secondary_running = any(line.endswith("SecondaryNameNode") for line in jps_lines)

        # Step 7: Harvest crash reason on failure
        daemon_error = None
        if not (namenode_running and datanode_running):
            daemon_error = self._harvest_daemon_error(java_home)
            logger.error(
                f"Daemons did not start.\njps output:\n{jps_output}\n"
                f"start-dfs.sh stderr:\n{result.stderr.strip()}\n"
                f"Harvested error:\n{daemon_error}"
            )

        return {
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
            "namenode_running": namenode_running,
            "datanode_running": datanode_running,
            "secondary_namenode_running": secondary_running,
            "jps_output": jps_output.strip(),
            "ssh_ready": ssh_ok,
            "daemon_error": daemon_error,
        }

    def _harvest_daemon_error(self, java_home: str) -> str:
        hadoop_home = self._hadoop_home()
        log_dir = os.environ.get("HADOOP_LOG_DIR", os.path.join(hadoop_home, "logs"))
        alt_log_dir = f"/tmp/hadoop-{self._get_current_user()}/logs"

        collected = []
        for search_dir in [log_dir, alt_log_dir]:
            if not os.path.isdir(search_dir):
                continue
            for fname in sorted(os.listdir(search_dir)):
                if not (fname.endswith(".log") or fname.endswith(".out")):
                    continue
                fpath = os.path.join(search_dir, fname)
                try:
                    with open(fpath, "r", errors="ignore") as f:
                        lines = f.readlines()[-40:]
                    hits = [l.strip() for l in lines
                            if any(kw in l for kw in ("ERROR", "FATAL", "Exception", "WARN"))]
                    if hits:
                        collected.append(f"--- {fname} ---")
                        collected.extend(hits[-10:])
                except Exception:
                    pass

        if not collected:
            for search_dir in [log_dir, alt_log_dir]:
                if not os.path.isdir(search_dir):
                    continue
                for fname in os.listdir(search_dir):
                    if "namenode" in fname.lower() and fname.endswith(".log"):
                        try:
                            with open(os.path.join(search_dir, fname), "r", errors="ignore") as f:
                                tail = f.readlines()[-20:]
                            collected.append(f"--- {fname} (tail) ---")
                            collected.extend(l.strip() for l in tail)
                        except Exception:
                            pass
                        break

        return "\n".join(collected) if collected else "No log files found in " + log_dir

    def _stop_hdfs(self, args: dict) -> dict:
        hadoop_home = self._hadoop_home()
        current_user = self._get_current_user()
        script = os.path.join(hadoop_home, "sbin", "stop-dfs.sh")
        env = os.environ.copy()
        env["HADOOP_HOME"] = hadoop_home
        env["HDFS_NAMENODE_USER"] = current_user
        env["HDFS_DATANODE_USER"] = current_user
        env["HDFS_SECONDARYNAMENODE_USER"] = current_user
        env["YARN_RESOURCEMANAGER_USER"] = current_user
        env["YARN_NODEMANAGER_USER"] = current_user
        result = subprocess.run([script], capture_output=True, text=True, timeout=60, env=env)
        return {"returncode": result.returncode, "stdout": result.stdout.strip(), "stderr": result.stderr.strip()}

    def _restart_namenode(self, args: dict) -> dict:
        hadoop_home = self._hadoop_home()
        hdfs = os.path.join(hadoop_home, "bin", "hdfs")
        stop = self._run([hdfs, "--daemon", "stop", "namenode"], timeout=30)
        start = self._run([hdfs, "--daemon", "start", "namenode"], timeout=30)
        return {"stop": stop, "start": start}

    def _restart_datanode(self, args: dict) -> dict:
        hadoop_home = self._hadoop_home()
        hdfs = os.path.join(hadoop_home, "bin", "hdfs")
        stop = self._run([hdfs, "--daemon", "stop", "datanode"], timeout=30)
        start = self._run([hdfs, "--daemon", "start", "datanode"], timeout=30)
        return {"stop": stop, "start": start}

    def _leave_safemode(self, args: dict) -> dict:
        hadoop_home = self._hadoop_home()
        hdfs = shutil.which("hdfs") or os.path.join(hadoop_home, "bin", "hdfs")
        return self._run([hdfs, "dfsadmin", "-safemode", "leave"], timeout=30)

    def _check_hdfs_health(self, args: dict) -> dict:
        hadoop_home = self._hadoop_home()
        hdfs = shutil.which("hdfs") or os.path.join(hadoop_home, "bin", "hdfs")
        return self._run([hdfs, "dfsadmin", "-report"], timeout=30)

    def _analyze_logs(self, args: dict) -> dict:
        hadoop_home = self._hadoop_home()
        log_dir = os.environ.get("HADOOP_LOG_DIR", os.path.join(hadoop_home, "logs"))
        errors = []
        if os.path.isdir(log_dir):
            for fname in os.listdir(log_dir):
                if not fname.endswith(".log"):
                    continue
                fpath = os.path.join(log_dir, fname)
                try:
                    with open(fpath, "r", errors="ignore") as f:
                        lines = f.readlines()[-100:]
                    for line in lines:
                        if "ERROR" in line or "FATAL" in line:
                            errors.append({"file": fname, "line": line.strip()})
                except Exception:
                    pass
        return {"errors_found": len(errors), "errors": errors[:20]}

    def _check_disk_space(self, args: dict) -> dict:
        return self._run(["df", "-h", "/"])

    def _format_namenode(self, args: dict) -> dict:
        hadoop_home = self._hadoop_home()
        hdfs = os.path.join(hadoop_home, "bin", "hdfs")
        return self._run([hdfs, "namenode", "-format", "-force"], timeout=60)

    def _request_human_approval(self, args: dict) -> dict:
        reason = args.get("reason", "Unknown")
        logger.warning(f"HUMAN APPROVAL REQUIRED: {reason}")
        return {"status": "paused", "reason": reason}
