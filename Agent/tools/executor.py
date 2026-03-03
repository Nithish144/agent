"""
Tool Executor — Safe predefined tool implementations.
Windows-aware. Never runs arbitrary LLM shell commands.
"""

import subprocess
import logging
import os
import sys
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

HADOOP_HOME = os.environ.get("HADOOP_HOME", "/usr/local/hadoop")
IS_WINDOWS = sys.platform == "win32"


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

    def _install_java(self, args: dict) -> dict:
        version = args["version"]
        if IS_WINDOWS:
            return {"status": "manual_required", "message": f"Download OpenJDK {version} from https://adoptium.net"}

        import shutil as _shutil
        if _shutil.which("java"):
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
            logger.info(f"Java {version} installed successfully")
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

        # Check if already extracted (avoid re-download)
        hadoop_dir = f"/usr/local/hadoop-{version}"
        if os.path.isdir(hadoop_dir):
            logger.info(f"Hadoop {version} already extracted at {hadoop_dir}, creating symlink only")
            link = self._run(["ln", "-sfn", hadoop_dir, "/usr/local/hadoop"])
            # Add to PATH for current process
            os.environ["PATH"] = f"/usr/local/hadoop/bin:{os.environ.get('PATH', '')}"
            os.environ["HADOOP_HOME"] = "/usr/local/hadoop"
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
        # Update PATH for current process so state_detector finds it
        os.environ["PATH"] = f"/usr/local/hadoop/bin:{os.environ.get('PATH', '')}"
        os.environ["HADOOP_HOME"] = "/usr/local/hadoop"
        # Cleanup tar
        try:
            os.remove(tarfile)
        except Exception:
            pass
        return {"version": version, "extract": extract, "symlink": link}

    def _configure_java_home(self, args: dict) -> dict:
        if IS_WINDOWS:
            return {"status": "manual_required", "message": "Set JAVA_HOME in System Environment Variables"}
        # Auto-detect JAVA_HOME
        try:
            java_path = subprocess.run(["which", "java"], capture_output=True, text=True).stdout.strip()
            real_path = subprocess.run(["readlink", "-f", java_path], capture_output=True, text=True).stdout.strip()
            java_home = os.path.dirname(os.path.dirname(real_path))
        except Exception:
            java_home = "/usr/lib/jvm/java-11-openjdk-amd64"

        # Set for current process
        os.environ["JAVA_HOME"] = java_home

        # Write to hadoop-env.sh
        env_file = os.path.join(HADOOP_HOME, "etc", "hadoop", "hadoop-env.sh")
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

        # Also write to /etc/environment for persistence
        try:
            with open("/etc/environment", "a") as f:
                f.write(f'\nJAVA_HOME="{java_home}"\n')
        except Exception:
            pass

        logger.info(f"JAVA_HOME set to {java_home}")
        return {"java_home": java_home, "configured_in": env_file}

    def _configure_hdfs_site(self, args: dict) -> dict:
        replication = str(args["replication_factor"])
        filepath = os.path.join(HADOOP_HOME, "etc", "hadoop", "hdfs-site.xml")
        self._update_xml_property(filepath, "dfs.replication", replication)
        self._update_xml_property(filepath, "dfs.namenode.name.dir", "file:///data/namenode")
        self._update_xml_property(filepath, "dfs.datanode.data.dir", "file:///data/datanode")
        # Create data dirs
        os.makedirs("/data/namenode", exist_ok=True)
        os.makedirs("/data/datanode", exist_ok=True)
        return {"updated": filepath, "replication_factor": replication}

    def _configure_core_site(self, args: dict) -> dict:
        filepath = os.path.join(HADOOP_HOME, "etc", "hadoop", "core-site.xml")
        self._update_xml_property(filepath, "fs.defaultFS", "hdfs://localhost:9000")
        return {"updated": filepath}

    def _auto_detect_java_home(self) -> str:
        """Always auto-detect JAVA_HOME from system — never rely on env variable."""
        try:
            java_bin = subprocess.run(["which", "java"], capture_output=True, text=True).stdout.strip()
            if java_bin:
                real = subprocess.run(["readlink", "-f", java_bin], capture_output=True, text=True).stdout.strip()
                return os.path.dirname(os.path.dirname(real))
        except Exception:
            pass
        # Fallback: search common locations
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
        """Write JAVA_HOME into hadoop-env.sh so start-dfs.sh always finds it."""
        env_file = os.path.join(HADOOP_HOME, "etc", "hadoop", "hadoop-env.sh")
        if not os.path.exists(env_file):
            logger.warning(f"hadoop-env.sh not found at {env_file}")
            return
        with open(env_file, "r") as f:
            content = f.read()
        # Remove old JAVA_HOME lines and add fresh one
        import re
        content = re.sub(r"\n?#?\s*export JAVA_HOME=.*", "", content)
        content = content.rstrip() + f"\nexport JAVA_HOME={java_home}\n"
        with open(env_file, "w") as f:
            f.write(content)
        logger.info(f"✅ Wrote JAVA_HOME={java_home} into hadoop-env.sh")

    def _start_hdfs(self, args: dict) -> dict:
        # Step 1: Auto-detect JAVA_HOME and write to hadoop-env.sh
        java_home = self._auto_detect_java_home()
        logger.info(f"Auto-detected JAVA_HOME: {java_home}")
        self._write_java_home_to_hadoop_env(java_home)
        os.environ["JAVA_HOME"] = java_home

        # Step 2: Format NameNode if first time
        namenode_current = "/tmp/hadoop-root/dfs/name/current"
        if not os.path.isdir(namenode_current):
            logger.info("Formatting NameNode for first time...")
            env_fmt = os.environ.copy()
            env_fmt["JAVA_HOME"] = java_home
            fmt = subprocess.run(
                [os.path.join(HADOOP_HOME, "bin", "hdfs"), "namenode", "-format", "-force"],
                capture_output=True, text=True, timeout=60, env=env_fmt
            )
            logger.info(f"Format returncode: {fmt.returncode}")

        # Step 3: Start HDFS with full env
        script = os.path.join(HADOOP_HOME, "sbin", "start-dfs.sh")
        env = os.environ.copy()
        env["JAVA_HOME"] = java_home
        env["HADOOP_HOME"] = HADOOP_HOME
        env["PATH"] = java_home + "/bin:" + HADOOP_HOME + "/bin:" + env.get("PATH", "")
        env["HDFS_NAMENODE_USER"] = "root"
        env["HDFS_DATANODE_USER"] = "root"
        env["HDFS_SECONDARYNAMENODE_USER"] = "root"
        result = subprocess.run([script], capture_output=True, text=True, timeout=90, env=env)
        return {"returncode": result.returncode, "stdout": result.stdout.strip(), "stderr": result.stderr.strip()}

    def _stop_hdfs(self, args: dict) -> dict:
        script = os.path.join(HADOOP_HOME, "sbin", "stop-dfs.sh")
        env = os.environ.copy()
        env["HDFS_NAMENODE_USER"] = "root"
        env["HDFS_DATANODE_USER"] = "root"
        env["HDFS_SECONDARYNAMENODE_USER"] = "root"
        result = subprocess.run([script], capture_output=True, text=True, timeout=60, env=env)
        return {"returncode": result.returncode, "stdout": result.stdout.strip(), "stderr": result.stderr.strip()}

    def _restart_namenode(self, args: dict) -> dict:
        hdfs = os.path.join(HADOOP_HOME, "bin", "hdfs")
        stop = self._run([hdfs, "--daemon", "stop", "namenode"], timeout=30)
        start = self._run([hdfs, "--daemon", "start", "namenode"], timeout=30)
        return {"stop": stop, "start": start}

    def _restart_datanode(self, args: dict) -> dict:
        hdfs = os.path.join(HADOOP_HOME, "bin", "hdfs")
        stop = self._run([hdfs, "--daemon", "stop", "datanode"], timeout=30)
        start = self._run([hdfs, "--daemon", "start", "datanode"], timeout=30)
        return {"stop": stop, "start": start}

    def _leave_safemode(self, args: dict) -> dict:
        hdfs = shutil.which("hdfs") or os.path.join(HADOOP_HOME, "bin", "hdfs")
        return self._run([hdfs, "dfsadmin", "-safemode", "leave"], timeout=30)

    def _check_hdfs_health(self, args: dict) -> dict:
        hdfs = shutil.which("hdfs") or os.path.join(HADOOP_HOME, "bin", "hdfs")
        return self._run([hdfs, "dfsadmin", "-report"], timeout=30)

    def _analyze_logs(self, args: dict) -> dict:
        log_dir = os.environ.get("HADOOP_LOG_DIR", os.path.join(HADOOP_HOME, "logs"))
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
        hdfs = os.path.join(HADOOP_HOME, "bin", "hdfs")
        return self._run([hdfs, "namenode", "-format", "-force"], timeout=60)

    def _request_human_approval(self, args: dict) -> dict:
        reason = args.get("reason", "Unknown")
        logger.warning(f"HUMAN APPROVAL REQUIRED: {reason}")
        return {"status": "paused", "reason": reason}


import shutil
