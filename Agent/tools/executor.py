"""
Tool Executor — Executes validated tool calls.
Each tool maps to a safe, predefined operation.
Never runs arbitrary shell commands from LLM output.

Windows note: installation tools (install_java, install_hadoop) print
guidance instead of running apt-get, since Windows uses different installers.
"""

import subprocess
import logging
import os
import sys
import xml.etree.ElementTree as ET
from typing import Any

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

    # ── Helpers ─────────────────────────────────────────────────────────────

    def _run(self, cmd: list, timeout: int = 60) -> dict:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return {
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }

    def _windows_guidance(self, action: str, url: str, extra: str = "") -> dict:
        msg = (
            f"Windows detected. Cannot auto-{action} via apt-get. "
            f"Please install manually: {url}"
        )
        if extra:
            msg += f" | {extra}"
        logger.warning(f"⚠️  {msg}")
        return {"status": "manual_action_required", "message": msg, "url": url}

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

    # ── Tool Implementations ─────────────────────────────────────────────────

    def _install_java(self, args: dict) -> dict:
        version = args["version"]
        if IS_WINDOWS:
            return self._windows_guidance(
                "install Java",
                f"https://adoptium.net/temurin/releases/?version={version}",
                f"Download OpenJDK {version} installer for Windows"
            )
        pkg = f"openjdk-{version}-jdk"
        result = self._run(["apt-get", "install", "-y", pkg], timeout=180)
        return {"installed": pkg, **result}

    def _install_hadoop(self, args: dict) -> dict:
        version = args["version"]
        if IS_WINDOWS:
            return self._windows_guidance(
                "install Hadoop",
                f"https://hadoop.apache.org/releases.html",
                f"Download hadoop-{version}.tar.gz, extract to C:\\hadoop, set HADOOP_HOME"
            )
        url = f"https://downloads.apache.org/hadoop/common/hadoop-{version}/hadoop-{version}.tar.gz"
        logger.info(f"Downloading Hadoop {version} from {url}")
        dl = self._run(["wget", "-q", "-O", f"/tmp/hadoop-{version}.tar.gz", url], timeout=300)
        if dl["returncode"] != 0:
            return {"error": "Download failed", **dl}
        extract = self._run(["tar", "-xzf", f"/tmp/hadoop-{version}.tar.gz", "-C", "/usr/local/"], timeout=120)
        link = self._run(["ln", "-sfn", f"/usr/local/hadoop-{version}", "/usr/local/hadoop"])
        return {"version": version, "extract": extract, "symlink": link}

    def _configure_java_home(self, args: dict) -> dict:
        if IS_WINDOWS:
            java_home = os.environ.get("JAVA_HOME", "")
            if java_home:
                return {"status": "already_set", "java_home": java_home}
            return self._windows_guidance(
                "set JAVA_HOME",
                "https://www.java.com/en/",
                "Set JAVA_HOME in System Environment Variables"
            )
        try:
            java_path = subprocess.run(["which", "java"], capture_output=True, text=True).stdout.strip()
            real_path = subprocess.run(["readlink", "-f", java_path], capture_output=True, text=True).stdout.strip()
            java_home = os.path.dirname(os.path.dirname(real_path))
        except Exception:
            java_home = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64")

        env_file = f"{HADOOP_HOME}/etc/hadoop/hadoop-env.sh"
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
        return {"java_home": java_home, "configured_in": env_file}

    def _configure_hdfs_site(self, args: dict) -> dict:
        replication = str(args["replication_factor"])
        filepath = os.path.join(HADOOP_HOME, "etc", "hadoop", "hdfs-site.xml")
        self._update_xml_property(filepath, "dfs.replication", replication)
        self._update_xml_property(filepath, "dfs.namenode.name.dir", "file:///data/namenode")
        self._update_xml_property(filepath, "dfs.datanode.data.dir", "file:///data/datanode")
        return {"updated": filepath, "replication_factor": replication}

    def _configure_core_site(self, args: dict) -> dict:
        filepath = os.path.join(HADOOP_HOME, "etc", "hadoop", "core-site.xml")
        self._update_xml_property(filepath, "fs.defaultFS", "hdfs://localhost:9000")
        return {"updated": filepath}

    def _start_hdfs(self, args: dict) -> dict:
        if IS_WINDOWS:
            script = os.path.join(HADOOP_HOME, "sbin", "start-dfs.cmd")
            if os.path.exists(script):
                return self._run([script], timeout=60)
            return self._windows_guidance("start HDFS", "https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html")
        return self._run([f"{HADOOP_HOME}/sbin/start-dfs.sh"], timeout=60)

    def _stop_hdfs(self, args: dict) -> dict:
        if IS_WINDOWS:
            return self._run([os.path.join(HADOOP_HOME, "sbin", "stop-dfs.cmd")], timeout=60)
        return self._run([f"{HADOOP_HOME}/sbin/stop-dfs.sh"], timeout=60)

    def _restart_namenode(self, args: dict) -> dict:
        hadoop_cmd = "hadoop.cmd" if IS_WINDOWS else "hdfs"
        stop = self._run([hadoop_cmd, "--daemon", "stop", "namenode"], timeout=30)
        start = self._run([hadoop_cmd, "--daemon", "start", "namenode"], timeout=30)
        return {"stop": stop, "start": start}

    def _restart_datanode(self, args: dict) -> dict:
        hadoop_cmd = "hadoop.cmd" if IS_WINDOWS else "hdfs"
        stop = self._run([hadoop_cmd, "--daemon", "stop", "datanode"], timeout=30)
        start = self._run([hadoop_cmd, "--daemon", "start", "datanode"], timeout=30)
        return {"stop": stop, "start": start}

    def _leave_safemode(self, args: dict) -> dict:
        return self._run(["hdfs", "dfsadmin", "-safemode", "leave"], timeout=30)

    def _check_hdfs_health(self, args: dict) -> dict:
        return self._run(["hdfs", "dfsadmin", "-report"], timeout=30)

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
        if IS_WINDOWS:
            return self._run(["wmic", "logicaldisk", "get", "size,freespace,caption"])
        return self._run(["df", "-h", "/"])

    def _format_namenode(self, args: dict) -> dict:
        return self._run(["hdfs", "namenode", "-format", "-force"], timeout=60)

    def _request_human_approval(self, args: dict) -> dict:
        reason = args.get("reason", "Unknown reason")
        logger.warning(f"HUMAN APPROVAL REQUIRED: {reason}")
        return {"status": "paused", "reason": reason, "action_required": "human_approval"}