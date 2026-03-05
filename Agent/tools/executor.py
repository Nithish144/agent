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
import re
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)
IS_WINDOWS = sys.platform == "win32"


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


class ToolExecutor:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self._dispatch = {
            "install_java":           self._install_java,
            "install_hadoop":         self._install_hadoop,
            "configure_java_home":    self._configure_java_home,
            "configure_hdfs_site":    self._configure_hdfs_site,
            "configure_core_site":    self._configure_core_site,
            "start_hdfs":             self._start_hdfs,
            "stop_hdfs":              self._stop_hdfs,
            "restart_namenode":       self._restart_namenode,
            "restart_datanode":       self._restart_datanode,
            "leave_safemode":         self._leave_safemode,
            "check_hdfs_health":      self._check_hdfs_health,
            "analyze_logs":           self._analyze_logs,
            "check_disk_space":       self._check_disk_space,
            "request_human_approval": self._request_human_approval,
            "format_namenode":        self._format_namenode,
        }

    def _hh(self) -> str:
        return _resolve_hadoop_home()

    def _get_current_user(self) -> str:
        try:
            return pwd.getpwuid(os.getuid()).pw_name
        except Exception:
            return "root"

    def _run(self, cmd: list, timeout: int = 60, env: dict = None) -> dict:
        result = subprocess.run(cmd, capture_output=True, text=True,
                                timeout=timeout, env=env or os.environ.copy())
        return {"returncode": result.returncode,
                "stdout": result.stdout.strip(), "stderr": result.stderr.strip()}

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

    def _write_java_home_to_hadoop_env(self, java_home: str):
        env_file = os.path.join(self._hh(), "etc", "hadoop", "hadoop-env.sh")
        if not os.path.exists(env_file):
            logger.warning(f"hadoop-env.sh not found: {env_file}")
            return
        content = open(env_file).read()
        content = re.sub(r"\n?#?\s*export JAVA_HOME=.*", "", content)
        content = content.rstrip() + f"\nexport JAVA_HOME={java_home}\n"
        open(env_file, "w").write(content)
        logger.info(f"Wrote JAVA_HOME={java_home} to hadoop-env.sh")

    def _write_daemon_users_to_hadoop_env(self):
        """
        Persist HDFS + YARN daemon user vars into hadoop-env.sh.
        This is what makes start-dfs.sh / stop-dfs.sh / start-all.sh / stop-all.sh
        work from ANY terminal with zero manual exports — permanently.
        """
        hadoop_home = self._hh()
        env_file = os.path.join(hadoop_home, "etc", "hadoop", "hadoop-env.sh")
        if not os.path.exists(env_file):
            logger.warning(f"hadoop-env.sh not found: {env_file}")
            return
        user = self._get_current_user()
        content = open(env_file).read()
        for key in ["HDFS_NAMENODE_USER", "HDFS_DATANODE_USER", "HDFS_SECONDARYNAMENODE_USER",
                    "YARN_RESOURCEMANAGER_USER", "YARN_NODEMANAGER_USER"]:
            if re.search(rf"export {key}=", content):
                content = re.sub(rf"export {key}=\S+", f"export {key}={user}", content)
            else:
                content = content.rstrip() + f"\nexport {key}={user}\n"
        open(env_file, "w").write(content)
        # Also set in current process
        for key in ["HDFS_NAMENODE_USER", "HDFS_DATANODE_USER", "HDFS_SECONDARYNAMENODE_USER",
                    "YARN_RESOURCEMANAGER_USER", "YARN_NODEMANAGER_USER"]:
            os.environ[key] = user
        logger.info(f"Wrote daemon users (user={user}) to {env_file}")

    def _write_profile_d(self, hadoop_home: str, java_home: str):
        """
        Write /etc/profile.d/hadoop.sh — sourced by every login shell.
        Also patches ~/.bashrc and /home/ubuntu/.bashrc for non-login terminals.
        """
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
            logger.info(f"Wrote {profile_file}")
        except Exception as e:
            logger.warning(f"Could not write {profile_file}: {e}")

        source_line = f"\n# Hadoop AI Agent\n[ -f {profile_file} ] && source {profile_file}\n"
        import pwd as _pwd
        bashrc_files = {os.path.expanduser("~/.bashrc")}
        try:
            for entry in _pwd.getpwall():
                if entry.pw_uid >= 1000 and os.path.isdir(entry.pw_dir):
                    bashrc_files.add(os.path.join(entry.pw_dir, ".bashrc"))
        except Exception:
            pass
        for bashrc in bashrc_files:
            try:
                try:
                    existing = open(bashrc).read()
                except FileNotFoundError:
                    existing = ""
                if profile_file not in existing:
                    open(bashrc, "a").write(source_line)
                    logger.info(f"Patched {bashrc}")
            except Exception as e:
                # Try sudo for protected files
                try:
                    result = subprocess.run(
                        ["sudo", "-n", "bash", "-c",
                         f"grep -q '{profile_file}' {bashrc} 2>/dev/null || echo '\n# Hadoop AI Agent\n[ -f {profile_file} ] && source {profile_file}' >> {bashrc}"],
                        capture_output=True, text=True, timeout=10)
                    if result.returncode == 0:
                        logger.info(f"Patched {bashrc} via sudo")
                except Exception:
                    logger.warning(f"Could not patch {bashrc}: {e}")

    def execute(self, tool_name: str, arguments: dict) -> dict:
        if tool_name not in self._dispatch:
            return {"success": False, "error": f"Unknown tool: {tool_name}"}
        if self.dry_run:
            return {"success": True, "dry_run": True, "tool": tool_name}
        try:
            return {"success": True, **self._dispatch[tool_name](arguments)}
        except Exception as e:
            logger.error(f"Tool error [{tool_name}]: {e}")
            return {"success": False, "error": str(e)}

    def _sudo(self, cmd: list) -> list:
        """Prepend sudo -n if not already running as root."""
        return cmd if os.getuid() == 0 else ["sudo", "-n"] + cmd

    def _install_java(self, args: dict) -> dict:
        version = args["version"]
        if IS_WINDOWS:
            return {"status": "manual_required"}
        if shutil.which("java"):
            java_home = _resolve_java_home()
            os.environ["JAVA_HOME"] = java_home
            return {"installed": "java (already present)", "returncode": 0, "stdout": "", "stderr": ""}
        self._run(self._sudo(["apt-get", "update", "-qq"]), timeout=120)
        result = self._run(self._sudo(["apt-get", "install", "-y", f"openjdk-{version}-jdk"]), timeout=300)
        if result["returncode"] == 0:
            java_home = _resolve_java_home()
            os.environ["JAVA_HOME"] = java_home
            os.environ["PATH"] = java_home + "/bin:" + os.environ.get("PATH", "")
        else:
            logger.error(f"Java install failed (rc={result['returncode']}): {result['stderr']}")
        return {"installed": f"openjdk-{version}-jdk", **result}

    def _install_hadoop(self, args: dict) -> dict:
        version = args["version"]
        if IS_WINDOWS:
            return {"status": "manual_required"}
        hadoop_dir = f"/usr/local/hadoop-{version}"
        if os.path.isdir(hadoop_dir):
            self._run(self._sudo(["ln", "-sfn", hadoop_dir, "/usr/local/hadoop"]))
            os.environ["HADOOP_HOME"] = hadoop_dir
            os.environ["PATH"] = f"{hadoop_dir}/bin:{hadoop_dir}/sbin:{os.environ.get('PATH','')}"
            return {"version": version, "status": "already_exists"}
        # Try multiple mirrors in order — fallback if one fails
        mirrors = [
            f"https://archive.apache.org/dist/hadoop/common/hadoop-{version}/hadoop-{version}.tar.gz",
            f"https://downloads.apache.org/hadoop/common/hadoop-{version}/hadoop-{version}.tar.gz",
        ]
        tarfile = f"/tmp/hadoop-{version}.tar.gz"
        dl = {"returncode": 1, "stdout": "", "stderr": "no mirrors tried"}
        for url in mirrors:
            logger.info(f"Downloading Hadoop from {url} ...")
            dl = self._run(
                ["wget", "--continue", "--tries=3", "--timeout=60",
                 "--show-progress", "-O", tarfile, url],
                timeout=3600  # 1 hour max — resumes if interrupted
            )
            if dl["returncode"] == 0:
                break
            logger.warning(f"Mirror failed: {url} — trying next mirror")
        if dl["returncode"] != 0:
            return {"error": "Download failed", **dl}
        extract = self._run(self._sudo(["tar", "-xzf", tarfile, "-C", "/usr/local/"]), timeout=120)
        self._run(self._sudo(["ln", "-sfn", hadoop_dir, "/usr/local/hadoop"]))
        os.environ["HADOOP_HOME"] = hadoop_dir
        os.environ["PATH"] = f"{hadoop_dir}/bin:{hadoop_dir}/sbin:{os.environ.get('PATH','')}"
        try:
            os.remove(tarfile)
        except Exception:
            pass
        return {"version": version, "extract": extract}

    def _configure_java_home(self, args: dict) -> dict:
        if IS_WINDOWS:
            return {"status": "manual_required"}
        java_home = _resolve_java_home()
        os.environ["JAVA_HOME"] = java_home
        self._write_java_home_to_hadoop_env(java_home)
        return {"java_home": java_home}

    def _configure_core_site(self, args: dict) -> dict:
        filepath = os.path.join(self._hh(), "etc", "hadoop", "core-site.xml")
        self._update_xml_property(filepath, "fs.defaultFS", "hdfs://localhost:9000")
        return {"updated": filepath}

    def _configure_hdfs_site(self, args: dict) -> dict:
        replication = str(args["replication_factor"])
        filepath = os.path.join(self._hh(), "etc", "hadoop", "hdfs-site.xml")
        self._update_xml_property(filepath, "dfs.replication", replication)
        self._update_xml_property(filepath, "dfs.namenode.name.dir", "file:///data/namenode")
        self._update_xml_property(filepath, "dfs.datanode.data.dir", "file:///data/datanode")
        os.makedirs("/data/namenode", exist_ok=True)
        os.makedirs("/data/datanode", exist_ok=True)
        return {"updated": filepath, "replication_factor": replication}

    def _start_hdfs(self, args: dict) -> dict:
        hadoop_home = self._hh()
        java_home   = _resolve_java_home()
        user        = self._get_current_user()
        logger.info(f"start_hdfs: HADOOP_HOME={hadoop_home} JAVA_HOME={java_home} user={user}")

        self._write_java_home_to_hadoop_env(java_home)
        self._write_daemon_users_to_hadoop_env()
        self._write_profile_d(hadoop_home, java_home)
        os.environ["JAVA_HOME"]   = java_home
        os.environ["HADOOP_HOME"] = hadoop_home

        # SSH pre-flight — auto-fix if not ready
        try:
            chk = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                 "-o", "StrictHostKeyChecking=no", "localhost", "echo", "ssh_ok"],
                capture_output=True, text=True, timeout=10)
            ssh_ok = chk.returncode == 0 and "ssh_ok" in chk.stdout
        except Exception:
            ssh_ok = False

        if not ssh_ok:
            logger.info("SSH not ready — auto-fixing SSH...")
            try:
                self._run(self._sudo(["apt-get", "install", "-y", "openssh-server"]), timeout=120)
                self._run(self._sudo(["service", "ssh", "start"]), timeout=30)
                ssh_dir = os.path.expanduser("~/.ssh")
                os.makedirs(ssh_dir, exist_ok=True)
                os.chmod(ssh_dir, 0o700)
                id_rsa = os.path.join(ssh_dir, "id_rsa")
                if not os.path.isfile(id_rsa):
                    subprocess.run(
                        ["ssh-keygen", "-t", "rsa", "-P", "", "-f", id_rsa],
                        capture_output=True, text=True, timeout=15)
                pub_key_file = id_rsa + ".pub"
                auth_keys = os.path.join(ssh_dir, "authorized_keys")
                if os.path.isfile(pub_key_file):
                    pub_key = open(pub_key_file).read().strip()
                    existing = open(auth_keys).read() if os.path.isfile(auth_keys) else ""
                    if pub_key not in existing:
                        open(auth_keys, "a").write(pub_key + "\n")
                if os.path.isfile(auth_keys):
                    os.chmod(auth_keys, 0o600)
                time.sleep(2)
                chk2 = subprocess.run(
                    ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                     "-o", "StrictHostKeyChecking=no", "localhost", "echo", "ssh_ok"],
                    capture_output=True, text=True, timeout=10)
                ssh_ok = chk2.returncode == 0 and "ssh_ok" in chk2.stdout
                logger.info(f"SSH auto-fix done: ssh_ok={ssh_ok}")
            except Exception as e:
                logger.error(f"SSH auto-fix error: {e}")

        if not ssh_ok:
            return {
                "returncode": 1, "stdout": "", "stderr": "SSH auto-fix failed",
                "namenode_running": False, "datanode_running": False,
                "secondary_namenode_running": False, "jps_output": "",
                "ssh_ready": False,
                "daemon_error": "SSH could not be configured. Run manually: sudo apt install openssh-server -y && sudo service ssh start",
            }


        # Format NameNode if first time
        if not os.path.isdir("/tmp/hadoop-root/dfs/name/current"):
            logger.info("Formatting NameNode...")
            e = os.environ.copy()
            e["JAVA_HOME"] = java_home
            subprocess.run([os.path.join(hadoop_home, "bin", "hdfs"),
                            "namenode", "-format", "-force"],
                           capture_output=True, text=True, timeout=60, env=e)

        env = os.environ.copy()
        env["JAVA_HOME"]                   = java_home
        env["HADOOP_HOME"]                 = hadoop_home
        env["PATH"]                        = f"{java_home}/bin:{hadoop_home}/bin:{hadoop_home}/sbin:{env.get('PATH','')}"
        env["HDFS_NAMENODE_USER"]          = user
        env["HDFS_DATANODE_USER"]          = user
        env["HDFS_SECONDARYNAMENODE_USER"] = user
        env["YARN_RESOURCEMANAGER_USER"]   = user
        env["YARN_NODEMANAGER_USER"]       = user

        result = subprocess.run(
            [os.path.join(hadoop_home, "sbin", "start-dfs.sh")],
            capture_output=True, text=True, timeout=90, env=env)
        time.sleep(5)

        jps_bin = shutil.which("jps") or os.path.join(java_home, "bin", "jps")
        try:
            jps_out = subprocess.run([jps_bin], capture_output=True,
                                     text=True, timeout=15, env=env).stdout
        except Exception:
            jps_out = ""

        lines             = [l.strip() for l in jps_out.splitlines()]
        namenode_running  = any(l.endswith("NameNode") for l in lines)
        datanode_running  = any(l.endswith("DataNode") for l in lines)
        secondary_running = any(l.endswith("SecondaryNameNode") for l in lines)

        daemon_error = None
        if not (namenode_running and datanode_running):
            daemon_error = self._harvest_daemon_error(java_home, hadoop_home)
            logger.error(f"Daemons failed. jps:\n{jps_out}\nstderr:\n{result.stderr}")

        return {
            "returncode":                 result.returncode,
            "stdout":                     result.stdout.strip(),
            "stderr":                     result.stderr.strip(),
            "namenode_running":           namenode_running,
            "datanode_running":           datanode_running,
            "secondary_namenode_running": secondary_running,
            "jps_output":                 jps_out.strip(),
            "ssh_ready":                  ssh_ok,
            "daemon_error":               daemon_error,
        }

    def _harvest_daemon_error(self, java_home: str, hadoop_home: str = None) -> str:
        if not hadoop_home:
            hadoop_home = self._hh()
        log_dirs = [os.path.join(hadoop_home, "logs"),
                    f"/tmp/hadoop-{self._get_current_user()}/logs"]
        collected = []
        for d in log_dirs:
            if not os.path.isdir(d):
                continue
            for fname in sorted(os.listdir(d)):
                if not (fname.endswith(".log") or fname.endswith(".out")):
                    continue
                try:
                    lines = open(os.path.join(d, fname), errors="ignore").readlines()[-40:]
                    hits = [l.strip() for l in lines
                            if any(k in l for k in ("ERROR", "FATAL", "Exception"))]
                    if hits:
                        collected.append(f"--- {fname} ---")
                        collected.extend(hits[-10:])
                except Exception:
                    pass
        return "\n".join(collected) if collected else "No logs found"

    def _stop_hdfs(self, args: dict) -> dict:
        hadoop_home = self._hh()
        user        = self._get_current_user()
        env = os.environ.copy()
        env["HADOOP_HOME"]                 = hadoop_home
        env["JAVA_HOME"]                   = _resolve_java_home()
        env["HDFS_NAMENODE_USER"]          = user
        env["HDFS_DATANODE_USER"]          = user
        env["HDFS_SECONDARYNAMENODE_USER"] = user
        env["YARN_RESOURCEMANAGER_USER"]   = user
        env["YARN_NODEMANAGER_USER"]       = user
        result = subprocess.run(
            [os.path.join(hadoop_home, "sbin", "stop-dfs.sh")],
            capture_output=True, text=True, timeout=60, env=env)
        return {"returncode": result.returncode,
                "stdout": result.stdout.strip(), "stderr": result.stderr.strip()}

    def _restart_namenode(self, args: dict) -> dict:
        hdfs = os.path.join(self._hh(), "bin", "hdfs")
        return {"stop": self._run([hdfs, "--daemon", "stop", "namenode"], 30),
                "start": self._run([hdfs, "--daemon", "start", "namenode"], 30)}

    def _restart_datanode(self, args: dict) -> dict:
        hdfs = os.path.join(self._hh(), "bin", "hdfs")
        return {"stop": self._run([hdfs, "--daemon", "stop", "datanode"], 30),
                "start": self._run([hdfs, "--daemon", "start", "datanode"], 30)}

    def _leave_safemode(self, args: dict) -> dict:
        hdfs = shutil.which("hdfs") or os.path.join(self._hh(), "bin", "hdfs")
        return self._run([hdfs, "dfsadmin", "-safemode", "leave"], 30)

    def _check_hdfs_health(self, args: dict) -> dict:
        hdfs = shutil.which("hdfs") or os.path.join(self._hh(), "bin", "hdfs")
        return self._run([hdfs, "dfsadmin", "-report"], 30)

    def _analyze_logs(self, args: dict) -> dict:
        log_dir = os.path.join(self._hh(), "logs")
        errors = []
        if os.path.isdir(log_dir):
            for fname in os.listdir(log_dir):
                if not fname.endswith(".log"):
                    continue
                try:
                    for line in open(os.path.join(log_dir, fname), errors="ignore").readlines()[-100:]:
                        if "ERROR" in line or "FATAL" in line:
                            errors.append({"file": fname, "line": line.strip()})
                except Exception:
                    pass
        return {"errors_found": len(errors), "errors": errors[:20]}

    def _check_disk_space(self, args: dict) -> dict:
        return self._run(["df", "-h", "/"])

    def _format_namenode(self, args: dict) -> dict:
        return self._run([os.path.join(self._hh(), "bin", "hdfs"),
                          "namenode", "-format", "-force"], 60)

    def _request_human_approval(self, args: dict) -> dict:
        reason = args.get("reason", "Unknown")
        logger.warning(f"HUMAN APPROVAL REQUIRED: {reason}")
        return {"status": "paused", "reason": reason}
