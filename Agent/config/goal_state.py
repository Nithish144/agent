"""
Goal State — Defines what a healthy Hadoop cluster looks like.
"""

GOAL_STATE = {
    # Java 11 OR 21 both work with Hadoop 3.3.6
    # We remove java_version from goal to avoid false gaps on Java 21 systems
    "java_installed": True,
    "hadoop_installed": True,
    "hadoop_version": "3.3.6",
    "java_home_configured": True,
    "namenode_running": True,
    "datanode_running": True,
    "replication_factor": 3,
    "hdfs_safemode": False,
    "critical_log_errors": False,
}