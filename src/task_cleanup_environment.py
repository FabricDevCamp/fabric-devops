"""Cleanup Environment"""

from fabric_devops import DeploymentManager


# DANGER | This script will delete all workspaces, connections, 
# DANGER | deployment pipelines and GitHub repos in your test environment
DeploymentManager.cleanup_dev_environment()
