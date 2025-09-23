"""Cleanup Environment"""

from fabric_devops import DeploymentManager, EnvironmentSettings

# DANGER | This script will delete all workspaces, connections,
# DANGER | deployment pipelines and GitHub repos in your test environment

EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False
DeploymentManager.cleanup_dev_environment()
