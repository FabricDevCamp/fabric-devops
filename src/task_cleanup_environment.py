"""Cleanup Environment"""
import os
from fabric_devops import DeploymentManager, EnvironmentSettings

# DANGER | This script will delete all workspaces, connections,
# DANGER | deployment pipelines and GitHub repos in your test environment

if os.getenv("RUN_AS_SERVICE_PRINCIPAL") == 'true':
    EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = True
else:
    EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False

DeploymentManager.cleanup_dev_environment()
