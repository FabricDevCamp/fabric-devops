"""Test1.py"""

from fabric_devops import AppLogger, EntraIdTokenManager, EnvironmentSettings, DeploymentManager, FabricRestApi

workspace = FabricRestApi.get_workspace_by_name("billy Bob")

DeploymentManager.sync_workspace_to_github_repo(workspace)

AppLogger.log_job_complete(workspace['id'])
