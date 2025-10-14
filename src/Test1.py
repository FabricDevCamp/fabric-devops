"""Test1.py"""

import json
from fabric_devops import AppLogger, EntraIdTokenManager, EnvironmentSettings, DeploymentManager, FabricRestApi

# workspace = DeploymentManager.deploy_environment_solution("Shmacme")
# DeploymentManager.sync_workspace_to_github_repo(workspace)

# AppLogger.log_job_complete(workspace['id'])

workspace = FabricRestApi.get_workspace_by_name("Shmacme")
settings = FabricRestApi.get_workspace_spark_settings(workspace['id'])


print ( json.dumps(settings, indent=4) )