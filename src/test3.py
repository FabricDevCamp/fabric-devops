"""Test3"""
import json
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi, StagingEnvironments, DeploymentJob

workspace = FabricRestApi.get_workspace_by_name('Alpha')
git = FabricRestApi.get_git_connection(workspace['id'])


print( json.dumps(git, indent=4) )
