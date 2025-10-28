import json
import os
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi, AdoProjectManager, GitHubRestApi

workspace = FabricRestApi.create_workspace("Test3")

warehouse = FabricRestApi.create_warehouse(workspace['id'], "fuzzy")

# ado_project= AdoProjectManager.create_project('Test3')

github_repo = GitHubRestApi.create_repository("Test3")

FabricRestApi.connect_workspace_to_github_repo(workspace, "Test3" )
