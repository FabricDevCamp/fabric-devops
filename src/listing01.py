"""Deploy solution to new workspace"""
import json

from fabric_devops import DeploymentManager,  AppLogger, StagingEnvironments,\
                          FabricRestApi, AdoProjectManager, GitHubRestApi 

WORKSPACE_NAME = 'Product Sales-dev'
NOTEBOOK_NAME = 'Build 01 Silver Tables'

workspace = FabricRestApi.get_workspace_by_name(WORKSPACE_NAME)
notebook = FabricRestApi.get_item_by_name(workspace.id, NOTEBOOK_NAME, 'Notebook')

notebook_def = FabricRestApi.get_item_definition(workspace.id, notebook).as_dict()

print( json.dumps(notebook_def, indent=4))
