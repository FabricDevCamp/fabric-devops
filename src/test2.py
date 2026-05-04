"""Deploy solution to new workspace"""
import json

from fabric_devops import DeploymentManager,  AppLogger, StagingEnvironments,\
                          FabricRestApi, AdoProjectManager, ItemDefinitionFactory 


target_workspace = 'Max'

AppLogger.log_job(f"Deploying Notebook Solution to [{target_workspace}] using Fabric REST APIs")

workspace = FabricRestApi.create_workspace(target_workspace)

DeploymentManager.create_web_datasource_url_variable_library(
    workspace, 
    StagingEnvironments.get_dev_environment())

lakehouse_name = "sales"
lakehouse = FabricRestApi.create_lakehouse(workspace.id, lakehouse_name)

create_notebook_request = ItemDefinitionFactory.get_create_notebook_request_from_folder(
    'Notebook Solution',
    'Create Lakehouse Tables.Notebook',
    workspace.id,
    lakehouse
)

notebook_settings_part = create_notebook_request

print( json.dumps(create_notebook_request, indent=4) )

notebook = FabricRestApi.create_item_no_sdk(workspace.id, create_notebook_request)

# create ADO project and connect workspace
AdoProjectManager.create_project(target_workspace, workspace)
FabricRestApi.connect_workspace_to_ado_repo(workspace, target_workspace)

AppLogger.log_job_complete(workspace.id)
