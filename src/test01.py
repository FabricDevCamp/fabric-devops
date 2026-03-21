"""Deploy solution to new workspace"""
import os

from fabric_devops import DeploymentManager,  AppLogger, StagingEnvironments,\
                          FabricRestApi, AdoProjectManager, GitHubRestApi 

WORKSPACE_NAME = 'Minnie'
SOLUTION_NAME = 'Power BI Solution'

deploy_job = StagingEnvironments.get_dev_environment()

workspace = DeploymentManager.deploy_solution_by_name(
    SOLUTION_NAME,
    WORKSPACE_NAME,
    deploy_job)

# create ADO project and connect workspace
AdoProjectManager.create_project(WORKSPACE_NAME, workspace)
FabricRestApi.connect_workspace_to_ado_repo(workspace, WORKSPACE_NAME)

AppLogger.log_job_complete(workspace.id)
