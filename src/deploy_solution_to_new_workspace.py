"""Deploy solution to new workspace"""
import os

from fabric_devops import DeploymentManager,  AppLogger, StagingEnvironments,\
                          FabricRestApi, AdoProjectManager, GitHubRestApi 

WORKSPACE_NAME = os.getenv("WORKSPACE_NAME")
SOLUTION_NAME = os.getenv("SOLUTION_NAME")
TARGET_ENVIRONMENT = os.getenv("TARGET_ENVIRONMENT")
GIT_INTEGRATION_PROVIDER = os.getenv("GIT_INTEGRATION_PROVIDER")
DEPLOY_USING_FABRIC_CICD = os.getenv("DEPLOY_USING_FABRIC_CICD") == 'true'

match TARGET_ENVIRONMENT:
    case 'dev':
        deploy_job = StagingEnvironments.get_dev_environment()
    case 'test':
        deploy_job = StagingEnvironments.get_test_environment()
    case 'prod':
        deploy_job = StagingEnvironments.get_prod_environment()

workspace = DeploymentManager.deploy_solution_by_name(
    SOLUTION_NAME,
    WORKSPACE_NAME,
    deploy_job,
    DEPLOY_USING_FABRIC_CICD)

match GIT_INTEGRATION_PROVIDER:

    case 'Azure DevOps':
        # create ADO project and connect workspace
        AdoProjectManager.create_project(WORKSPACE_NAME, workspace)
        FabricRestApi.connect_workspace_to_ado_repo(workspace, WORKSPACE_NAME)
        
    case 'GitHub':
        # create GitHub repo and connect workspace
        repo_name = WORKSPACE_NAME.replace(" ", "-")
        GitHubRestApi.create_repository(repo_name)
        FabricRestApi.connect_workspace_to_github_repo(workspace, repo_name)

AppLogger.log_job_complete(workspace.id)
