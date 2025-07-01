"""Deploy solution with GIT Integration"""

import os

from fabric_devops import DeploymentManager, GitHubRestApi

PROJECT_NAME = os.getenv("PROJECT_NAME")
SOLUTION_NAME = os.getenv("SOLUTION_NAME")

DEPLOYMENT_PIPELINE_NAME = PROJECT_NAME
DEV_WORKSPACE_NAME = f"{PROJECT_NAME}-dev"
TEST_WORKSPACE_NAME = f"{PROJECT_NAME}-test"
PROD_WORKSPACE_NAME = f"{PROJECT_NAME}"

WORKSPACE = None

match os.getenv("SOLUTION_NAME"):

    case 'Custom Power BI Solution':
        WORKSPACE = DeploymentManager.deploy_powerbi_solution(DEV_WORKSPACE_NAME)

    case 'Custom Notebook Solution':
        WORKSPACE = DeploymentManager.deploy_notebook_solution(DEV_WORKSPACE_NAME)

    case 'Custom Shortcut Solution':
        WORKSPACE = DeploymentManager.deploy_shortcut_solution(DEV_WORKSPACE_NAME)

    case 'Custom Data Pipeline Solution':
        WORKSPACE = DeploymentManager.deploy_data_pipeline_solution(DEV_WORKSPACE_NAME)

    case 'Custom Warehouse Solution':
        WORKSPACE = DeploymentManager.deploy_warehouse_solution(DEV_WORKSPACE_NAME)
        
    case 'Custom Realtime Solution':
        WORKSPACE = DeploymentManager.deploy_realtime_solution(DEV_WORKSPACE_NAME)

    case 'Custom Variable Library Solution':
        WORKSPACE = DeploymentManager.deploy_variable_library_solution(DEV_WORKSPACE_NAME)

DeploymentManager.connect_workspace_to_github_repo(WORKSPACE, PROJECT_NAME)

pull_request = GitHubRestApi.create_pull_request(PROJECT_NAME, 'dev', 'test')

pull_request_id = pull_request['number']

GitHubRestApi.merge_pull_request(
    PROJECT_NAME,
    pull_request_id, 
    'Push dev to test', 
    'intial merge')
