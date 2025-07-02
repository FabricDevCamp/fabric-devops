"""Deploy solution with GIT Integration"""

import os

from fabric_devops import DeploymentManager, GitHubRestApi, FabricRestApi

PROJECT_NAME = os.getenv("PROJECT_NAME")
SOLUTION_NAME = os.getenv("SOLUTION_NAME")

DEPLOYMENT_PIPELINE_NAME = PROJECT_NAME
DEV_WORKSPACE_NAME = f"{PROJECT_NAME}-dev"
TEST_WORKSPACE_NAME = f"{PROJECT_NAME}-test"
PROD_WORKSPACE_NAME = f"{PROJECT_NAME}"


DEV_WORKSPACE = DeploymentManager.deploy_solution_by_name(DEV_WORKSPACE_NAME, SOLUTION_NAME)

DeploymentManager.connect_workspace_to_github_repo(DEV_WORKSPACE, PROJECT_NAME)

GitHubRestApi.create_and_merge_pull_request(
    PROJECT_NAME,
    'dev', 
    'test',
    'Push dev to test',
    'intial merge')

TEST_WORKSPACE = FabricRestApi.create_workspace(TEST_WORKSPACE_NAME)
FabricRestApi.connect_workspace_to_github_repo(TEST_WORKSPACE, PROJECT_NAME, 'test')
FabricRestApi.disconnect_workspace_from_git(TEST_WORKSPACE['id'])

GitHubRestApi.create_and_merge_pull_request(
    PROJECT_NAME,
    'test', 
    'main',
    'Push test to main',
    'intial merge')

PROD_WORKSPACE = FabricRestApi.create_workspace(PROD_WORKSPACE_NAME)
FabricRestApi.connect_workspace_to_github_repo(PROD_WORKSPACE, PROJECT_NAME, 'main')
FabricRestApi.disconnect_workspace_from_git(PROD_WORKSPACE['id'])
