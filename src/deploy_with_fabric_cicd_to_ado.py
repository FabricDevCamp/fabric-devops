"""Deploy solution with fabric_cicd to Azure DevOps Repo"""

import os

from fabric_devops import DeploymentManager, AdoProjectManager, FabricRestApi, AppLogger, StagingEnvironments

PROJECT_NAME = os.getenv("PROJECT_NAME")
SOLUTION_NAME = os.getenv("SOLUTION_NAME")

DEPLOYMENT_PIPELINE_NAME = PROJECT_NAME
DEV_WORKSPACE_NAME = f"{PROJECT_NAME}-dev"
TEST_WORKSPACE_NAME = f"{PROJECT_NAME}-test"
PROD_WORKSPACE_NAME = f"{PROJECT_NAME}"

dev_deploy_job = StagingEnvironments.get_dev_environment()
test_deploy_job = StagingEnvironments.get_test_environment()
prod_deploy_job = StagingEnvironments.get_prod_environment()

DEV_WORKSPACE = DeploymentManager.deploy_solution_by_name(SOLUTION_NAME, DEV_WORKSPACE_NAME)
TEST_WORKSPACE = FabricRestApi.create_workspace(TEST_WORKSPACE_NAME)
PROD_WORKSPACE = FabricRestApi.create_workspace(PROD_WORKSPACE_NAME)

DeploymentManager.setup_ado_repo_for_fabric_cicd(
    DEV_WORKSPACE,
    TEST_WORKSPACE,
    PROD_WORKSPACE,
    PROJECT_NAME
)

# create first feature workspace
FEATURE_NAME = 'feature1'
FEATURE_WORKSPACE_NAME = F'{PROJECT_NAME}-dev-{FEATURE_NAME}'
FEATURE_WORKSPACE = FabricRestApi.create_workspace(FEATURE_WORKSPACE_NAME)

AdoProjectManager.create_branch(PROJECT_NAME, FEATURE_NAME, "dev")
FabricRestApi.connect_workspace_to_ado_repo(FEATURE_WORKSPACE, PROJECT_NAME, FEATURE_NAME)

DeploymentManager.apply_post_deploy_fixes(
    FEATURE_WORKSPACE_NAME,
    StagingEnvironments.get_dev_environment(),
    True)

FabricRestApi.commit_workspace_to_git(
    FEATURE_WORKSPACE['id'],
    commit_comment = 'Sync updates from feature workspace to repo after applying fixes')

AdoProjectManager.create_and_merge_pull_request(PROJECT_NAME, FEATURE_NAME,'dev')
AdoProjectManager.create_and_merge_pull_request(PROJECT_NAME, 'dev', 'test')
AdoProjectManager.create_and_merge_pull_request(PROJECT_NAME, 'test', 'main')

AppLogger.log_job_complete(FEATURE_WORKSPACE['id'])

