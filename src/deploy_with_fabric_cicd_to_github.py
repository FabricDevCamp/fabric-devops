"""Deploy solution with fabric_cicd"""

import os

from fabric_devops import DeploymentManager, GitHubRestApi, FabricRestApi, AppLogger, StagingEnvironments

PROJECT_NAME = os.getenv("PROJECT_NAME")
SOLUTION_NAME = os.getenv("SOLUTION_NAME")

DEPLOYMENT_PIPELINE_NAME = PROJECT_NAME
DEV_WORKSPACE_NAME = f"{PROJECT_NAME}-dev"
TEST_WORKSPACE_NAME = f"{PROJECT_NAME}-test"
PROD_WORKSPACE_NAME = f"{PROJECT_NAME}"


DEV_WORKSPACE = DeploymentManager.deploy_solution_by_name(DEV_WORKSPACE_NAME, SOLUTION_NAME)

DeploymentManager.sync_workspace_to_github_repo(DEV_WORKSPACE, PROJECT_NAME)

GitHubRestApi.create_and_merge_pull_request(
    PROJECT_NAME,
    'dev', 
    'test',
    'Push dev to test',
    'intial merge')

TEST_WORKSPACE = FabricRestApi.create_workspace(TEST_WORKSPACE_NAME)
FabricRestApi.connect_workspace_to_github_repo(TEST_WORKSPACE, PROJECT_NAME, 'test')
FabricRestApi.disconnect_workspace_from_git(TEST_WORKSPACE['id'])

DeploymentManager.apply_post_deploy_fixes(
    TEST_WORKSPACE_NAME,
    StagingEnvironments.get_test_environment(),
    True)


GitHubRestApi.create_and_merge_pull_request(
    PROJECT_NAME,
    'test',
    'main',
    'Push test to main',
    'intial merge')

PROD_WORKSPACE = FabricRestApi.create_workspace(PROD_WORKSPACE_NAME)
FabricRestApi.connect_workspace_to_github_repo(PROD_WORKSPACE, PROJECT_NAME, 'main')
FabricRestApi.disconnect_workspace_from_git(PROD_WORKSPACE['id'])

DeploymentManager.apply_post_deploy_fixes(
    PROD_WORKSPACE_NAME,
    StagingEnvironments.get_prod_environment(),
    True)

AppLogger.log_step('Add Workflow Files')
GitHubRestApi.copy_files_from_folder_to_repo(PROJECT_NAME, 'dev', 'SetupForFabricCICD')

GitHubRestApi.create_and_merge_pull_request(
    PROJECT_NAME,
    'dev',
    'test',
    'Push config to test',
    'Push config to test')

GitHubRestApi.create_and_merge_pull_request(
    PROJECT_NAME,
    'test',
    'main',
    'Push config to prod',
    'Push config to prod')


AppLogger.log_step("Create parameter.yml")

parameter_file_content = DeploymentManager.generate_parameter_yml_file(
    DEV_WORKSPACE_NAME,
    TEST_WORKSPACE_NAME,
    PROD_WORKSPACE_NAME
)

GitHubRestApi.write_file_to_repo(
    PROJECT_NAME,
    "dev",
    "workspace/parameter.yml",
    parameter_file_content,
    "param file commit"    
)

AppLogger.log_step("Create workspace.config.json")

workspace_config = DeploymentManager.generate_workspace_config_file(
    DEV_WORKSPACE_NAME,
    TEST_WORKSPACE_NAME,
    PROD_WORKSPACE_NAME
)

GitHubRestApi.write_file_to_repo(
    PROJECT_NAME,
    "dev",
    "workspace/workspace.config.json",
    workspace_config,
    "workspace config file commit"    
)

GitHubRestApi.create_and_merge_pull_request(
    PROJECT_NAME,
    'dev',
    'test',
    'Push config to test',
    'Push config to test')

GitHubRestApi.create_and_merge_pull_request(
    PROJECT_NAME,
    'test',
    'main',
    'Push config to prod',
    'Push config to prod')


# create first feature workspace
FEATURE_NAME = 'feature1'
FEATURE_WORKSPACE_NAME = F'{PROJECT_NAME}-dev-{FEATURE_NAME}'
FEATURE_WORKSPACE = FabricRestApi.create_workspace(FEATURE_WORKSPACE_NAME)
GitHubRestApi.create_branch(PROJECT_NAME,FEATURE_NAME)
FabricRestApi.connect_workspace_to_github_repo(FEATURE_WORKSPACE, PROJECT_NAME, FEATURE_NAME)

DeploymentManager.apply_post_deploy_fixes(
    FEATURE_WORKSPACE_NAME,
    StagingEnvironments.get_dev_environment(),
    True)

