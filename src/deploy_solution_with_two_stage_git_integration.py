"""Deploy Demo Solution with ADO GIT Intergation"""

import os

from fabric_devops import DeploymentManager, EnvironmentSettings, AppLogger, StagingEnvironments,\
                          AdoProjectManager, FabricRestApi, GitHubRestApi

if os.getenv("RUN_AS_SERVICE_PRINCIPAL") == 'true':
    EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = True
else:
    EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False

SOLUTION_NAME = os.getenv("SOLUTION_NAME")
PROJECT_NAME = os.getenv("PROJECT_NAME")

DEV_WORKSPACE_NAME = F'{PROJECT_NAME}-dev'
PROD_WORKSPACE_NAME = F'{PROJECT_NAME}'

dev_deploy_job = StagingEnvironments.get_dev_environment()
prod_deploy_job = StagingEnvironments.get_prod_environment()

dev_workspace = DeploymentManager.deploy_solution_by_name(
    SOLUTION_NAME, 
    DEV_WORKSPACE_NAME, 
    dev_deploy_job)

prod_workspace = FabricRestApi.create_workspace(PROD_WORKSPACE_NAME)

DeploymentManager.setup_two_stage_ado_repo(
    dev_workspace,
    prod_workspace,
    PROJECT_NAME)



# create feature1 workspace
FEATURE1_NAME = 'feature1'
FEATURE1_WORKSPACE_NAME = F'{DEV_WORKSPACE_NAME}-{FEATURE1_NAME}'
FEATURE1_WORKSPACE = FabricRestApi.create_workspace(FEATURE1_WORKSPACE_NAME)

# create feature1 branch and connect to feature1 workspace
AdoProjectManager.create_branch(PROJECT_NAME, FEATURE1_NAME, 'dev')
FabricRestApi.connect_workspace_to_ado_repo(FEATURE1_WORKSPACE, PROJECT_NAME, FEATURE1_NAME)

# apply post sync/deploy fixes to feature1 workspace
DeploymentManager.apply_post_sync_fixes(
    FEATURE1_WORKSPACE_NAME,
    StagingEnvironments.get_dev_environment(),
    True)

FabricRestApi.commit_workspace_to_git(
    FEATURE1_WORKSPACE['id'],
    commit_comment = 'Sync updates from feature workspace to repo after applying fixes')

# # create feature2 workspace
# FEATURE2_NAME = 'feature2'
# FEATURE2_WORKSPACE_NAME = F'{WORKSPACE_NAME} - {FEATURE2_NAME}'
# FEATURE2_WORKSPACE = FabricRestApi.create_workspace(FEATURE2_WORKSPACE_NAME)

# # create feature2 branch and connect to feature2 workspace
# AdoProjectManager.create_branch(WORKSPACE_NAME, FEATURE2_NAME, 'main')
# FabricRestApi.connect_workspace_to_ado_repo(FEATURE2_WORKSPACE, WORKSPACE_NAME, FEATURE2_NAME)

# # do not apply post sync/deploy fixes to feature2 workspace

# AppLogger.log_job_complete(workspace['id'])
