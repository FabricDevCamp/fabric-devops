"""Deploy Demo Solution with ADO GIT Intergation"""

import os
from fabric_devops import DeploymentManager, StagingEnvironments, EnvironmentSettings, FabricRestApi, \
                          AppLogger, AdoProjectManager

if os.getenv("RUN_AS_SERVICE_PRINCIPAL") == 'true':
    EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = True
else:
    EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False

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

DeploymentManager.setup_one_branch_ado_repo_for_fabric_cicd(
    DEV_WORKSPACE,
    TEST_WORKSPACE,
    PROD_WORKSPACE,
    PROJECT_NAME
)

# create first feature workspace
FEATURE_NAME = 'feature1'
FEATURE_WORKSPACE_NAME = F'{PROJECT_NAME}-dev-{FEATURE_NAME}'
FEATURE_WORKSPACE = FabricRestApi.create_workspace(FEATURE_WORKSPACE_NAME)

AdoProjectManager.create_branch(PROJECT_NAME, FEATURE_NAME, "main")
FabricRestApi.connect_workspace_to_ado_repo(FEATURE_WORKSPACE, PROJECT_NAME, FEATURE_NAME)

DeploymentManager.apply_post_deploy_fixes(
    FEATURE_WORKSPACE_NAME,
    StagingEnvironments.get_dev_environment(),
    True)

AppLogger.log_job_complete(FEATURE_WORKSPACE['id'])

