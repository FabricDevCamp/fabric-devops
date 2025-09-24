"""Test1"""

from fabric_devops import DeploymentManager, StagingEnvironments, AppLogger, FabricRestApi

SOLUTION_NAME = "Custom Power BI Solution"
PROJECT_NAME = "Pammy"

DEPLOYMENT_PIPELINE_NAME = PROJECT_NAME
DEV_WORKSPACE_NAME = f"{PROJECT_NAME}-dev"
TEST_WORKSPACE_NAME = f"{PROJECT_NAME}-test"
PROD_WORKSPACE_NAME = f"{PROJECT_NAME}"

DeploymentManager.delete_deployment_pipeline_by_name(PROJECT_NAME)

DeploymentManager.setup_deployment_pipeline(PROJECT_NAME, SOLUTION_NAME)

DeploymentManager.deploy_from_dev_to_test(PROJECT_NAME)

DeploymentManager.apply_post_deploy_fixes(
    TEST_WORKSPACE_NAME,
    StagingEnvironments.get_test_environment(),
    run_etl_jobs=True)

DeploymentManager.deploy_from_test_to_prod(PROJECT_NAME)

DeploymentManager.apply_post_deploy_fixes(
    PROD_WORKSPACE_NAME,
    StagingEnvironments.get_prod_environment(),
    run_etl_jobs=True)
