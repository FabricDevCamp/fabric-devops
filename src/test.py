"""Setup Deployment Pipelines"""

from fabric_devops import DeploymentManager, StagingEnvironments

PROJECT_NAME = "Apollo"

DEPLOYMENT_PIPELINE_NAME = PROJECT_NAME
DEV_WORKSPACE_NAME = f"{PROJECT_NAME}-dev"
TEST_WORKSPACE_NAME = f"{PROJECT_NAME}-test"
PROD_WORKSPACE_NAME = f"{PROJECT_NAME}"

# DeploymentManager.apply_post_deploy_fixes(
#     TEST_WORKSPACE_NAME,
#     StagingEnvironments.get_test_environment(),
#     run_etl_jobs=True)

# DeploymentManager.deploy_from_test_to_prod(PROJECT_NAME)

DeploymentManager.apply_post_deploy_fixes(
    PROD_WORKSPACE_NAME,
    StagingEnvironments.get_prod_environment(),
    run_etl_jobs=True)
