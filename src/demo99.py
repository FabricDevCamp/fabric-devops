"""Setup Deployment Pipelines"""

import os
from fabric_devops import DeploymentManager, StagingEnvironments, EnvironmentSettings

PROJECT_NAME = 'Bob'
SOLUTION_NAME = 'Power BI Solution'

DEPLOYMENT_PIPELINE_NAME = PROJECT_NAME
DEV_WORKSPACE_NAME = f"{PROJECT_NAME}-dev"
TEST_WORKSPACE_NAME = f"{PROJECT_NAME}-test"
PROD_WORKSPACE_NAME = f"{PROJECT_NAME}"

DeploymentManager.delete_deployment_pipeline_by_name(PROJECT_NAME)

DeploymentManager.setup_project_with_deployment_pipeline(PROJECT_NAME, SOLUTION_NAME)

DeploymentManager.deploy_from_dev_to_test(PROJECT_NAME)

DeploymentManager.apply_post_pipeline_deploy_fixes(
    TEST_WORKSPACE_NAME,
    StagingEnvironments.get_test_environment(),
    run_etl_jobs=True)

DeploymentManager.deploy_from_test_to_prod(PROJECT_NAME)

DeploymentManager.apply_post_pipeline_deploy_fixes(
    PROD_WORKSPACE_NAME,
    StagingEnvironments.get_prod_environment(),
    run_etl_jobs=True)


# from fabric_devops import DeploymentManager,  AppLogger, StagingEnvironments,\
#                           AdoProjectManager, FabricRestApi, FabricCicdManager

# PROJECT_NAME = os.getenv("PROJECT_NAME")
# SOLUTION_NAME = os.getenv("SOLUTION_NAME")

# DEV_WORKSPACE_NAME = f"{PROJECT_NAME}-dev"
# TEST_WORKSPACE_NAME = f"{PROJECT_NAME}-test"
# PROD_WORKSPACE_NAME = f"{PROJECT_NAME}"

# dev_deploy_job = StagingEnvironments.get_dev_environment()
# test_deploy_job = StagingEnvironments.get_test_environment()
# prod_deploy_job = StagingEnvironments.get_prod_environment()
    
# if SOLUTION_NAME in ['Shortcut Solution', 'Pipeline Solution', 'Medallion Solution']:
#     DeploymentManager.update_adls_connection_id_variables(SOLUTION_NAME)

# DEV_WORKSPACE = FabricCicdManager.deploy_to_new_workspace(
#     SOLUTION_NAME,
#     DEV_WORKSPACE_NAME,
#     dev_deploy_job.name
# )

# DeploymentManager.update_solution_post_deploy(DEV_WORKSPACE, SOLUTION_NAME)

# TEST_WORKSPACE = FabricCicdManager.deploy_to_new_workspace(
#     SOLUTION_NAME,
#     TEST_WORKSPACE_NAME,
#     test_deploy_job.name)

# DeploymentManager.update_solution_post_deploy(TEST_WORKSPACE, SOLUTION_NAME)

# PROD_WORKSPACE =  FabricCicdManager.deploy_to_new_workspace(
#     SOLUTION_NAME,
#     PROD_WORKSPACE_NAME,
#     prod_deploy_job.name)

# DeploymentManager.update_solution_post_deploy(PROD_WORKSPACE, SOLUTION_NAME)
