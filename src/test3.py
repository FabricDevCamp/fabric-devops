"""Setup Deployment Pipelines"""

from fabric_devops import DeploymentManager, StagingEnvironments

DEPLOYMENT_PIPELINE_NAME = 'Apollo'
TEST_WORKSPACE_NAME = f"{DEPLOYMENT_PIPELINE_NAME}-test"
PROD_WORKSPACE_NAME = f"{DEPLOYMENT_PIPELINE_NAME}"
LAKEHOUSE_NAME = 'sales'
DATA_PIPELINE_NAME = 'Create Lakehouse Tables'
VARIABLE_LIBRARY_NAME = 'SolutionConfig'

DeploymentManager.deploy_from_dev_to_test(DEPLOYMENT_PIPELINE_NAME)