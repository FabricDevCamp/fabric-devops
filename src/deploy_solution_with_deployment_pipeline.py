"""Setup Deployment Pipelines"""

from fabric_devops import DeploymentManager, StagingEnvironments

DEPLOYMENT_PIPELINE_NAME = 'Apollo'
TEST_WORKSPACE_NAME = f"{DEPLOYMENT_PIPELINE_NAME}-test"
PROD_WORKSPACE_NAME = f"{DEPLOYMENT_PIPELINE_NAME}"
LAKEHOUSE_NAME = 'sales'
DATA_PIPELINE_NAME = 'Create Lakehouse Tables'
VARIABLE_LIBRARY_NAME = 'SolutionConfig'

def complete_step1():
    """Complete Step 1"""
    DeploymentManager.cleanup_dev_environment()
    DeploymentManager.setup_deployment_pipeline(DEPLOYMENT_PIPELINE_NAME)
    DeploymentManager.deploy_from_dev_to_test(DEPLOYMENT_PIPELINE_NAME)

    DeploymentManager.update_variable_library(
        TEST_WORKSPACE_NAME,
        VARIABLE_LIBRARY_NAME,
        StagingEnvironments.get_test_environment())

    DeploymentManager.run_data_pipeline(TEST_WORKSPACE_NAME, DATA_PIPELINE_NAME)

    DeploymentManager.get_sql_endpoint_info_by_name(TEST_WORKSPACE_NAME, LAKEHOUSE_NAME)

def complete_step2():
    """Complete Step 2"""

    DeploymentManager.deploy_from_dev_to_test(DEPLOYMENT_PIPELINE_NAME)
    DeploymentManager.create_and_bind_model_connection(TEST_WORKSPACE_NAME)

    DeploymentManager.deploy_from_test_to_prod(DEPLOYMENT_PIPELINE_NAME)

    DeploymentManager.update_variable_library(
        PROD_WORKSPACE_NAME,
        VARIABLE_LIBRARY_NAME,
        StagingEnvironments.get_prod_environment())

    DeploymentManager.run_data_pipeline(
        PROD_WORKSPACE_NAME,
        DATA_PIPELINE_NAME)
    
    DeploymentManager.get_sql_endpoint_info_by_name(PROD_WORKSPACE_NAME, LAKEHOUSE_NAME)

def complete_step3():
    """Complete Step 3"""
    DeploymentManager.deploy_from_test_to_prod(DEPLOYMENT_PIPELINE_NAME)
    DeploymentManager.create_and_bind_model_connection(PROD_WORKSPACE_NAME)




# complete_step1()

# BY Hand - set deployment rule for semantic model to lakehouse and then deploy to stage

# complete_step2()

# BY Hand - set deployment rule for semantic model to lakehouse and then deploy to stage

complete_step3()
