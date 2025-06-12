"""Setup Deployment Pipelines"""

import os

from fabric_devops import DeploymentManager, FabricRestApi, StagingEnvironments

PROJECT_NAME = os.getenv("PROJECT_NAME")
SOLUTION_NAME = os.getenv("SOLUTION_NAME")

DEPLOYMENT_PIPELINE_NAME = PROJECT_NAME
DEV_WORKSPACE_NAME = f"{PROJECT_NAME}-dev"
TEST_WORKSPACE_NAME = f"{PROJECT_NAME}-test"
PROD_WORKSPACE_NAME = f"{PROJECT_NAME}"

DeploymentManager.delete_deployment_pipeline_by_name(PROJECT_NAME)

DeploymentManager.setup_deployment_pipeline(PROJECT_NAME, SOLUTION_NAME)

DeploymentManager.deploy_from_dev_to_test(PROJECT_NAME)
# apply post deploy fixes to test
TEST_WORKSPACE = FabricRestApi.get_workspace_by_name(TEST_WORKSPACE_NAME)
SEMANTIC_MODEL_NAME = 'Product Sales Imported Model'
TEST_SEMANTIC_MODEL = FabricRestApi.get_item_by_name(
    TEST_WORKSPACE['id'],
    SEMANTIC_MODEL_NAME,
    'SemanticModel')
DEPOYMENT_JOB = StagingEnvironments.get_test_environment()
TEST_DATASOURCE_PATH = DEPOYMENT_JOB.parameters[DEPOYMENT_JOB.web_datasource_path_parameter]

DeploymentManager.update_imported_semantic_model_source(
    TEST_WORKSPACE_NAME, 
    SEMANTIC_MODEL_NAME,
    TEST_DATASOURCE_PATH)

FabricRestApi.create_and_bind_semantic_model_connecton(TEST_WORKSPACE, TEST_SEMANTIC_MODEL['id'])

DeploymentManager.deploy_from_test_to_prod(PROJECT_NAME)

# apply post deploy fixes to prod
PROD_WORKSPACE = FabricRestApi.get_workspace_by_name(PROD_WORKSPACE_NAME)
SEMANTIC_MODEL_NAME = 'Product Sales Imported Model'
PROD_SEMANTIC_MODEL = FabricRestApi.get_item_by_name(
    PROD_WORKSPACE['id'],
    SEMANTIC_MODEL_NAME,
    'SemanticModel')
DEPOYMENT_JOB = StagingEnvironments.get_prod_environment()
PROD_DATASOURCE_PATH = DEPOYMENT_JOB.parameters[DEPOYMENT_JOB.web_datasource_path_parameter]

DeploymentManager.update_imported_semantic_model_source(
    DEV_WORKSPACE_NAME, 
    SEMANTIC_MODEL_NAME,
    PROD_DATASOURCE_PATH)

FabricRestApi.create_and_bind_semantic_model_connecton(PROD_WORKSPACE, PROD_SEMANTIC_MODEL['id'])



# def complete_step1():
#     """Complete Step 1"""

#     DeploymentManager.update_variable_library(
#         TEST_WORKSPACE_NAME,
#         VARIABLE_LIBRARY_NAME,
#         StagingEnvironments.get_test_environment())

#     DeploymentManager.run_data_pipeline(TEST_WORKSPACE_NAME, DATA_PIPELINE_NAME)

#     DeploymentManager.get_sql_endpoint_info_by_name(TEST_WORKSPACE_NAME, LAKEHOUSE_NAME)

# def complete_step2():
#     """Complete Step 2"""

#     DeploymentManager.deploy_from_dev_to_test(DEPLOYMENT_PIPELINE_NAME)
#     DeploymentManager.create_and_bind_model_connection(TEST_WORKSPACE_NAME)

#     DeploymentManager.deploy_from_test_to_prod(DEPLOYMENT_PIPELINE_NAME)

#     DeploymentManager.update_variable_library(
#         PROD_WORKSPACE_NAME,
#         VARIABLE_LIBRARY_NAME,
#         StagingEnvironments.get_prod_environment())

#     DeploymentManager.run_data_pipeline(
#         PROD_WORKSPACE_NAME,
#         DATA_PIPELINE_NAME)
    
#     DeploymentManager.get_sql_endpoint_info_by_name(PROD_WORKSPACE_NAME, LAKEHOUSE_NAME)

# def complete_step3():
#     """Complete Step 3"""
#     DeploymentManager.deploy_from_test_to_prod(DEPLOYMENT_PIPELINE_NAME)
#     DeploymentManager.create_and_bind_model_connection(PROD_WORKSPACE_NAME)



# match os.getenv("SOLUTION_NAME"):

#     case 'Custom Power BI Solution':
#         deploy_powerbi_solution()

#     case 'Custom Notebook Solution':
#         deploy_notebook_solution()

#     case 'Custom Shortcut Solution':
#         deploy_shortcut_solution()

#     case 'Custom Data Pipeline Solution':
#         deploy_data_pipeline_solution()

#     case 'Custom Variable Library Solution':
#         deploy_variable_library_solution()

#     case 'Custom Warehouse Solution':
#         deploy_warehouse_solution()

#     case 'Custom Realtime Solution':
#         deploy_realtime_solution()

#     case 'Deploy All Solutions':
#         deploy_powerbi_solution()
#         deploy_notebook_solution()
#         deploy_shortcut_solution()
#         deploy_data_pipeline_solution()
#         deploy_warehouse_solution()
#         deploy_realtime_solution()
#         #deploy_variable_library_solution()




# complete_step1()

# # BY Hand - set deployment rule for semantic model to lakehouse and then deploy to stage

# # complete_step2()

# # BY Hand - set deployment rule for semantic model to lakehouse and then deploy to stage

# complete_step3()
