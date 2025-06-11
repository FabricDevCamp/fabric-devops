"""Setup Deployment Pipelines"""

import os

from fabric_devops import DeploymentManager, StagingEnvironments

PROJECT_NAME = os.getenv("PROJECT_NAME")
SOLUTION_NAME = os.getenv("SOLUTION_NAME")

DeploymentManager.delete_deployment_pipeline_by_name(PROJECT_NAME)

DeploymentManager.setup_deployment_pipeline(PROJECT_NAME, SOLUTION_NAME)

# DeploymentManager.deploy_from_dev_to_test(PROJECT_NAME)


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
