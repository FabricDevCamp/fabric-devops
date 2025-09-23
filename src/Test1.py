"""Test1"""
import json
from fabric_devops import DeploymentManager, AppLogger, FabricRestApi


WORKSPACE_NAME = "Puff"

workspace = DeploymentManager.deploy_udf_solution(WORKSPACE_NAME)

AppLogger.log_job_complete( workspace['id'] )





# udf = FabricRestApi.get_item_by_name(workspace['id'], 'hello_fabric', 'UserDataFunction')

# response = FabricRestApi.call_user_defined_function(
#     workspace['id'],
#     udf['id'],
#     'hello_fabric',
#     {
#         "name": "TeddyP"
#     }
# )

# AppLogger.log_job_complete( json.dumps(response) )

