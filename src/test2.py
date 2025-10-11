import json
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi

EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False

WORKSPACE_NAME = "NFL Agent"

lakehouse_name = "nfl_data"

model_name = "NFL Stats"

workspace = FabricRestApi.get_workspace_by_name(WORKSPACE_NAME)

lakehouse = FabricRestApi.get_item_by_name(workspace['id'], lakehouse_name, 'Lakehouse')
onelake_path = FabricRestApi.get_onelake_path_for_lakehouse(workspace['id'], lakehouse)


model = FabricRestApi.get_item_by_name(workspace['id'], model_name, 'SemanticModel')
sources = FabricRestApi.list_datasources_for_semantic_model(workspace['id'], model['id'])

print( json.dumps(sources, indent=4) )



# agent = FabricRestApi.create_item(workspace['id'], create_agent_request)

# create_model_request = \
#     ItemDefinitionFactory.get_create_item_request_from_folder(
#         'NFL Stats.SemanticModel')

# model_redirects = {
#     '{WORKSPACE_ID}': workspace['id'],
#     '{LAKRHOUSE_ID}': lakehouse['id']
# }

# create_model_request = \
#     ItemDefinitionFactory.update_part_in_create_request(
#         create_model_request,
#         'definition/expressions.tmdl',
#         model_redirects)

# model = FabricRestApi.create_item(workspace['id'], create_model_request)

# FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

# report_folder  = 'NFL Stats.Report'

# create_report_request = \
#     ItemDefinitionFactory.get_create_report_request_from_folder(
#         report_folder,
#         model['id'])

# FabricRestApi.create_item(workspace['id'], create_report_request)

#     # create_agent_request = \
# #     ItemDefinitionFactory.get_create_item_request_from_folder(
# #         'Sales Agent.DataAgent')

# # agent_redirects = {
# #     # '{WORKSPACE_ID}': workspace['id'],
# #     # '{SEMANTIC_MODEL_ID}': model['id'],
# #     # '{SEMANTIC_MODEL_NAME}': model['displayName'],            
# # }

# # create_agent_request = \
# #     ItemDefinitionFactory.update_part_in_create_request(
# #         create_agent_request,
# #         'Files/Config/published/semantic-model-Product Sales DirectLake Model/datasource.json',
# #         agent_redirects)

# # create_agent_request = \
# #     ItemDefinitionFactory.update_part_in_create_request(
# #         create_agent_request,
# #         'Files/Config/draft/semantic-model-Product Sales DirectLake Model/datasource.json',
# #         agent_redirects)

# return workspace

