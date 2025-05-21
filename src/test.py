"""Deploy Power BI Solution"""

from fabric_devops import FabricRestApi, AppLogger

AppLogger.log_job("Patch Test")

workspace_name = "Custom Realtime Solution"
eventhouse_name = "DemoEventhouse"
semantic_model_name = 'Semantic Model on KQL Database'

workspace = FabricRestApi.get_workspace_by_name(workspace_name)

eventhouse_item = FabricRestApi.get_item_by_name(workspace['id'], eventhouse_name, 'Eventhouse')
eventhouse = FabricRestApi.get_eventhouse(workspace['id'], eventhouse_item['id'])
query_service_uri = eventhouse['properties']['queryServiceUri']
 

model = FabricRestApi.get_item_by_name(workspace['id'], semantic_model_name, 'SemanticModel')

FabricRestApi.patch_oauth_connection_to_kqldb(workspace, model, query_service_uri)
