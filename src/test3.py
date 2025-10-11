"""Test3"""
import json
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi, ItemDefinitionFactory


workspace_name = "Angela"
lakehouse_name = "nfl_data"
semantic_model_name = "NFL Stats"

workspace = FabricRestApi.get_workspace_by_name(workspace_name)
# lakehouse = FabricRestApi.get_item_by_name(workspace['id'], lakehouse_name, 'Lakehouse')
model = FabricRestApi.get_item_by_name(workspace['id'], semantic_model_name, 'SemanticModel')

# onelake_path = FabricRestApi.get_onelake_path_for_lakehouse(workspace['id'], lakehouse)


create_agent_request = \
    ItemDefinitionFactory.get_create_item_request_from_folder(
        'NFL Agent 2.DataAgent')

agent_redirects = {
    '{WORKSPACE_ID}': workspace['id'],
    '{SEMANTIC_MODEL_ID}': model['id']
}

create_agent_request = \
    ItemDefinitionFactory.update_part_in_create_request(
        create_agent_request,
        'Files/Config/published/semantic-model-NFL Stats/datasource.json',
        agent_redirects)

create_agent_request = \
    ItemDefinitionFactory.update_part_in_create_request(
        create_agent_request,
        'Files/Config/draft/semantic-model-NFL Stats/datasource.json',
        agent_redirects)


agent = FabricRestApi.create_item(workspace['id'], create_agent_request)
