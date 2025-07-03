"""Setup Deployment Pipelines"""
import json
import os

from fabric_devops import FabricRestApi
def generate_parameter_yml_file(source_workspace_name, target_workspace_name, environment_name):
    """Generate parameter.yml file"""

    target_workspace = FabricRestApi.get_workspace_by_name(target_workspace_name)
    target_workspace_items = FabricRestApi.list_workspace_items(target_workspace['id'])
    target_items = {}
    for target_item in target_workspace_items:
        item_name = target_item['displayName'] + "." + target_item['type']
        target_items[item_name] = target_item['id']
 
    source_workspace = FabricRestApi.get_workspace_by_name(source_workspace_name)
    source_workspace_items  = FabricRestApi.list_workspace_items(source_workspace['id'])

    tab = (' ' * 4)
    file_content = 'find_replace:\n'

    file_content += tab + '"' + source_workspace['id'] + f'": #  source workspace Id - [{source_workspace["displayName"]}]\n'
    file_content += tab + tab + f'{environment_name}: "{target_workspace["id"]}"  #  target workspace Id - [{target_workspace["displayName"]}]\n\n'

    for workspace_item in source_workspace_items:
        item_name = workspace_item['displayName'] + "." + workspace_item['type']
        file_content += tab + '"' + workspace_item['id'] + f'": # [{item_name}] in [{source_workspace["displayName"]}]\n'
        if item_name in target_items:
            file_content += tab + tab + f'{environment_name}: "{target_items[item_name]}" # [{item_name}] in [{target_workspace["displayName"]}]\n\n'

    return file_content

DEPLOYMENT_PIPELINE_NAME = 'Apollo'
DEV_WORKSPACE_NAME = f"{DEPLOYMENT_PIPELINE_NAME}-dev"
TEST_WORKSPACE_NAME = f"{DEPLOYMENT_PIPELINE_NAME}-test"
PROD_WORKSPACE_NAME = f"{DEPLOYMENT_PIPELINE_NAME}"



parameter_file_name = 'parameter.yml'

parameter_file_content = generate_parameter_yml_file(
    TEST_WORKSPACE_NAME,
    PROD_WORKSPACE_NAME,
    "PROD"
)

folder_path = ".//templates//Exports/"
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

full_path = folder_path + parameter_file_name
os.makedirs(os.path.dirname(full_path), exist_ok=True)
with open(full_path, 'w', encoding='utf-8') as file:
    file.write(parameter_file_content)
