"""Publish All Items"""

import os
import json

from azure.identity import ClientSecretCredential

from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

client_id = os.getenv("FABRIC_CLIENT_ID")
client_secret = os.getenv("FABRIC_CLIENT_SECRET")
tenant_id = os.getenv("FABRIC_TENANT_ID")
token_credential = \
    ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

github_workpace = os.getenv('GITHUB_WORKSPACE')
branch = os.getenv('BRANCH_NAME')
print(github_workpace, flush=True)

config_file = github_workpace +  '/workspace/workspace.config.json'


if os.path.exists(config_file) is False:
    print(f"'{config_file}' does not exists.")
else:        
    print(config_file, flush=True)

    with open(config_file, 'r', encoding='utf-8') as file:
        config = json.load(file)
        print(config, flush=True)

    workspace_config = config[branch] 

    # Sample values for FabricWorkspace parameters
    workspace_id = workspace_config['workspace_id']
    environment = workspace_config['environment']
    
    repository_directory = "workspace"
    item_type_in_scope = [ "Lakehouse", "Notebook", "SemanticModel", "Report"]

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        token_credential=token_credential,
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)

    # Unpublish all items defined in item_type_in_scope not found in repository
    # this fails when trying to delete default semantic model for lakehouse
    # unpublish_all_orphan_items(target_workspace)
