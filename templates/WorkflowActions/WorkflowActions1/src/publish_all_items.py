"""Publish All Items"""

import os

from azure.identity import ClientSecretCredential

from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

client_id = os.getenv("FABRIC_CLIENT_ID")
client_secret = os.getenv("FABRIC_CLIENT_SECRET")
tenant_id = os.getenv("FABRIC_TENANT_ID")
token_credential = \
    ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

# Sample values for FabricWorkspace parameters
workspace_id = "f54cd087-3253-4529-81eb-dfc4cfd445c8"
environment = "TEST"
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
unpublish_all_orphan_items(target_workspace)
