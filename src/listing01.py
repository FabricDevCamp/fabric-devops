"""Hello world for Fabric REST API Python SDK"""

import os
from azure.identity import ClientSecretCredential
from microsoft_fabric_api import FabricClient

credential = ClientSecretCredential(
    os.getenv('AZURE_TENANT_ID'),
    os.getenv('AZURE_CLIENT_ID'),
    os.getenv('AZURE_CLIENT_SECRET')
)
    
fabric_client = FabricClient(credential)
    
# Call Create Workspace API
create_workspace_request = {
    "display_name": 'Hello Python SDK',
    "description":  'Look ma, I can create a workspace using code!',
    "capacity_id":  os.getenv('FABRIC_CAPACITY_ID')
}

fabric_client.core.workspaces.create_workspace(create_workspace_request)

# Call List Workspace API
workspaces = fabric_client.core.workspaces.list_workspaces()
for workspace in workspaces:
    print(f"{workspace.display_name} - [{workspace.id}]")
