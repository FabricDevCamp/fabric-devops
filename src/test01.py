from azure.identity import DefaultAzureCredential
from microsoft_fabric_api import FabricClient

# Create credential and client
credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
fabric_client = FabricClient(credential)

# Get the list of workspaces using the client
workspaces = [workspace for workspace in fabric_client.core.workspaces.list_workspaces()]
print(f"Number of workspaces: {len(workspaces)}")
for workspace in workspaces:
    print(f"Workspace: {workspace.display_name}, Capacity ID: {workspace.capacity_id}")