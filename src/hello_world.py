    """Hello World for Fabric REST API Pythn SDK"""

    import os
    from azure.identity import ClientSecretCredential
    from microsoft_fabric_api import FabricClient

    credential = ClientSecretCredential(
        tenant_id = os.getenv("AZURE_TENANT_ID"),
        client_id = os.getenv("AZURE_CLIENT_ID"),
        client_secret = os.getenv("AZURE_CLIENT_SECRET")
    )

    fabric_client = FabricClient(credential)

    # create request body for creating new workspace
    create_workspace_request = {
        "displayName": "Hello Python SDK",
        "description": "This isn't so hard",
        "capacityId": os.getenv("FABRIC_CAPACITY_ID")
    }

    # call Create Workspace API
    workspace = fabric_client.core.workspaces.create_workspace(create_workspace_request)

    # enumerate workspaces by calling List Workspaces API
    workspaces = fabric_client.core.workspaces.list_workspaces()
    for workspace in workspaces:
        print(f'{workspace.display_name} - [{workspace.id}]')
