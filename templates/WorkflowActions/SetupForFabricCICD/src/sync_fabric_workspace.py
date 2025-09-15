
import json
import os

from azure.identity import AzureCliCredential, ClientSecretCredential

from fabric_cicd import FabricWorkspace

client_id = os.getenv("FABRIC_CLIENT_ID")
client_secret = os.getenv("FABRIC_CLIENT_SECRET")
tenant_id = os.getenv("FABRIC_TENANT_ID")
token_credential = \
    ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

github_workpace = os.getenv('GITHUB_WORKSPACE')
branch = os.getenv('BRANCH_NAME')

config_file = github_workpace +  '/workspace/workspace.config.json'

if os.path.exists(config_file) is False:
    print(f"'{config_file}' does not exists.")
else:
    with open(config_file, 'r', encoding='utf-8') as file:
        config = json.load(file)

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

    # Get commit heads from workspace and remote branch
    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/get-status
    git_status_url = f"{target_workspace.base_api_url}/git/status"
    git_status = target_workspace.endpoint.invoke(method="GET", url=git_status_url)
    workspace_head = git_status["body"]["workspaceHead"]
    remote_head = os.getenv("BUILD_SOURCEVERSION")

    # Force update to sync workspace with remote branch
    # https://learn.microsoft.com/en-us/rest/api/fabric/core/git/update-from-git
    git_update_url = f"{target_workspace.base_api_url}/git/updateFromGit"
    git_update_body = {
        "workspaceHead": f"{workspace_head}",
        "remoteCommitHash": f"{remote_head}",
        "conflictResolution": {
            "conflictResolutionType": "Workspace",
            "conflictResolutionPolicy": "PreferRemote",
        },
        "options": {"allowOverrideItems": True},
    }
    target_workspace.endpoint.invoke(method="POST", url=git_update_url, body=git_update_body, max_retries=10)
