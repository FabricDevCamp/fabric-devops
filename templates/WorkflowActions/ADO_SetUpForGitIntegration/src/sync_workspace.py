"""Display Workspace Info"""

from fabric_devops_utils import EnvironmentSettings, DeploymentManager, FabricRestApi


workspace_id = EnvironmentSettings.WORKSPACE_ID

git_status = FabricRestApi.get_git_status(workspace_id)

update_from_git_request = {
    "workspaceHead": git_status['workspaceHead'],
    "remoteCommitHash": git_status['remoteCommitHash'],
    "conflictResolution": {
        "conflictResolutionType": "Workspace",
        "conflictResolutionPolicy": "PreferRemote"
    },
    "options": { "allowOverrideItems": True }                
}

FabricRestApi.update_workspace_from_git(workspace_id, update_from_git_request)

DeploymentManager.apply_post_sync_fixes(workspace_id)
