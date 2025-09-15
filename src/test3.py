"""Test3"""
import json
from fabric_devops import DeploymentManager, FabricRestApi, EnvironmentSettings, AdoProjectManager

WORKSPACE1_NAME = "Apollo-dev"
workspace1 = FabricRestApi.get_workspace_by_name(WORKSPACE1_NAME)


git_status = FabricRestApi.get_git_status(workspace1['id'])

update_from_git_request = {
        "workspaceHead": git_status['workspaceHead'],
        "remoteCommitHash": git_status['remoteCommitHash'],
                "conflictResolution": {
                    "conflictResolutionType": "Workspace",
                    "conflictResolutionPolicy": "PreferWorkspace"
                },
                "options": {
                    "allowOverrideItems": True
                }
                            
            }

FabricRestApi.update_workspace_from_git(workspace1['id'], update_from_git_request)
