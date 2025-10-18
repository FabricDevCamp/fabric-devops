"""Synch Workspace with GIT REPO"""

import os
from fabric_devops_utils import EnvironmentSettings, DeploymentManager, FabricRestApi, AppLogger

AppLogger.log_step("Synching workspace after PR completion")

branch_name = os.environ.get('BUILD_SOURCEBRANCH').replace('refs/heads/', '')

AppLogger.log_substep(f'Pipeline trigger by branch [{branch_name}]')

match branch_name:
    
    case 'dev':
        workspace_id = EnvironmentSettings.DEV_WORKSPACE_ID
        FabricRestApi.update_workspace_from_git(workspace_id)
        DeploymentManager.apply_post_sync_fixes(workspace_id)

    case 'main':
        workspace_id = EnvironmentSettings.PROD_WORKSPACE_ID
        FabricRestApi.update_workspace_from_git(workspace_id)
        DeploymentManager.apply_post_sync_fixes(workspace_id)

    case _:
        AppLogger.log_error("Ouch, unknown branch name")

