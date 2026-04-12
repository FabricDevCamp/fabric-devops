"""Synch Dev Workspace by updating from GIT and applying any post-sync fixes"""

import os

from fabric_devops_utils import EnvironmentSettings, DeploymentManager, FabricRestApi, AppLogger

AppLogger.log_job(
    "Synching workspace by updating from GIT and applying any post-sync fixes..."
)

branch_name =  os.environ.get('BRANCH_NAME')

AppLogger.log_step(f'Pipeline triggered by pull request completion on branch [{branch_name}]')

match branch_name:
    
    case 'main':
        workspace_id = EnvironmentSettings.WORKSPACE_ID_DEV
        FabricRestApi.update_workspace_from_git(workspace_id)
        deployment_job = EnvironmentSettings.DEPLOYMENT_JOBS['dev']
        DeploymentManager.apply_post_sync_fixes(workspace_id, deployment_job)
        AppLogger.log_job_complete(workspace_id)

    case _:
        AppLogger.log_error("Ouch, unknown branch name")
