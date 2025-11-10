"""Synch Workspace with GIT REPO"""

import os

from fabric_devops_utils import EnvironmentSettings, DeploymentManager, FabricRestApi, AppLogger

AppLogger.log_job("Synching workspace after PR completion")

branch_name = os.environ.get('BUILD_SOURCEBRANCH').replace('refs/heads/', '')

AppLogger.log_step(f'Pipeline triggered by PR completing on branch [{branch_name}]')

match branch_name:
    
    case 'dev':
        workspace_id = EnvironmentSettings.DEV_WORKSPACE_ID
        FabricRestApi.update_workspace_from_git(workspace_id)
        deployment_job = EnvironmentSettings.DEPLOYMENT_JOBS['dev']
        DeploymentManager.apply_post_sync_fixes(workspace_id, deployment_job)
        FabricRestApi.commit_workspace_to_git(workspace_id, 'Sync Dev Workspace Items to GIT')
        AppLogger.log_job_complete(workspace_id)

    case _:
        AppLogger.log_error("Ouch, unknown branch name")
