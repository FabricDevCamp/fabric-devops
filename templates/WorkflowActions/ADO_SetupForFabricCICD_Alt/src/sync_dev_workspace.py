"""Synch Workspace with GIT REPO"""

import os

from fabric_devops_utils import EnvironmentSettings, DeploymentManager, FabricRestApi, AppLogger

AppLogger.log_job("Synching workspace after PR completion")

AppLogger.log_step('Pipeline triggered by PR completing on branch main')
AppLogger.log_step('Syncing changes to DEV workspace')

workspace_id = EnvironmentSettings.DEV_WORKSPACE_ID
FabricRestApi.update_workspace_from_git(workspace_id)
deployment_job = EnvironmentSettings.DEPLOYMENT_JOBS['dev']
DeploymentManager.apply_post_sync_fixes(workspace_id, deployment_job)
FabricRestApi.commit_workspace_to_git(
    workspace_id, 
    commit_comment='Sync Dev Workspace Items to GIT')

AppLogger.log_job_complete(workspace_id)
