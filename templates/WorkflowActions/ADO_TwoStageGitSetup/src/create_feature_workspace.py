"""Display Workspace Info"""
import os
from fabric_devops_utils import EnvironmentSettings, AppLogger, FabricRestApi, AdoProjectManager, DeploymentManager

AppLogger.log_job("Creating feature branch")

dev_workspace = FabricRestApi.get_workspace_info(EnvironmentSettings.DEV_WORKSPACE_ID)
DEV_WORKSPACE_NAME = dev_workspace['displayName']
PROJECT_NAME = DEV_WORKSPACE_NAME.replace('-dev', '')

FEATURE_NAME = os.getenv("FEATURE_NAME")
FEATURE_BRANCH_NAME = f'dev-{FEATURE_NAME}'
FEATURE_BRANCH = AdoProjectManager.create_branch(PROJECT_NAME, FEATURE_BRANCH_NAME, 'dev')

FEATURE_WORKSPACE_NAME = F'{DEV_WORKSPACE_NAME}-{FEATURE_NAME}'
FEATURE_WORKSPACE = FabricRestApi.create_workspace(FEATURE_WORKSPACE_NAME)
FabricRestApi.connect_workspace_to_ado_repo(FEATURE_WORKSPACE, PROJECT_NAME, FEATURE_BRANCH_NAME)
deployment_job = EnvironmentSettings.DEPLOYMENT_JOBS['dev']
DeploymentManager.apply_post_sync_fixes(FEATURE_WORKSPACE['id'], deployment_job)

FabricRestApi.commit_workspace_to_git(
    FEATURE_WORKSPACE['id'],
    commit_comment = 'Sync updates from feature workspace to repo after applying fixes')

AppLogger.log_job_complete(FEATURE_WORKSPACE['id'])

AppLogger.log_step("Env Vars")
for var in os.environ.items():
    AppLogger.log_substep(f'{var[0]}={var[1]}')
