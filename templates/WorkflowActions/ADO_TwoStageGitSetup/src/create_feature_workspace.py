"""Display Workspace Info"""
import os
from fabric_devops_utils import EnvironmentSettings, AppLogger, FabricRestApi, AdoProjectManager, DeploymentManager

AppLogger.log_job("Creating feature branch")

dev_workspace = FabricRestApi.get_workspace_info(EnvironmentSettings.DEV_WORKSPACE_ID)
DEV_WORKSPACE_NAME = dev_workspace['displayName']
PROJECT_NAME = EnvironmentSettings.ADO_PROJECT_NAME

RUN_POST_DEPLOY_FIXES = os.getenv("RUN_POST_DEPLOY_FIXES") == 'true'
ADD_ADMIN_USER = os.getenv("ADD_ADMIN_USER") == 'true'

FEATURE_NAME = os.getenv("FEATURE_NAME")
FEATURE_BRANCH_NAME = f'dev-{FEATURE_NAME}'
FEATURE_BRANCH = AdoProjectManager.create_branch(PROJECT_NAME, FEATURE_BRANCH_NAME, 'dev')

FEATURE_WORKSPACE_NAME = F'{DEV_WORKSPACE_NAME}-{FEATURE_NAME}'
FEATURE_WORKSPACE = FabricRestApi.create_workspace(FEATURE_WORKSPACE_NAME)
    
AppLogger.log_substep('Adding workspace role of [Member] for developers group')
FabricRestApi.add_workspace_group(
    FEATURE_WORKSPACE['id'], 
    EnvironmentSettings.DEVELOPERS_GROUP_ID, 
    'Member')

if ADD_ADMIN_USER:
    AppLogger.log_substep('Adding workspace role of [Admin] for admin user')
    FabricRestApi.add_workspace_user(
        FEATURE_WORKSPACE['id'], 
        EnvironmentSettings.ADMIN_USER_ID, 
        'Admin')
    
FabricRestApi.connect_workspace_to_ado_repo(FEATURE_WORKSPACE, PROJECT_NAME, FEATURE_BRANCH_NAME)

if RUN_POST_DEPLOY_FIXES:
    deployment_job = EnvironmentSettings.DEPLOYMENT_JOBS['dev']
    DeploymentManager.apply_post_sync_fixes(FEATURE_WORKSPACE['id'], deployment_job)

    FabricRestApi.commit_workspace_to_git(
        FEATURE_WORKSPACE['id'],
        commit_comment = 'Sync updates from feature workspace to repo after applying fixes')

AppLogger.log_job_complete(FEATURE_WORKSPACE['id'])
