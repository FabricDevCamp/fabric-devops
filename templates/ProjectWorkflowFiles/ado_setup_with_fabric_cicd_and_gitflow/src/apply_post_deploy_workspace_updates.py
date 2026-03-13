import os

from fabric_devops_utils import EnvironmentSettings, DeploymentManager, FabricRestApi, AppLogger

AppLogger.log_job("Apply Post-Deploy Workspace Updates")

branch_name = os.environ.get('BUILD_SOURCEBRANCH').replace('refs/heads/', '')

AppLogger.log_step(f'Pipeline triggered by Pull request completing on branch [{branch_name}]')

match branch_name:
    
    case 'dev':
        AppLogger.log_error("Ouch, pipeline should not execute on dev branch")
    
    case 'test':
        workspace_id = EnvironmentSettings.WORKSPACE_ID_TEST
        DeploymentManager.apply_post_deploy_fixes(workspace_id)
        AppLogger.log_job_complete(workspace_id)

    case 'main':
        workspace_id = EnvironmentSettings.WORKSPACE_ID_PROD
        DeploymentManager.apply_post_deploy_fixes(workspace_id)
        AppLogger.log_job_complete(workspace_id)

    case _:
        AppLogger.log_error("Ouch, unknown branch name")
