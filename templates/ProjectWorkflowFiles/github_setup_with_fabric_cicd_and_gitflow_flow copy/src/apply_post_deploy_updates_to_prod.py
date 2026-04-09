"""Apply post deploy updates to prod workspace"""

from fabric_devops_utils import EnvironmentSettings, DeploymentManager, AppLogger

AppLogger.log_job("Apply Post-Deploy Workspace Updates to [prod] workspace")
workspace_id = EnvironmentSettings.WORKSPACE_ID_PROD
DeploymentManager.apply_post_deploy_fixes(workspace_id)
AppLogger.log_job_complete(workspace_id)
