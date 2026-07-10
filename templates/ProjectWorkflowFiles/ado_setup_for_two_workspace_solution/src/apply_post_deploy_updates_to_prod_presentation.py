"""Apply post deploy updates to prod workspace"""

from fabric_devops_utils import EnvironmentSettings, DeploymentManager, AppLogger

AppLogger.log_job("Apply Post-Deploy Workspace Updates to [prod] workspace")
workspace_id = EnvironmentSettings.WORKSPACE_ID_PROD_PRESENTATION
DeploymentManager.apply_post_deploy_fixes_to_presentation_workspace(workspace_id)
AppLogger.log_job_complete(workspace_id)
