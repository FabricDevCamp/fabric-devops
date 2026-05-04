"""Deploy solution to new workspace"""

from fabric_devops import DeploymentManager,  AppLogger

PROJECT_NAME = 'Charo'

workspace = DeploymentManager.deploy_two_workspace_solution_using_apis(PROJECT_NAME)

AppLogger.log_job_complete(workspace.id)
