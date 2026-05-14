"""Deploy solution to new workspace"""

from fabric_devops import DeploymentManager,  AppLogger

PROJECT_NAME = 'Apollo'
SOLUTION_NAME = 'Medallion Solution'
workspace = DeploymentManager.deploy_two_workspace_solution_using_apis(PROJECT_NAME)

# workspace = DeploymentManager.deploy_solution_by_name(SOLUTION_NAME, PROJECT_NAME)

AppLogger.log_job_complete(workspace['presentation'].id)
