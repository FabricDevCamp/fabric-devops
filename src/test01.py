"""Deploy solution to new workspace"""

from fabric_devops import DeploymentManager, StagingEnvironments

WORKSPACE_NAME = "Project2A"
SOLUTION_NAME = 'Notebook Solution'
DEPLOY_USING_FABRIC_CICD = False
deploy_job = StagingEnvironments.get_dev_environment()

workspace = DeploymentManager.deploy_solution_by_name(
    SOLUTION_NAME,
    WORKSPACE_NAME,
    deploy_job,
    DEPLOY_USING_FABRIC_CICD)
