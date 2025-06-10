"""Setup GIT Connection"""

from fabric_devops import DeploymentManager

WORKSPACE = WORKSPACE = DeploymentManager.deploy_notebook_solution(
            'Custom Notebook Solution')

DeploymentManager.connect_workspace_to_github_repo(WORKSPACE)