"""Deploy solution to new workspace"""

import os
from fabric_devops import DeploymentManager

os.system('cls')

PROJECT_NAME = 'Apollo'
DeploymentManager.setup_ado_repo_with_two_workspace_solution(PROJECT_NAME)

