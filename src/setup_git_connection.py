"""Setup GIT Connection"""

import os

from fabric_devops import DeploymentManager

os.system('cls')

WORKSPACE_NAME = 'Crimea'

DeploymentManager.setup_workspace_with_github_repo(WORKSPACE_NAME)

print('')
