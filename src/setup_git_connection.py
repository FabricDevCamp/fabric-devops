"""Setup GIT Connection"""

import os

from fabric_devops import DeploymentManager

os.system('cls')

WORKSPACE_NAME = 'Bianca'

DeploymentManager.setup_workspace_with_github_repo(WORKSPACE_NAME)

# AppLogger.log_job_ended()
