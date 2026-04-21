"""Deploy solution to new workspace"""

from fabric_devops import DeploymentManager

DeploymentManager.setup_ado_repo_for_terraform("Bob")
