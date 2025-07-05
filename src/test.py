"""Setup Deployment Pipelines"""

from fabric_devops import GitHubRestApi, DeploymentManager

param = DeploymentManager.generate_workspace_config_file('Apollo-test', 'Apollo')

print(param)