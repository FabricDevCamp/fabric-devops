"""Setup Deployment Pipelines"""

from fabric_devops import GitHubRestApi, DeploymentManager

param = DeploymentManager.generate_parameter_yml_file('Apollo-dev', 'Apollo-test', 'TEST')

print(param)