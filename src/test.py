"""Setup Deployment Pipelines"""

from fabric_devops import GitHubRestApi

GitHubRestApi.create_repository('Test69', add_secrets=True)