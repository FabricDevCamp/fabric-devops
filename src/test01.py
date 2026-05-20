"""Deploy solution to new workspace"""

from fabric_devops import GitHubRestApi,  AppLogger

PROJECT_NAME = 'max'
project = GitHubRestApi.create_repository(PROJECT_NAME)

