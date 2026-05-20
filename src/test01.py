"""Deploy solution to new workspace"""

from fabric_devops import GitHubRestApi,  AppLogger

PROJECT_NAME = 'maxwell'
project = GitHubRestApi.create_repository(PROJECT_NAME)

