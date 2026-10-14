"""Create GitHub repository"""
import os

from fabric_devops import AppLogger, GitHubRestApi 

repos = GitHubRestApi.get_github_repositories()
for repo in repos:
    print(repo)


REPO_NAME = 'chuck-norris' # os.getenv("REPO_NAME")
GitHubRestApi.create_repository(REPO_NAME)
