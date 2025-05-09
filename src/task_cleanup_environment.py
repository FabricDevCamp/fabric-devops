"""Cleanup Environment"""

from fabric_devops import DeploymentManager, GitHubRestApi, AppLogger

AppLogger.log_step('Deleting Demo Repos')
repos = GitHubRestApi.get_github_repositories()
for repo in repos:
    AppLogger.log_substep(f"Deleting {repo['name']} [{repo['id']}]")
    GitHubRestApi.delete_github_repository(repo['name'])

DeploymentManager.cleanup_dev_environment()
