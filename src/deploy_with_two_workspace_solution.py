"""Deploy solution to new workspace"""
import os

from fabric_devops import DeploymentManager, AppLogger

PROJECT_NAME = os.getenv("PROJECT_NAME")
DeploymentManager.setup_ado_repo_with_two_workspace_solution(PROJECT_NAME)

AppLogger.log_job_complete()
