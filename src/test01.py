"""Deploy solution to new workspace"""
import os

from fabric_devops import DeploymentManager, AppLogger

PROJECT_NAME = 'Bindie'
DeploymentManager.deploy_notebook_solution_using_apis(PROJECT_NAME)

AppLogger.log_job_complete()
