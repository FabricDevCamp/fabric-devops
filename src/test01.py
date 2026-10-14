"""Setup project with deployment pipeline"""
import os

from fabric_devops import DeploymentManager, AppLogger

PROJECT_NAME = 'Dionysus' # os.getenv("PROJECT_NAME")
SOLUTION_NAME = 'Power BI Solution' # os.getenv("SOLUTION_NAME")

DeploymentManager.setup_github_repo_with_deployment_pipeline(
    PROJECT_NAME,
    SOLUTION_NAME,
    False
)

AppLogger.log_job_complete()
