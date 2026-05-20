"""Setup project with fabric-cicd and release flow """
import os

from fabric_devops import DeploymentManager, AppLogger

PROJECT_NAME = 'Customer-Sales'
SOLUTION_NAME = 'Power BI Solution'
CREATE_FEATURE_WORKSPACE = False
    
DeploymentManager.setup_github_repo_with_fabric_cicd_and_github_flow(
    PROJECT_NAME,
    SOLUTION_NAME,
    CREATE_FEATURE_WORKSPACE
        )

AppLogger.log_job_complete()
