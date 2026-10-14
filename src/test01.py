"""Setup project with fabric-cicd and release flow """
import os

from fabric_devops import DeploymentManager, AppLogger

PROJECT_NAME = 'Bixby'
SOLUTION_NAME = 'Power BI Solution'
GIT_INTEGRATION_PROVIDER = 'GitHub'
CREATE_FEATURE_WORKSPACE = False

match GIT_INTEGRATION_PROVIDER:

    case 'Azure DevOps':
        DeploymentManager.setup_ado_repo_with_fabric_cicd_and_github_flow(
            PROJECT_NAME,
            SOLUTION_NAME,
            CREATE_FEATURE_WORKSPACE
        )
        
    case 'GitHub':
    
        DeploymentManager.setup_github_repo_with_fabric_cicd_and_github_flow(
            PROJECT_NAME,
            SOLUTION_NAME,
            CREATE_FEATURE_WORKSPACE
        )

AppLogger.log_job_complete()
