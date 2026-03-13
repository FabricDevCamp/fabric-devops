"""Setup project with fabric-cicd and release flow """
import os

from fabric_devops import DeploymentManager, AppLogger

PROJECT_NAME = os.getenv("PROJECT_NAME")
SOLUTION_NAME = os.getenv("SOLUTION_NAME")
GIT_INTEGRATION_PROVIDER = os.getenv("GIT_INTEGRATION_PROVIDER")
CREATE_FEATURE_WORKSPACE = os.getenv("CREATE_FEATURE_WORKSPACE") == 'true'

match GIT_INTEGRATION_PROVIDER:

    case 'Azure DevOps':
    
        DeploymentManager.setup_ado_repo_with_fabric_cicd_and_release_flow(
            PROJECT_NAME,
            SOLUTION_NAME,
            CREATE_FEATURE_WORKSPACE
        )
        
    case 'GitHub':
    
        DeploymentManager.setup_github_repo_with_fabric_cicd_and_release_flow(
            PROJECT_NAME,
            SOLUTION_NAME,
            CREATE_FEATURE_WORKSPACE            
        )

AppLogger.log_job_complete()