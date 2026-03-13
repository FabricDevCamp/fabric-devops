"""demo"""

import json
from fabric_devops import DeploymentManager, AppLogger

SOLUTION_NAME = 'Medallion Solution'
WORKSPACE_NAME = 'Daddio'
workspace = DeploymentManager.deploy_solution_by_name(
    SOLUTION_NAME,
    WORKSPACE_NAME,
    deploy_using_fabric_cicd=False
)

AppLogger.log_job_complete(workspace.id)