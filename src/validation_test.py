'''Validation tests'''

from fabric_devops import DeploymentManager, StagingEnvironments, AppLogger

SOLUTION_NAMES = [
    'Power BI Solution',
    'Notebook Solution',
    'Shortcut Solution',
    'Pipeline Solution',
    'Medallion Solution'
]

for SOLUTION_NAME in SOLUTION_NAMES:

    WORKSPACE_NAME = f'Demo {SOLUTION_NAME}'
    workspace = DeploymentManager.deploy_solution_by_name(
        SOLUTION_NAME,
        WORKSPACE_NAME,
        StagingEnvironments.get_prod_environment(),
        deploy_using_fabric_cicd=True
    )
    AppLogger.log_job_complete(workspace.id)

    WORKSPACE_NAME = f'Demo {SOLUTION_NAME} API deploy'
    workspace = DeploymentManager.deploy_solution_by_name(
        SOLUTION_NAME,
        WORKSPACE_NAME,
        StagingEnvironments.get_prod_environment(),
        deploy_using_fabric_cicd=False
    )
    AppLogger.log_job_complete(workspace.id)
