"""Deploy Demo Solution with ADO GIT Intergation"""

from fabric_devops import DeploymentManager, AppLogger, StagingEnvironments, \
                          AdoProjectManager, FabricRestApi

SOLUTION_NAME = 'Custom Data Pipeline Solution with Variable Library'
WORKSPACE_NAME = 'Product Sales'

deploy_job = StagingEnvironments.get_dev_environment()

workspace = DeploymentManager.deploy_solution_by_name(SOLUTION_NAME, WORKSPACE_NAME, deploy_job)

# create ADO project and connect project main repo to workspace
AdoProjectManager.create_project(WORKSPACE_NAME)
FabricRestApi.connect_workspace_to_ado_repo(workspace, WORKSPACE_NAME)

# create feature1 workspace
FEATURE1_NAME = 'feature1'
FEATURE1_WORKSPACE_NAME = F'{WORKSPACE_NAME}-{FEATURE1_NAME}'
FEATURE1_WORKSPACE = FabricRestApi.create_workspace(FEATURE1_WORKSPACE_NAME)

# create feature1 branch and connect to feature1 workspace
AdoProjectManager.create_branch(WORKSPACE_NAME, FEATURE1_NAME, 'main')
FabricRestApi.connect_workspace_to_ado_repo(FEATURE1_WORKSPACE, WORKSPACE_NAME, FEATURE1_NAME)

# apply post sync/deploy fixes to feature1 workspace
DeploymentManager.apply_post_sync_fixes(
    FEATURE1_WORKSPACE_NAME,
    StagingEnvironments.get_dev_environment(),
    True)

FabricRestApi.commit_workspace_to_git(workspace['id'])

# create feature2 workspace
# FEATURE2_NAME = 'feature2'
# FEATURE2_WORKSPACE_NAME = F'{WORKSPACE_NAME} - {FEATURE2_NAME}'
# FEATURE2_WORKSPACE = FabricRestApi.create_workspace(FEATURE2_WORKSPACE_NAME)

# # create feature2 branch and connect to feature2 workspace
# AdoProjectManager.create_branch(WORKSPACE_NAME, FEATURE2_NAME, 'main')
# FabricRestApi.connect_workspace_to_ado_repo(FEATURE2_WORKSPACE, WORKSPACE_NAME, FEATURE2_NAME)

# do not apply post sync/deploy fixes to feature2 workspace

AppLogger.log_job_complete(workspace['id'])
