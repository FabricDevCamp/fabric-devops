"""Deploy Solution"""
import os

from fabric_devops import DeploymentManager, EnvironmentSettings, AppLogger, StagingEnvironments,\
                          AdoProjectManager, FabricRestApi, GitHubRestApi


PROJECT_NAME = 'Ringo'
SOLUTION_NAME ="Custom Pipeline Solution"
deploy_job = StagingEnvironments.get_dev_environment()

DEV_WORKSPACE_NAME = F'{PROJECT_NAME}-dev'
PROD_WORKSPACE_NAME = F'{PROJECT_NAME}'

dev_deploy_job = StagingEnvironments.get_dev_environment()
prod_deploy_job = StagingEnvironments.get_prod_environment()

dev_workspace = DeploymentManager.deploy_solution_by_name(
    SOLUTION_NAME, 
    DEV_WORKSPACE_NAME,
    dev_deploy_job)

prod_workspace = FabricRestApi.create_workspace(PROD_WORKSPACE_NAME)

DeploymentManager.setup_two_stage_ado_repo(
      dev_workspace,
      prod_workspace,
      PROJECT_NAME)

        