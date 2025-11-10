import json
import os
from fabric_devops import DeploymentManager, StagingEnvironments, FabricRestApi, AdoProjectManager, GitHubRestApi


PROJECT_NAME = 'Alpha'

DEPLOYMENT_PIPELINE_NAME = PROJECT_NAME
DEV_WORKSPACE_NAME = f"{PROJECT_NAME}-dev"
TEST_WORKSPACE_NAME = f"{PROJECT_NAME}-test"
PROD_WORKSPACE_NAME = f"{PROJECT_NAME}"


DEV_WORKSPACE = FabricRestApi.get_workspace_by_name(DEV_WORKSPACE_NAME)
TEST_WORKSPACE = FabricRestApi.get_workspace_by_name(TEST_WORKSPACE_NAME)
PROD_WORKSPACE = FabricRestApi.get_workspace_by_name(PROD_WORKSPACE_NAME)

result = DeploymentManager.generate_parameter_yml_file(
    DEV_WORKSPACE,
    TEST_WORKSPACE,
    PROD_WORKSPACE)
