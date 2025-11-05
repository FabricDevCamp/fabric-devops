import json
import os
from fabric_devops import DeploymentManager, StagingEnvironments, FabricRestApi, AdoProjectManager, GitHubRestApi


PROJECT_NAME = 'Product Sales'

DEPLOYMENT_PIPELINE_NAME = PROJECT_NAME
DEV_WORKSPACE_NAME = f"{PROJECT_NAME}-dev"
TEST_WORKSPACE_NAME = f"{PROJECT_NAME}-test"
PROD_WORKSPACE_NAME = f"{PROJECT_NAME}"


DEV_WORKSPACE = FabricRestApi.get_workspace_by_name(DEV_WORKSPACE_NAME)
TEST_WORKSPACE = FabricRestApi.get_workspace_by_name(TEST_WORKSPACE_NAME)
PROD_WORKSPACE = FabricRestApi.get_workspace_by_name(PROD_WORKSPACE_NAME)

deployment_rule_substitutions = \
    DeploymentManager.get_deployment_rules_for_deployment_pipeline(
    DEV_WORKSPACE,
    TEST_WORKSPACE,
    PROD_WORKSPACE)

print( deployment_rule_substitutions )