"""Test3"""
import json
import os

from fabric_devops import FabricRestApi, DeploymentManager

DEPLOYMENT_PIPELINE_NAME = 'Beboop'
DEV_WORKSPACE_NAME = f"{DEPLOYMENT_PIPELINE_NAME}-dev"
TEST_WORKSPACE_NAME = f"{DEPLOYMENT_PIPELINE_NAME}-test"
PROD_WORKSPACE_NAME = f"{DEPLOYMENT_PIPELINE_NAME}"

DEV_WORKSPACE = FabricRestApi.get_workspace_by_name(DEV_WORKSPACE_NAME)
TEST_WORKSPACE = FabricRestApi.get_workspace_by_name(TEST_WORKSPACE_NAME)
PROD_WORKSPACE = FabricRestApi.get_workspace_by_name(PROD_WORKSPACE_NAME)


params = DeploymentManager.generate_parameter_yml_file(
    DEV_WORKSPACE_NAME,
    TEST_WORKSPACE_NAME,
    PROD_WORKSPACE_NAME
)

print(params)