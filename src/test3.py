"""Test3"""
import json
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi, StagingEnvironments, AdoProjectManager


feature_name = 'feature1'
workspace_name = 'Product Sales - feature1'

deploy_job = StagingEnvironments.get_dev_environment()

workspace = FabricRestApi.get_workspace_by_name(workspace_name)

DeploymentManager.update_variable_library(
    workspace_name,
    'environment_settings',
    StagingEnvironments.get_dev_environment(),
    feature_name
)