"""Test3"""
import json
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi, StagingEnvironments, AdoProjectManager


workspace_name = 'jerico'
var_lib_name = "environment_settings"

workspace =  FabricRestApi.get_workspace_by_name(workspace_name)
var_lib = FabricRestApi.get_item_by_name(workspace['id'], var_lib_name, 'VariableLibrary')

FabricRestApi.set_active_valueset_for_variable_library(
    workspace['id'],
    var_lib,
    'dev')