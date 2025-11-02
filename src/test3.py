"""Test3"""
import json
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi, StagingEnvironments, AdoProjectManager

EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False

workspace_name = 'JasonBourne'
workspace =  FabricRestApi.get_workspace_by_name(workspace_name)

FabricRestApi.reset_shortcut_cache(workspace['id'])
