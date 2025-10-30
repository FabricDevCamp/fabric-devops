"""Test3"""
import json
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi, StagingEnvironments, AdoProjectManager


workspace_name = 'Betsy'
workspace =  FabricRestApi.get_workspace_by_name(workspace_name)

AdoProjectManager.create_project(workspace_name, workspace)
