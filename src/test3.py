"""Test3"""
import json
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi, ItemDefinitionFactory

DeploymentManager.deploy_data_pipeline_solution_with_varlib("Psycho")