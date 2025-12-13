"""Test3"""
import json
from fabric_devops import DeploymentManager, ItemDefinitionFactory, FabricRestApi, StagingEnvironments, DeploymentJob


workspace = FabricRestApi.get_workspace_by_name('Embed')
ItemDefinitionFactory.export_item_definitions_from_workspace_oldway("Embed", "Report")