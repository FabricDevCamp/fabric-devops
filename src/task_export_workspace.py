"""Cleanup Environment"""

from fabric_devops import DeploymentManager, EnvironmentSettings, ItemDefinitionFactory


WORKSPACE_NAME = "Babyface"

ItemDefinitionFactory.export_item_definitions_from_workspace_oldway(WORKSPACE_NAME)
