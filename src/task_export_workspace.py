"""Cleanup Environment"""

from fabric_devops import DeploymentManager, EnvironmentSettings, ItemDefinitionFactory


EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False

WORKSPACE_NAME = "NFL Agent"

ItemDefinitionFactory.export_item_definitions_from_workspace_oldway(WORKSPACE_NAME)
