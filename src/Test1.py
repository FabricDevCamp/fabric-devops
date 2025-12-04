"""Deploy Demo Solution with ADO GIT Intergation"""
import json
from fabric_devops import DeploymentManager, AppLogger, StagingEnvironments, \
                          AdoProjectManager, FabricRestApi, GitHubRestApi, ItemDefinitionFactory

ItemDefinitionFactory.export_item_definitions_from_workspace_oldway("Bruce")