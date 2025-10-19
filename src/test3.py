"""Test3"""
import json
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi, ItemDefinitionFactory



FabricRestApi.commit_workspace_to_git(
    '759cc0f4-74d2-40b4-b1de-0d5be02047dc',
    commit_comment = 'Sync updates from feature workspace to repo after applying fixes')

