"""Deploy Demo Solution with ADO GIT Intergation"""
import json
from fabric_devops import DeploymentManager, AppLogger, StagingEnvironments, \
                          AdoProjectManager, FabricRestApi, GitHubRestApi, ItemDefinitionFactory

FabricRestApi.create_azure_storage_connection_with_sas_token(
    "https://fabricdevcamp.dfs.core.windows.net",
    "sampledata/ProductSales/Prod/"
)