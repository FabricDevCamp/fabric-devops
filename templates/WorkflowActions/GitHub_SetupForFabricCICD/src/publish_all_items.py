"""Publish All Items"""

from pathlib import Path
import os
from azure.identity import ClientSecretCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
from fabric_devops_utils import EnvironmentSettings, AppLogger

AppLogger.log_job("Publihing all items with fabric_cicd after PR completion")

# authenticate
client_id = EnvironmentSettings.FABRIC_CLIENT_ID
client_secret = EnvironmentSettings.FABRIC_CLIENT_SECRET
tenant_id = EnvironmentSettings.FABRIC_TENANT_ID
token_credential = \
  ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

# determine target branch between test and main
branch_name = os.environ.get('BUILD_SOURCEBRANCH').replace('refs/heads/', '')
AppLogger.log_substep(f'Pipeline triggered by PR completing on branch [{branch_name}]')

match branch_name: 
    case 'test':
      workspace_id = EnvironmentSettings.TEST_WORKSPACE_ID
      ENVIRONMENT = 'TEST'
    case 'main':
      workspace_id = EnvironmentSettings.PROD_WORKSPACE_ID
      ENVIRONMENT = 'PROD'

AppLogger.log_substep(f'Updating workspace with id [{workspace_id}] in environment {ENVIRONMENT}')

item_type_in_scope = [ "Lakehouse", "Notebook", "SemanticModel", "Report", "DataPipeline", 
                       "Environment", "CopyJob", "Dataflow", "Warehouse" ]

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
  workspace_id=workspace_id,
  environment=ENVIRONMENT,
  repository_directory = str(Path(__file__).resolve().parent.parent / "workspace"),
  item_type_in_scope=item_type_in_scope,
  token_credential=token_credential,
)

# Publish all items defined in item_type_in_scope
publish_all_items(target_workspace)

# Unpublish all items defined in item_type_in_scope not found in repository
unpublish_all_orphan_items(target_workspace)
