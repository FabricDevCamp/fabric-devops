
import os
from azure.identity import ClientSecretCredential
from fabric_cicd import deploy_with_config, append_feature_flag, change_log_level

# enable fabric-cicd debugging
change_log_level()

# enable feature flags for confoturation-based deployment
append_feature_flag("enable_experimental_features")
append_feature_flag("enable_config_deploy")
append_feature_flag("enable_shortcut_publish")

# Create a credential
credential = ClientSecretCredential(
    tenant_id=os.getenv('AZURE_TENANT_ID'),
    client_id=os.getenv('AZURE_CLIENT_ID'),
    client_secret=os.getenv('AZURE_CLIENT_SECRET')
)

config_file_path = f".//templates//FabricSolutions//Shotcut Solution/deploy.yml"
environment = 'prod'
workspace_id = '11111111-1111-1111-1111-111111111111'

config_override = {}
if workspace_id is not None:
    config_override = { "core": { "workspace_id": { environment: workspace_id}}}

deploy_with_config(
    config_file_path=config_file_path,
    environment=environment,
    config_override=config_override,
    token_credential=credential
)

