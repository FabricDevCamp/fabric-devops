"""Deploy to test workspace"""

from pathlib import Path
from azure.identity import ClientSecretCredential
from fabric_cicd import deploy_with_config, append_feature_flag
from fabric_devops_utils import EnvironmentSettings, AppLogger

append_feature_flag("enable_experimental_features")
append_feature_flag("enable_config_deploy")
append_feature_flag("enable_shortcut_publish")

AppLogger.log_job("Publihing all items to test with fabric_cicd after PR completion")

# authenticate as SPN
token_credential = ClientSecretCredential(
  client_id = EnvironmentSettings.AZURE_CLIENT_ID, 
  client_secret = EnvironmentSettings.AZURE_CLIENT_SECRET, 
  tenant_id = EnvironmentSettings.AZURE_TENANT_ID
)

# determine path to deployment configuration file
CONFIG_FILE_PATH = str(Path(__file__).resolve().parent.parent / "workspace//deploy.yml")

ENVIRONMEMT = 'test'

deploy_with_config(
  config_file_path=CONFIG_FILE_PATH,
  environment=ENVIRONMEMT,
  token_credential=token_credential
)
