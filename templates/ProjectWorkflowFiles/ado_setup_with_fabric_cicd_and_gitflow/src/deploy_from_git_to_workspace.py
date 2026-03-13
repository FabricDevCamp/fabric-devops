"""Deploy from GIT to workspace"""

from pathlib import Path
import os
from azure.identity import ClientSecretCredential
from fabric_cicd import deploy_with_config, append_feature_flag
from fabric_devops_utils import EnvironmentSettings, AppLogger

append_feature_flag("enable_experimental_features")
append_feature_flag("enable_config_deploy")
append_feature_flag("enable_shortcut_publish")

AppLogger.log_job("Deploying all workspace items using fabric-cicd trigger by PR completion")

# authenticate as SPN
token_credential = ClientSecretCredential(
  client_id = EnvironmentSettings.AZURE_CLIENT_ID, 
  client_secret = EnvironmentSettings.AZURE_CLIENT_SECRET, 
  tenant_id = EnvironmentSettings.AZURE_TENANT_ID
)

# determine path to deployment configuration file
CONFIG_FILE_PATH = str(Path(__file__).resolve().parent.parent / "workspace//deploy.yml")

# determine target environment by inspevting branch name
branch_name = os.environ.get('BUILD_SOURCEBRANCH').replace('refs/heads/', '')
if branch_name == 'main':
    ENVIRONMEMT = 'prod'
else:
    ENVIRONMEMT = branch_name

deploy_with_config(
  config_file_path=CONFIG_FILE_PATH,
  environment=ENVIRONMEMT
)
