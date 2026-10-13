"""Deploy solution to new workspace"""

import subprocess
from fabric_devops import FabricRestApi, EnvironmentSettings

FabricRestApi.display_workspaces()

x = EnvironmentSettings.AZURE_CLIENT_ID