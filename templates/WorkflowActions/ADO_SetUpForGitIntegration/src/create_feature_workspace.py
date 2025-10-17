"""Display Workspace Info"""
import json
from fabric_devops_utils import EnvironmentSettings, AppLogger, FabricRestApi


workspace_id = EnvironmentSettings.WORKSPACE_ID

workspace_info = FabricRestApi.get_workspace_info(workspace_id)

print(json.dumps(workspace_info, indent=4))
