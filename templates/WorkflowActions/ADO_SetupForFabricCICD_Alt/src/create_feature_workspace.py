"""Display Workspace Info"""
import json
from fabric_devops_utils import EnvironmentSettings, AppLogger, FabricRestApi


AppLogger.log_job("Create feature workspace")

AppLogger.log_step("Getting Dev workspace info")
dev_workspace_id = EnvironmentSettings.DEV_WORKSPACE_ID
dev_workspace_info = FabricRestApi.get_workspace_info(dev_workspace_id)
print(json.dumps(dev_workspace_info, indent=4))

AppLogger.log_step("Getting Prod workspace info")

prod_workspace_id = EnvironmentSettings.PROD_WORKSPACE_ID
prod_workspace_info = FabricRestApi.get_workspace_info(prod_workspace_id)
print(json.dumps(prod_workspace_info, indent=4))
