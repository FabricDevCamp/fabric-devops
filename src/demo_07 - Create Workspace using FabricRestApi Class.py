"""Demo 07 - Create Workspace using FabricRestApi Class"""

from fabric_devops import FabricRestApi, AppLogger

AppLogger.clear_console()

# Begin Demo Script to Create new workspace
WORKSPACE_NAME = "Benji"

workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)

workspace_id = workspace['id']
WORKSPACE_WEB_URL = \
    f'https://app.powerbi.com/groups/{workspace_id}/list?experience=fabric-developer'
AppLogger.log_step(f'Workspace accessible at {WORKSPACE_WEB_URL}')

AppLogger.log_step(f"Next step is to add items to workspace with id of [{workspace_id}]")
AppLogger.log_step_complete()
