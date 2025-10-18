"""Demo 05 - Execute Fabric REST API Call to List Capacities"""

from fabric_devops_utils import AppLogger, FabricRestApi

# run demo
AppLogger.clear_console()

FabricRestApi.display_capacities()

FabricRestApi.display_workspaces()

FabricRestApi.display_connections()

AppLogger.log_job_complete()
