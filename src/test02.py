"""Setup project with fabric-cicd and release flow """
import os

from fabric_devops import DeploymentManager,FabricRestApi, AppLogger


SOURCE_WORKSPACE_NAME = os.getenv("SOURCE_WORKSPACE")
TARGET_WORKSPACE_NAME = os.getenv("TARGET_WORKSPACE")


source_workspace = FabricRestApi.get_workspace_by_name(SOURCE_WORKSPACE_NAME)

AppLogger.log_step(f"Source workspace: {source_workspace.display_name} [{source_workspace.id}]") 

target_workspace = FabricRestApi.create_workspace(TARGET_WORKSPACE_NAME)

AppLogger.log_step(f"Target workspace: {target_workspace.display_name} [{target_workspace.id}]")

AppLogger.log_job_complete()
