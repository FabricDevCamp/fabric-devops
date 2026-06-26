"""Setup project with fabric-cicd and release flow """
import json
import os

from fabric_devops import ItemDefinitionFactory,FabricRestApi, AppLogger

SOURCE_WORKSPACE_NAME = 'source2' # os.getenv("SOURCE_WORKSPACE_NAME")
TARGET_WORKSPACE_NAME = 'target2' # os.getenv("TARGET_WORKSPACE_NAME")

# source_workspace = FabricRestApi.get_workspace_by_name(SOURCE_WORKSPACE_NAME)

AppLogger.log_step(f"Source workspace: {SOURCE_WORKSPACE_NAME}")

EXPORT = FabricRestApi.export_item_definitions(SOURCE_WORKSPACE_NAME)

IMPORT_REQUEST = {
    "definitionParts": EXPORT['definitionParts'],
    "options": {
        "allowPairingByName": False
    }
}

AppLogger.log_step(f"Target workspace: {TARGET_WORKSPACE_NAME}")
# workspace = FabricRestApi.create_workspace(TARGET_WORKSPACE_NAME)
FabricRestApi.import_item_definitions(TARGET_WORKSPACE_NAME, IMPORT_REQUEST)

#AppLogger.log_job_complete(target_workspace.id)
