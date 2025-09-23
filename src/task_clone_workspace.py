"""Clone Workspace"""
from fabric_devops import ItemDefinitionFactory, AppLogger, FabricRestApi

WORKSPACE_NAME = "Custom Warehouse Solution"
workspace = FabricRestApi.get_workspace_by_name(WORKSPACE_NAME)

ItemDefinitionFactory.export_item_definitions_from_workspace(WORKSPACE_NAME)

# AppLogger.log_step(f"Cloning workspace [{WORKSPACE_NAME}]")
# AppLogger.log_substep("Exporting item definitions")
# export = FabricRestApi.export_item_definitions(workspace["id"])['DefinitionParts']

# WORKSPACE_CLONE_NAME = f"{WORKSPACE_NAME} Clone"

# AppLogger.log_substep(f"Creating workspace [{WORKSPACE_CLONE_NAME}]")
# workspace_clone = FabricRestApi.create_workspace(WORKSPACE_CLONE_NAME)

# AppLogger.log_substep("Importing item definitions")
# FabricRestApi.import_item_definitions(workspace_clone["id"], export)

# AppLogger.log_job_complete(workspace_clone['id'])
