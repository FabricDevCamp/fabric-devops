"""Clone Workspace"""
from fabric_devops import ItemDefinitionFactory, AppLogger, FabricRestApi

workspaces = FabricRestApi.list_workspaces()

for workspace in workspaces:

    workspace_name = workspace['displayName']
    AppLogger.log_step(f"Exporting workspace [{workspace_name}]")
    ItemDefinitionFactory.export_item_definitions_from_workspace(workspace_name)


    # AppLogger.log_step(f"Cloning workspace [{workspace_name}]")
    # AppLogger.log_substep("Exporting item definitions")
    # export = FabricRestApi.export_item_definitions(workspace["id"])['DefinitionParts']

    # WORKSPACE_CLONE_NAME = f"{workspace_name} Clone"

    # AppLogger.log_substep(f"Creating workspace [{WORKSPACE_CLONE_NAME}]")
    # workspace_clone = FabricRestApi.create_workspace(WORKSPACE_CLONE_NAME)

    # AppLogger.log_substep("Importing item definitions")
    # FabricRestApi.import_item_definitions(workspace_clone["id"], export)

    # AppLogger.log_job_complete(workspace_clone['id'])
