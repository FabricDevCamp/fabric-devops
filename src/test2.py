from fabric_devops import DeploymentManager, FabricRestApi

workspace_name = "Contoso"
workspace = FabricRestApi.get_workspace_by_name(workspace_name)

FabricRestApi.delete_workspace(workspace['id'])

Deplo
