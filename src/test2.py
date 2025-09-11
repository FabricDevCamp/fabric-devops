from fabric_devops import DeploymentManager, FabricRestApi

WORKSPACE_NAME = "Contoso"
#DeploymentManager.deploy_powerbi_solution(WORKSPACE_NAME)

workspace = FabricRestApi.get_workspace_by_name(WORKSPACE_NAME)

FabricRestApi.connect_workspace_to_ado_repo(workspace, workspace['displayName'])
