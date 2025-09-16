from fabric_devops import DeploymentManager, FabricRestApi

WORKSPACE_NAME = "Contoso"
DeploymentManager.deploy_powerbi_solution(WORKSPACE_NAME)

