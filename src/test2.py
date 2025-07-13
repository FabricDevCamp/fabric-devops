from fabric_devops import DeploymentManager, FabricRestApi, AppLogger, StagingEnvironments


workspace_name = "Bob-dev"
project_name = "Bob"

workspace = DeploymentManager.deploy_powerbi_solution(workspace_name)
DeploymentManager.connect_workspace_to_ado_repo(workspace, project_name)
