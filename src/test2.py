"""Deploy Solution"""

from fabric_devops import DeploymentManager, EnvironmentSettings, AppLogger, StagingEnvironments,\
                          AdoProjectManager, FabricRestApi, GitHubRestApi


workspace_name = "Product Sales"
solution_name = "Custom Notebook Solution"
deploy_job = StagingEnvironments.get_prod_environment()

#workspace = DeploymentManager.deploy_solution_by_name(solution_name, workspace_name, deploy_job)

workspace = FabricRestApi.get_workspace_by_name(workspace_name)

# create ADO project and connect project main repo to workspace
# AdoProjectManager.get_project(workspace_name)
FabricRestApi.connect_workspace_to_ado_repo(workspace, workspace_name)
        
AppLogger.log_job_complete(workspace['id'])
