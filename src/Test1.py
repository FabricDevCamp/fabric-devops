"""Test1"""
from fabric_devops import DeploymentManager, FabricRestApi, EnvironmentSettings, AdoProjectManager

# EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False
# connections = FabricRestApi.list_connections()

# print(connections)

WORKSPACE1_NAME = "Acme"
workspace1 = DeploymentManager.deploy_powerbi_solution(WORKSPACE1_NAME)
DeploymentManager.sync_workspace_to_ado_repo(workspace1)

# WORKSPACE2_NAME = "Contoso"
# workspace2 = DeploymentManager.deploy_powerbi_solution(WORKSPACE2_NAME)
# DeploymentManager.sync_workspace_to_ado_repo(workspace2)
