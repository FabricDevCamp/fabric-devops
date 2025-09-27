from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi

EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False

WORKSPACE_NAME = "Angelina"

DeploymentManager.deploy_copyjob_solution(WORKSPACE_NAME)
