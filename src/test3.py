"""Test3"""

from fabric_devops import DeploymentManager, EnvironmentSettings

EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False

DeploymentManager.cleanup_dev_environment()



