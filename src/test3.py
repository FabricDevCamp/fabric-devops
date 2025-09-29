"""Test3"""
import json
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi

EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = True

print( json.dumps(FabricRestApi.list_capacities(), indent=4) )



