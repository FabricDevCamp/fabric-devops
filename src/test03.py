"""Setup project with fabric-cicd and release flow """
import json
import os
import yaml

from fabric_devops import AppLogger, FabricRestApi, AdoProjectManager, DeploymentManager

os.system("cls")


env_id = AdoProjectManager.create_environment('Apollo', 'foo5')

print(env_id)
