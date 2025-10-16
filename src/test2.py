import json
import os
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi




params = DeploymentManager.generate_parameter_yml_file(
    "Charo-dev",
    "Charo-test",
    "Charo"
)

folder_path = ".//exports//params/"
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

full_path = f'{folder_path}parameter.yml'
with open(full_path, 'w', encoding='utf-8') as file:
    file.write(params)