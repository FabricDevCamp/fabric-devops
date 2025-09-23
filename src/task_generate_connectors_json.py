"""Generator connector.json file"""
import json
import os
from fabric_devops import FabricRestApi

connectors = FabricRestApi.list_supported_connection_types()

connectors_json = json.dumps(connectors, indent=4)

folder_path = ".//templates//Exports//Connectors/"
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

full_path = f'{folder_path}connectors.json'
with open(full_path, 'w', encoding='utf-8') as file:
    file.write(connectors_json)
