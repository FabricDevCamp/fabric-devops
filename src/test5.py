"""Setup Deployment Pipelines"""
import json
import os

config  = {
    'workspace_id': '11111111-1111-1111-1111-111111111111'
}

config_json = json.dumps(config, indent=4)


deploy_config_file = 'deploy.config.json'

folder_path = f".//templates//Exports/"
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

full_path = folder_path + deploy_config_file
with open(full_path, 'w', encoding='utf-8') as file:
    file.write(config_json)