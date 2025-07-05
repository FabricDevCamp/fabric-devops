"""Setup Deployment Pipelines"""
import json


deploy_config_file = 'deploy.config.json'
folder_path = f".//templates//Exports/"
full_path = folder_path + deploy_config_file
with open(full_path, 'r', encoding='utf-8') as file:
    config = json.load(file)

print(config['workspace_id'])
