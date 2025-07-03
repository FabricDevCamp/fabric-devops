"""Setup Deployment Pipelines"""
import json
import os

from fabric_devops import FabricRestApi

DEPLOYMENT_PIPELINE_NAME = 'Apollo'
DEV_WORKSPACE_NAME = f"{DEPLOYMENT_PIPELINE_NAME}-dev"
TEST_WORKSPACE_NAME = f"{DEPLOYMENT_PIPELINE_NAME}-test"
PROD_WORKSPACE_NAME = f"{DEPLOYMENT_PIPELINE_NAME}"

DEV_WORKSPACE = FabricRestApi.get_workspace_by_name(DEV_WORKSPACE_NAME)
DEV_WORKSPACE_ITEMS = FabricRestApi.list_workspace_items(DEV_WORKSPACE['id'])

TEST_WORKSPACE = FabricRestApi.get_workspace_by_name(TEST_WORKSPACE_NAME)
TEST_WORKSPACE_ITEMS = FabricRestApi.list_workspace_items(TEST_WORKSPACE['id'])
TEST_ITEMS = {}
for test_item in TEST_WORKSPACE_ITEMS:
    item_name = test_item['displayName'] + "." + test_item['type'] 
    TEST_ITEMS[item_name] = test_item['id']

PROD_WORKSPACE = FabricRestApi.get_workspace_by_name(PROD_WORKSPACE_NAME)
PROD_WORKSPACE_ITEMS = FabricRestApi.list_workspace_items(PROD_WORKSPACE['id'])
PROD_ITEMS = {}
for prod_item in PROD_WORKSPACE_ITEMS:
    item_name = prod_item['displayName'] + "." + prod_item['type'] 
    PROD_ITEMS[item_name] = prod_item['id']

TAB = '    '
OUTPUT = 'find_replace:\n'

OUTPUT += TAB + '"' + DEV_WORKSPACE['id'] + '": # DEV workspace Id\n'
OUTPUT += TAB + TAB + f'TEST: "{TEST_WORKSPACE["id"]}"\n'
OUTPUT += TAB + TAB + f'PROD: "{PROD_WORKSPACE["id"]}"\n'


for workspace_item in DEV_WORKSPACE_ITEMS:
    item_name = workspace_item['displayName'] + "." + workspace_item['type']
    OUTPUT += TAB + '"' + workspace_item['id'] + f'": # [{item_name}] in DEV\n'
    if item_name in TEST_ITEMS:
        OUTPUT += TAB + TAB + f'TEST: "{TEST_ITEMS[item_name]}"\n'
    if item_name in PROD_ITEMS:
        OUTPUT += TAB + TAB + f'PROD: "{PROD_ITEMS[item_name]}"\n'

for workspace_item in TEST_WORKSPACE_ITEMS:
    item_name = workspace_item['displayName'] + "." + workspace_item['type']
    OUTPUT += TAB + '"' + workspace_item['id'] + f'": # [{item_name}] in TEST\n'
    if item_name in TEST_ITEMS:
        OUTPUT += TAB + TAB + f'TEST: "{TEST_ITEMS[item_name]}"\n'
    if item_name in PROD_ITEMS:
        OUTPUT += TAB + TAB + f'PROD: "{PROD_ITEMS[item_name]}"\n'


parameter_file_name = 'parameter.yml'

#file_path = file_path.replace('/', '\\')
folder_path = f".//templates//Exports/"
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

full_path = folder_path + parameter_file_name
os.makedirs(os.path.dirname(full_path), exist_ok=True)
with open(full_path, 'w', encoding='utf-8') as file:
    file.write(OUTPUT)
