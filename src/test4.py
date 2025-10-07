from fabric_devops import FabricRestApi, EntraIdTokenManager, EnvironmentSettings

WORKSPACE_NAME = "Custom Notebook Solution with Variable Library"
ITEM_NAME = "Create Lakehouse Tables with VarLib"
workspace = FabricRestApi.get_workspace_by_name(WORKSPACE_NAME)
item = FabricRestApi.get_item_by_name(workspace['id'], ITEM_NAME, 'Notebook')

response = FabricRestApi.create_notebook_schedule(workspace['id'], item)

print(response)