"""Deploy Warehouse Solution"""

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger, AppSettings

WORKSPACE_NAME = "Custom Warehouse Solution"
LAKEHOUSE_NAME = "staging"
WAREHOUSE_NAME = "sales"

DATA_PIPELINES = [
    { 'name': 'Load Tables in Staging Lakehouse', 'template':'LoadTablesInStagingLakehouse.json'}
]


AppLogger.log_job("Deploying Warehouse solution")

workspace = FabricRestApi.get_workspace_by_name(WORKSPACE_NAME)

lakehouse = FabricRestApi.get_item_by_name(workspace['id'],LAKEHOUSE_NAME,'Lakehouse')

warehouse_item = FabricRestApi.get_item_by_name(workspace['id'], WAREHOUSE_NAME, "Warehouse")

warehouse_props = FabricRestApi.get_warehouse(workspace['id'], warehouse_item['id'])


print(warehouse_props)