"""Demo 08 - Deploy Complete Fabric Solution"""

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger

AppLogger.clear_console()

AppLogger.log_job("Deploying Power BI Solution")

WORKSPACE_NAME = "Contoso"
SEMANTIC_MODEL_NAME = 'Product Sales Imported Model'
REPORT_NAME = 'Product Sales Summary'

workspace = FabricRestApi.get_workspace_by_name(WORKSPACE_NAME)

model = FabricRestApi.get_item_by_name(workspace['id'], SEMANTIC_MODEL_NAME, 'SemanticModel')


for report_number in range(126, 300):
    create_report_request = \
        ItemDefinitionFactory.get_report_create_request(model['id'],
                                                        (REPORT_NAME + str(report_number)),
                                                        'product_sales_summary.json')

    FabricRestApi.create_item(workspace['id'], create_report_request)


items_list = FabricRestApi.list_workspace_items(workspace['id'])
AppLogger.log_step(f'Item count { len(items_list) }')
