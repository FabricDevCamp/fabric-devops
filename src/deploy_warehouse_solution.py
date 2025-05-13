"""Deploy Warehouse Solution"""

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger, AppSettings

WORKSPACE_NAME = "Custom Warehouse Solution"
LAKEHOUSE_NAME = "staging"
WAREHOUSE_NAME = "sales"

DATA_PIPELINES = [
    { 'name': 'Load Tables in Staging Lakehouse', 'template':'LoadTablesInStagingLakehouse.json'},
    { 'name': 'Create Warehouse Tables', 'template':'CreateWarehouseTables.json'},
    { 'name': 'Create Warehouse Stored Procedures', 'template':'CreateWarehouseStoredProcedures.json'},
    { 'name': 'Refresh Warehouse Tables', 'template':'RefreshWarehouseTables.json'}
]

SEMANTIC_MODEL_NAME = 'Product Sales DirectLake Model'
REPORTS = [
    { 'name': 'Product Sales Summary', 'template':'product_sales_summary.json'},
    { 'name': 'Product Sales Time Intelligence', 'template':'product_sales_time_intelligence.json'},
    { 'name': 'Product Sales Top 10 Cities', 'template':'product_sales_top_ten_cities_report.json'},
]

AppLogger.log_job("Deploying Warehouse solution")

workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)

data_prep_folder = FabricRestApi.create_folder(workspace['id'], 'data_prep')
data_prep_folder_id = data_prep_folder['id']

lakehouse = FabricRestApi.create_lakehouse(
    workspace['id'],
    LAKEHOUSE_NAME,
    data_prep_folder_id)

warehouse = FabricRestApi.create_warehouse(
    workspace['id'],
    WAREHOUSE_NAME
)

warehouse_connect_string = FabricRestApi.get_warehouse_connection_string(
        workspace['id'],
        warehouse['id'])

connection = FabricRestApi.create_azure_storage_connection_with_account_key(
    AppSettings.AZURE_STORAGE_SERVER,
    AppSettings.AZURE_STORAGE_PATH,
    workspace)

for data_pipeline in DATA_PIPELINES:
    TEMPLATE_FILE = f"DataPipelines\\{data_pipeline['template']}"
    template_content = ItemDefinitionFactory.get_template_file(TEMPLATE_FILE)
    template_content = template_content.replace('{WORKSPACE_ID}', workspace['id']) \
                                       .replace('{LAKEHOUSE_ID}', lakehouse['id']) \
                                       .replace('{WAREHOUSE_ID}', warehouse['id']) \
                                       .replace('{WAREHOUSE_CONNECT_STRING}', warehouse_connect_string) \
                                       .replace('{CONNECTION_ID}', connection['id'])
    

    pipeline_create_request = ItemDefinitionFactory.get_data_pipeline_create_request(
        data_pipeline['name'],
        template_content)
    
    pipeline = FabricRestApi.create_item(
        workspace['id'], 
        pipeline_create_request,
        data_prep_folder_id)

    FabricRestApi.run_data_pipeline(workspace['id'], pipeline)


createModelRequest = \
    ItemDefinitionFactory.get_directlake_model_create_request(SEMANTIC_MODEL_NAME,
                                                              'sales_model_DirectLake.bim',
                                                              warehouse_connect_string,
                                                              warehouse['id'])

model = FabricRestApi.create_item(workspace['id'], createModelRequest)

FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

for report_data in REPORTS:
    create_report_request = ItemDefinitionFactory.get_report_create_request(model['id'],
                                                                            report_data['name'],
                                                                            report_data['template'])
    report = FabricRestApi.create_item(workspace['id'], create_report_request)

AppLogger.log_job_ended("Solution deployment complete")
