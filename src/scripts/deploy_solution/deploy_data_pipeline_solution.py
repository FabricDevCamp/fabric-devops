"""Deploy Data Pipeline Solution"""

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger, AppSettings

WORKSPACE_NAME = "Custom Data Pipeline Solution"
LAKEHOUSE_NAME = "sales"
NOTEBOOKS = [
    { 'name': 'Create 01 Silver Layer', 'template':'BuildSilverLayer.py'},
    { 'name': 'Create 02 Gold Layer', 'template':'BuildGoldLayer.py'},
]
DATA_PIPELINE_NAME = 'Create Lakehouse Tables'
SEMANTIC_MODEL_NAME = 'Product Sales DirectLake Model'
REPORTS = [
    { 'name': 'Product Sales Summary', 'template':'product_sales_summary.json'},
    { 'name': 'Product Sales Time Intelligence', 'template':'product_sales_time_intelligence.json'},
    { 'name': 'Product Sales Top 10 Cities', 'template':'product_sales_top_ten_cities_report.json'},
]

AppLogger.log_job("Deploying Lakehouse solution with Shortcut")

workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)

lakehouse = FabricRestApi.create_lakehouse(workspace['id'], LAKEHOUSE_NAME)

notebook_ids = {}
for notebook_data in NOTEBOOKS:
    create_notebook_request = \
    ItemDefinitionFactory.get_notebook_create_request(workspace['id'],  \
                                                      lakehouse,     \
                                                      notebook_data['name'], \
                                                      notebook_data['template'])
    notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
    notebook_ids[notebook['displayName']] = notebook['id']

connection = FabricRestApi.create_azure_storage_connection_with_account_key(
    AppSettings.AZURE_STORAGE_SERVER,
    AppSettings.AZURE_STORAGE_PATH,
    workspace)

pipeline_template = ItemDefinitionFactory.get_template_file(
    'DataPipelines//CreateLakehouseTables.json')

pipeline_definition = \
    pipeline_template.replace('{WORKSPACE_ID}', workspace['id']) \
                     .replace('{LAKEHOUSE_ID}', lakehouse['id']) \
                     .replace('{CONNECTION_ID}', connection['id']) \
                     .replace('{CONTAINER_NAME}', AppSettings.AZURE_STORAGE_CONTAINER) \
                     .replace('{CONTAINER_PATH}', AppSettings.AZURE_STORAGE_CONTAINER_PATH) \
                     .replace('{NOTEBOOK_ID_BUILD_SILVER}', list(notebook_ids.values())[0]) \
                     .replace('{NOTEBOOK_ID_BUILD_GOLD}', list(notebook_ids.values())[1])

create_pipeline_request = \
    ItemDefinitionFactory.get_data_pipeline_create_request(DATA_PIPELINE_NAME, pipeline_definition)

pipeline = FabricRestApi.create_item(workspace['id'], create_pipeline_request)
FabricRestApi.run_data_pipeline(workspace['id'], pipeline)

sqlEndpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

createModelRequest = \
    ItemDefinitionFactory.get_directlake_model_create_request(SEMANTIC_MODEL_NAME,
                                                              'sales_model_DirectLake.bim',
                                                              sqlEndpoint['server'],
                                                              sqlEndpoint['database'])

model = FabricRestApi.create_item(workspace['id'], createModelRequest)

FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

for report_data in REPORTS:
    create_report_request = ItemDefinitionFactory.get_report_create_request(model['id'],
                                                                            report_data['name'],
                                                                            report_data['template'])
    report = FabricRestApi.create_item(workspace['id'], create_report_request)

AppLogger.log_job_ended("Solution deployment complete")
