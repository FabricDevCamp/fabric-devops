"""Deploy Shortcut Solution"""

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger, AppSettings, SampleCustomerData

deploy_job = SampleCustomerData.get_seamarkfarms()

WORKSPACE_NAME = "Custom Shortcut Solution"
LAKEHOUSE_NAME = "sales"
NOTEBOOKS = [
    { 'name': 'Create 01 Silver Layer', 'template':'BuildSilverLayer.py'},
    { 'name': 'Create 02 Gold Layer', 'template':'BuildGoldLayer.py'},
]
SEMANTIC_MODEL_NAME = 'Product Sales DirectLake Model'
REPORTS = [
    { 'name': 'Product Sales Summary', 'template':'product_sales_summary.json'},
    { 'name': 'Product Sales Time Intelligence', 'template':'product_sales_time_intelligence.json'},
]


AppLogger.log_job("Deploying Lakehouse solution with Shortcut")

workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)

FabricRestApi.update_workspace_description(workspace['id'], 'Custom Shortcut Solution')

lakehouse = FabricRestApi.create_lakehouse(workspace['id'], LAKEHOUSE_NAME)

connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
    AppSettings.AZURE_STORAGE_SERVER,
    AppSettings.AZURE_STORAGE_PATH,
    workspace)

SHORTCUT_NAME = "sales-data"
SHORTCUT_PATH = "Files"
SHORTCUT_LOCATION = AppSettings.AZURE_STORAGE_SERVER
SHORTCUT_SUBPATH = AppSettings.AZURE_STORAGE_PATH

SHORTCUT = FabricRestApi.create_adls_gen2_shortcut(workspace['id'],
                                                   lakehouse['id'],
                                                   SHORTCUT_NAME,
                                                   SHORTCUT_PATH,
                                                   SHORTCUT_LOCATION,
                                                   SHORTCUT_SUBPATH,
                                                   connection['id'])

for notebook_data in NOTEBOOKS:
    create_notebook_request = \
    ItemDefinitionFactory.get_notebook_create_request(workspace['id'],  \
                                                      lakehouse,     \
                                                      notebook_data['name'], \
                                                      notebook_data['template'])
    notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
    FabricRestApi.run_notebook(workspace['id'], notebook)

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
