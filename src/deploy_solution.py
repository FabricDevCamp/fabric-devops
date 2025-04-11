"""Deploy Power BI Solution"""
import os

from fabric_devops.item_definition_factory import ItemDefinitionFactory
from fabric_devops.fabric_rest_api import FabricRestApi
from fabric_devops.app_logger import AppLogger
from fabric_devops.app_settings import AppSettings

AppSettings.init_app_settings()

def deploy_powerbi_solution():
    """Deploy Power BI Solution"""

    WORKSPANR_NAME = "Custom Power BI Solution"
    SEMANTIC_MODEL_NAME = 'Product Sales Imported Model'
    REPORT_NAME = 'Product Sales Summary'

    AppLogger.log_job("Deploying Power BI Solution")

    workspace = FabricRestApi.create_workspace(WORKSPANR_NAME)

    create_model_request = \
        ItemDefinitionFactory.get_semantic_model_create_request(SEMANTIC_MODEL_NAME,
                                                                'sales_model_import.bim')
    model = FabricRestApi.create_item(workspace['id'], create_model_request)

    web_url = FabricRestApi.get_web_url_from_semantic_model(workspace['id'], model['id'])

    AppLogger.log_substep(f'Creating anonymous Web connection to {web_url} ')

    connection = FabricRestApi.create_anonymous_web_connection(web_url, workspace)

    FabricRestApi.bind_semantic_model_to_connection(workspace['id'], model['id'], connection['id'])

    FabricRestApi.refresh_semantic_model(workspace['id'], model['id'])

    create_report_request = \
        ItemDefinitionFactory.get_report_create_request(model['id'],
                                                        REPORT_NAME,
                                                        'product_sales_summary.json')

    FabricRestApi.create_item(workspace['id'], create_report_request)

    AppLogger.log_job_ended("Solution deployment complete")

def deploy_notebook_solution():
    """Deploy Notebook Solution"""

    WORKSPACE_NAME = "Custom Notebook Solution"
    LAKEHOUSE_NAME = "sales"
    NOTEBOOK_NAME = "Create Lakehouse Tables"
    SEMANTIC_MODEL_NAME = 'Product Sales DirectLake Model'
    REPORT_NAME = 'Product Sales Summary'

    AppLogger.log_job("Deploying Lakehouse solution with Notebook")

    workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)
    lakehouse = FabricRestApi.create_lakehouse(workspace['id'], LAKEHOUSE_NAME)

    create_notebook_request = \
        ItemDefinitionFactory.get_notebook_create_request(workspace['id'],  \
                                                        lakehouse,     \
                                                        NOTEBOOK_NAME, \
                                                        'CreateLakehouseTables.py')
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

    createReportRequest = ItemDefinitionFactory.get_report_create_request(model['id'],
                                                                        REPORT_NAME,
                                                                        'product_sales_summary.json')

    report = FabricRestApi.create_item(workspace['id'], createReportRequest)

    AppLogger.log_job_ended("Solution deployment complete")

def deploy_shortcut_solution():
    """Deploy Shortcut Solution"""

    AppLogger.log_job("Deploying Lakehouse solution with Shortcut")

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

    workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)

    lakehouse = FabricRestApi.create_lakehouse(workspace['id'], LAKEHOUSE_NAME)

    connection = FabricRestApi.create_azure_storage_connection_with_account_key(
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
        FabricRestApi.create_item(workspace['id'], create_report_request)

    AppLogger.log_job_ended("Solution deployment complete")

def deploy_data_pipeline_solution():
    """Deploy Data Pipeline Solution"""

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


match os.getenv("SOLUTION_NAME"):

    case 'Custom Power BI Solution':
        deploy_powerbi_solution()

    case 'Custom Notebook Solution':
        deploy_notebook_solution()

    case 'Custom Shortcut Solution':
        deploy_shortcut_solution()

    case 'Custom Data Pipeline Solution':
        deploy_data_pipeline_solution()

    case '[Deploy all solutions]':
        deploy_powerbi_solution()
        deploy_notebook_solution()
        deploy_shortcut_solution()
        deploy_data_pipeline_solution()
