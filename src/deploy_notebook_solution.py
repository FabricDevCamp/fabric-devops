"""Deploy Notebook Solution"""

from fabric_devops.item_definition_factory import ItemDefinitionFactory
from fabric_devops.fabric_rest_api import FabricRestApi
from fabric_devops.app_logger import AppLogger

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
