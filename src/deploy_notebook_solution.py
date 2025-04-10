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

AppLogger.log_step(f"Creating [{LAKEHOUSE_NAME}.Lakehouse]")
lakehouse = FabricRestApi.create_lakehouse(workspace['id'], LAKEHOUSE_NAME)
AppLogger.log_substep(f"Lakehouse created with id of [{lakehouse['id']}]")

AppLogger.log_step(f"Creating [{NOTEBOOK_NAME}.Notebook]...")
create_notebook_request = \
    ItemDefinitionFactory.get_notebook_create_request(workspace['id'],  \
                                                      lakehouse,     \
                                                      NOTEBOOK_NAME, \
                                                      'CreateLakehouseTables.py')

notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
AppLogger.log_substep(f"Notebook created with id of [{notebook['id']}]")

AppLogger.log_substep(f"Running notebook [{NOTEBOOK_NAME}]...")
FabricRestApi.run_notebook(workspace['id'], notebook['id'])
AppLogger.log_substep("Notebook run has completed")

AppLogger.log_step(f"Getting SQL Endpoint info for lakehouse [{LAKEHOUSE_NAME}]...")
sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse['id'])
AppLogger.log_substep(f"server: {sql_endpoint['server']}")
AppLogger.log_substep(f"database: {sql_endpoint['database']}")

AppLogger.log_step(f"Creating [{SEMANTIC_MODEL_NAME}.SemanticModel]")
createModelRequest = \
    ItemDefinitionFactory.get_directlake_model_create_request(SEMANTIC_MODEL_NAME,
                                                              'sales_model_DirectLake.bim',
                                                              sql_endpoint['server'],
                                                              sql_endpoint['database'])

model = FabricRestApi.create_item(workspace['id'], createModelRequest)
AppLogger.log_substep(f"Semantic model created with id of [{model['id']}]")

FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

AppLogger.log_step(f"Creating [{REPORT_NAME}.Report]...")
createReportRequest = ItemDefinitionFactory.get_report_create_request(model['id'],
                                                                      REPORT_NAME,
                                                                      'product_sales_summary.json')
report = FabricRestApi.create_item(workspace['id'], createReportRequest)
AppLogger.log_substep(f"Report created with id of [{report['id']}]")

AppLogger.log_job_ended("Solution deployment complete")
