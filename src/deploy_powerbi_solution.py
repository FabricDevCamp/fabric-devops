"""Deploy Power BI Solution"""

from fabric_devops.item_definition_factory import ItemDefinitionFactory
from fabric_devops.fabric_rest_api import FabricRestApi
from fabric_devops.app_logger import AppLogger

WORKSPANR_NAME = "Custom Power BI Solution"
SEMANTIC_MODEL_NAME = 'Product Sales Imported Model'
REPORT_NAME = 'Product Sales Summary'

AppLogger.log_job("Deploying Power BI Solution")

workspace = FabricRestApi.create_workspace(WORKSPANR_NAME)

AppLogger.log_step(f"Creating [{SEMANTIC_MODEL_NAME}.SemanticModel]...")
create_model_request = \
    ItemDefinitionFactory.get_semantic_model_create_request(SEMANTIC_MODEL_NAME,
                                                            'sales_model_import.bim')

model = FabricRestApi.create_item(workspace['id'], create_model_request)
AppLogger.log_substep(f"Semantic model created with id of [{model['id']}]")

AppLogger.log_substep("Getting Web Url of Datasource")
web_url = FabricRestApi.get_web_url_from_semantic_model(workspace['id'], model['id'])

AppLogger.log_substep(f'Creating anonymous Web connection to {web_url} ')

connection = FabricRestApi.create_anonymous_web_connection(web_url, workspace)

AppLogger.log_substep("Binding semantic model to web connection")
FabricRestApi.bind_semantic_model_to_connection(workspace['id'], model['id'], connection['id'])

AppLogger.log_substep('Refreshing semantic model')
FabricRestApi.refresh_semantic_model(workspace['id'], model['id'])
AppLogger.log_substep("Refresh operation complete")

AppLogger.log_step(f"Creating [{REPORT_NAME}.Report]...")
create_report_request = \
    ItemDefinitionFactory.get_report_create_request(model['id'],
                                                    REPORT_NAME,
                                                    'product_sales_summary.json')

report = FabricRestApi.create_item(workspace['id'], create_report_request)
AppLogger.log_substep(f"Report created with id of [{report['id']}]")

AppLogger.log_job_ended("Solution deployment complete")
