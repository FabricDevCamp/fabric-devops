"""Deploy Power BI Solution"""

from fabric_devops import ItemDefinitionFactory, AppLogger, AppSettings, FabricRestApi

WORKSPACE_NAME = "Custom Power BI Solution"
SEMANTIC_MODEL_NAME = 'Product Sales Imported Model'
REPORT_NAME = 'Product Sales Summary'

AppLogger.log_job("Deploying Power BI Solution")

workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)

create_model_request = \
    ItemDefinitionFactory.get_semantic_model_create_request(SEMANTIC_MODEL_NAME,
                                                            'sales_model_import.bim')
model = FabricRestApi.create_item(workspace['id'], create_model_request)

web_url = FabricRestApi.get_web_url_from_semantic_model(workspace['id'], model['id'])

connection = FabricRestApi.create_anonymous_web_connection(web_url, workspace)

FabricRestApi.bind_semantic_model_to_connection(workspace['id'], model['id'], connection['id'])

FabricRestApi.refresh_semantic_model(workspace['id'], model['id'])

create_report_request = \
    ItemDefinitionFactory.get_report_create_request(model['id'],
                                                    REPORT_NAME,
                                                    'product_sales_summary.json')

report = FabricRestApi.create_item(workspace['id'], create_report_request)

AppLogger.log_job_ended("Solution deployment complete")
