"""Deploy Notebook Solution"""

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger, SampleCustomerData

deploy_job = SampleCustomerData.get_contoso()

WORKSPACE_NAME = deploy_job.target_workspace_name
LAKEHOUSE_NAME = "sales"
NOTEBOOK_NAME = "Create Lakehouse Tables"
SEMANTIC_MODEL_NAME = 'Product Sales DirectLake Model'
REPORT_NAME = 'Product Sales Summary'

AppLogger.log_job(f"Deploying Custom Notebook Solution to [{WORKSPACE_NAME}]")

deploy_job.display_deployment_parameters()

workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)

FabricRestApi.update_workspace_description(workspace['id'], 'Custom Notebook Solution')

lakehouse = FabricRestApi.create_lakehouse(workspace['id'], LAKEHOUSE_NAME)

create_notebook_request = \
    ItemDefinitionFactory.get_create_item_request_from_folder(
        'Create Lakehouse Tables.Notebook')

notebook_redirects = {
    '{WORKSPACE_ID}': workspace['id'],
    '{LAKEHOUSE_ID}': lakehouse['id'],
    '{LAKEHOUSE_NAME}': lakehouse['displayName'],
    '{WEB_DATASOURCE_PATH}': deploy_job.parameters[deploy_job.web_datasource_path_parameter]
}

create_notebook_request = \
    ItemDefinitionFactory.update_part_in_create_request(create_notebook_request,
                                                        'notebook-content.py', 
                                                        notebook_redirects)

notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)

FabricRestApi.run_notebook(workspace['id'], notebook)

sqlEndpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

create_model_request = \
    ItemDefinitionFactory.get_create_item_request_from_folder(
        'Product Sales DirectLake Model.SemanticModel')

model_redirects = {
    '{SQL_ENDPOINT_SERVER}': sqlEndpoint['server'],
    '{SQL_ENDPOINT_DATABASE}': sqlEndpoint['database']
}

create_model_request = \
    ItemDefinitionFactory.update_part_in_create_request(create_model_request,
                                                        'definition/expressions.tmdl',
                                                        model_redirects)

model = FabricRestApi.create_item(workspace['id'], create_model_request)

FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

create_report_request = \
    ItemDefinitionFactory.get_create_report_request_from_folder(
        'Product Sales Summary.Report',
        model['id'])

report = FabricRestApi.create_item(workspace['id'], create_report_request)

AppLogger.log_job_ended("Solution deployment complete")
