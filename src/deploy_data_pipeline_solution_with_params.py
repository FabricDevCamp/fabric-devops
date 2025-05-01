"""Deploy Data Pipeline Solution"""

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger, \
                          SampleCustomerData, DeploymentJob

deploy_job = SampleCustomerData.get_northwind()

WORKSPACE_NAME = deploy_job.target_workspace_name
LAKEHOUSE_NAME = "sales"
NOTEBOOK_FOLDERS = [
    'Build 01 Silver Layer.Notebook',
    'Build 02 Gold Layer.Notebook'
]
DATA_PIPELINE_FOLDER = 'Create Lakehouse Tables.DataPipeline'
SEMANTIC_MODEL_FOLDER = 'Product Sales DirectLake Model.SemanticModel'
REPORT_FOLDERS = [
    'Product Sales Summary.Report',
    'Product Sales Time Intelligence.Report',
    'Product Sales Top 10 Cities.Report'
]

AppLogger.log_job(f"Deploying Custom Data Pipeline Solution to [{WORKSPACE_NAME}]")

deploy_job.display_deployment_parameters()

workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)

lakehouse = FabricRestApi.create_lakehouse(workspace['id'], LAKEHOUSE_NAME)

notebook_ids = []

for notebook_folder in NOTEBOOK_FOLDERS:
    create_notebook_request = \
        ItemDefinitionFactory.get_create_notebook_request_from_folder(
            notebook_folder,
            workspace['id'],
            lakehouse)

    notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
    notebook_ids.append(notebook['id'])

adls_server_path = deploy_job.parameters[DeploymentJob.adls_server_path_parameter]
adls_container_name = deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
adls_container_path = deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
adls_path = adls_container_name + adls_container_path


connection = FabricRestApi.create_azure_storage_connection_with_account_key(
    adls_server_path,
    adls_path,
    workspace)

create_pipeline_request = \
    ItemDefinitionFactory.get_create_item_request_from_folder(
        DATA_PIPELINE_FOLDER)

pipelineRedirects = {
    '{WORKSPACE_ID}': workspace['id'],
    '{LAKEHOUSE_ID}': lakehouse['id'],
    '{CONNECTION_ID}': connection['id'],
    '{CONTAINER_NAME}': adls_container_name,
    '{CONTAINER_PATH}': adls_container_path,
    '{NOTEBOOK_ID_BUILD_SILVER}': notebook_ids[0],
    '{NOTEBOOK_ID_BUILD_GOLD}': notebook_ids[1]
}

create_pipeline_request = \
    ItemDefinitionFactory.update_part_in_create_request(
        create_pipeline_request, 
        'pipeline-content.json',
        pipelineRedirects)

pipeline = FabricRestApi.create_item(workspace['id'], create_pipeline_request)

FabricRestApi.run_data_pipeline(workspace['id'], pipeline)

sqlEndpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

create_model_request = \
    ItemDefinitionFactory.get_create_item_request_from_folder(
        SEMANTIC_MODEL_FOLDER)

model_redirects = {
    '{SQL_ENDPOINT_SERVER}': sqlEndpoint['server'],
    '{SQL_ENDPOINT_DATABASE}': sqlEndpoint['database']
}

create_model_request = \
    ItemDefinitionFactory.update_part_in_create_request(
        create_model_request,
        'definition/expressions.tmdl',
        model_redirects)

model = FabricRestApi.create_item(workspace['id'], create_model_request)

FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

for report_folder in REPORT_FOLDERS:
    create_report_request = \
        ItemDefinitionFactory.get_create_report_request_from_folder(
            report_folder,
            model['id'])

    report = FabricRestApi.create_item(workspace['id'], create_report_request)

AppLogger.log_job_ended("Solution deployment complete")
