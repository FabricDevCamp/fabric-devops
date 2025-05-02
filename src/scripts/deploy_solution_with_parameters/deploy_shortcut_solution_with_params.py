"""Deploy Shortcut Solution"""

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger, \
                          SampleCustomerData, DeploymentJob

deploy_job = SampleCustomerData.get_fabrikam()

WORKSPACE_NAME = deploy_job.target_workspace_name
LAKEHOUSE_NAME = "sales"
NOTEBOOK_FOLDERS = [
    'Create 01 Silver Layer.Notebook',
    'Create 02 Gold Layer.Notebook'
]
SEMANTIC_MODEL_FOLDER = 'Product Sales DirectLake Model.SemanticModel'
REPORT_FOLDERS = [
    'Product Sales Summary.Report',
    'Product Sales Time Intelligence.Report'
]

AppLogger.log_job(f"Deploying Custom Shortcut Solution to [{WORKSPACE_NAME}]")

deploy_job.display_deployment_parameters()

workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)

FabricRestApi.update_workspace_description(workspace['id'], 'Custom Shortcut Solution')

lakehouse = FabricRestApi.create_lakehouse(workspace['id'], LAKEHOUSE_NAME)

adls_container_name = deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
adls_container_path = deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
adls_server = deploy_job.parameters[DeploymentJob.adls_server_path_parameter]
ADLS_PATH = f'/{adls_container_name}{adls_container_path}'

connection = FabricRestApi.create_azure_storage_connection_with_account_key(
    adls_server,
    ADLS_PATH,
    workspace)

SHORTCUT_NAME = "sales-data"
SHORTCUT_PATH = "Files"
SHORTCUT_LOCATION = adls_server
SHORTCUT_SUBPATH = ADLS_PATH

SHORTCUT = FabricRestApi.create_adls_gen2_shortcut(workspace['id'],
                                                   lakehouse['id'],
                                                   SHORTCUT_NAME,
                                                   SHORTCUT_PATH,
                                                   SHORTCUT_LOCATION,
                                                   SHORTCUT_SUBPATH,
                                                   connection['id'])

for notebook_folder in NOTEBOOK_FOLDERS:
    create_notebook_request = \
        ItemDefinitionFactory.get_create_notebook_request_from_folder(
            notebook_folder,
            workspace['id'],
            lakehouse)

    notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
    FabricRestApi.run_notebook(workspace['id'], notebook)

sqlEndpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

create_model_request = \
    ItemDefinitionFactory.get_create_item_request_from_folder(
        SEMANTIC_MODEL_FOLDER)

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

for report_folder in REPORT_FOLDERS:
    create_report_request = \
        ItemDefinitionFactory.get_create_report_request_from_folder(
            report_folder,
            model['id'])

    report = FabricRestApi.create_item(workspace['id'], create_report_request)

AppLogger.log_job_ended("Solution deployment complete")
