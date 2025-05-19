"""Deploy Variable Solution"""

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger, \
                          AppSettings, VariableLibrary, SampleCustomerData

AppSettings.RUN_AS_SERVICE_PRINCIPAL = False

deploy_job = SampleCustomerData.get_seamarkfarms()

WORKSPACE_NAME = deploy_job.target_workspace_name
LAKEHOUSE_NAME = "sales"
NOTEBOOKS = [
    { 'name': 'Build 01 Silver Layer', 'template':'BuildSilverLayer.py'},
    { 'name': 'Build 02 Gold Layer', 'template':'BuildGoldLayer.py'},
]
DATA_PIPELINE_NAME = 'Create Lakehouse Tables'
SEMANTIC_MODEL_NAME = 'Product Sales DirectLake Model'
REPORTS = [
    { 'name': 'Product Sales Summary', 'template':'product_sales_summary.json'}
]

AppLogger.log_job("Deploying Custom Variable Library solution")

workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)

lakehouse = FabricRestApi.create_lakehouse(workspace['id'], LAKEHOUSE_NAME)

notebook_ids = []
for notebook_data in NOTEBOOKS:
    create_notebook_request = \
    ItemDefinitionFactory.get_notebook_create_request(workspace['id'],  \
                                                      lakehouse,     \
                                                      notebook_data['name'], \
                                                      notebook_data['template'])
    notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
    notebook_ids.append(notebook['id'])

connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
    AppSettings.AZURE_STORAGE_SERVER,
    AppSettings.AZURE_STORAGE_PATH,
    workspace)

variable_library = VariableLibrary()

variable_library.add_variable("web_datasource_path", 
                              deploy_job.parameters[deploy_job.web_datasource_path_parameter])
variable_library.add_variable("adls_server", 
                              deploy_job.parameters[deploy_job.adls_server_parameter])
variable_library.add_variable("adls_container_name",  
                              deploy_job.parameters[deploy_job.adls_container_name_parameter])
variable_library.add_variable("adls_container_path",  
                              deploy_job.parameters[deploy_job.adls_container_path_parameter])

variable_library.add_variable("adls_connection_id",  connection['id'])

variable_library.add_variable("lakehouse_id",  lakehouse['id'])

variable_library.add_variable("notebook_id_build_silver",  notebook_ids[0])

variable_library.add_variable("notebook_id_build_gold",  notebook_ids[1])

create_library_request = \
    ItemDefinitionFactory.get_variable_library_create_request(
        "SolutionConfig",
        variable_library
)

library = FabricRestApi.create_item(workspace['id'], create_library_request)

pipeline_definition = ItemDefinitionFactory.get_template_file(
    'DataPipelines//CreateLakehouseTablesWithVarLib.json')

create_pipeline_request = \
    ItemDefinitionFactory.get_data_pipeline_create_request(DATA_PIPELINE_NAME,
                                                           pipeline_definition)

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
