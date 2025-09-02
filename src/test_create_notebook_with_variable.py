"""Deploy solution with fabric_cicd"""

from fabric_devops import DeploymentManager, GitHubRestApi, FabricRestApi, AppLogger, \
                          ItemDefinitionFactory, VariableLibrary, StagingEnvironments

target_workspace = "Contoso"
deploy_job = StagingEnvironments.get_dev_environment()

lakehouse_name = "sales"

AppLogger.log_job(f"Deploying Custom Notebook Solution to [{target_workspace}]")

deploy_job.display_deployment_parameters('web')

workspace = FabricRestApi.create_workspace(target_workspace)

FabricRestApi.update_workspace_description(workspace['id'], 'Custom Notebook Solution')

web_datasource_path = deploy_job.parameters[deploy_job.web_datasource_path_parameter]

variable_library = VariableLibrary()
variable_library.add_variable("web_datasource_path", web_datasource_path)

create_library_request = \
    ItemDefinitionFactory.get_variable_library_create_request(
        "environment_settings",
        variable_library
)

FabricRestApi.create_item(workspace['id'], create_library_request)


lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

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

sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], sql_endpoint['database'])

create_model_request = \
    ItemDefinitionFactory.get_create_item_request_from_folder(
        'Product Sales DirectLake Model.SemanticModel')

model_redirects = {
    '{SQL_ENDPOINT_SERVER}': sql_endpoint['server'],
    '{SQL_ENDPOINT_DATABASE}': sql_endpoint['database']
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

FabricRestApi.create_item(workspace['id'], create_report_request)

AppLogger.log_job_complete(workspace['id'])
