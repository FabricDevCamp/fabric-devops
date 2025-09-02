"""Deploy solution with fabric_cicd"""

from fabric_devops import DeploymentJob, FabricRestApi, AppLogger, \
                          ItemDefinitionFactory, VariableLibrary, StagingEnvironments

AppLogger.log_job("Creating shortcut with variables")

target_workspace = "Contoso"
deploy_job = StagingEnvironments.get_prod_environment()
deploy_job.display_deployment_parameters("adls")


data_prep_folder_name = 'staging'
bronze_lakehouse_name = 'sales_bronze'
silver_lakehouse_name = 'sales_silver'
gold_lakehouse_name = 'sales'


workspace = FabricRestApi.create_workspace(target_workspace)
FabricRestApi.update_workspace_description(workspace['id'], 'Custom Shortcut Solution with Variable Library')

data_prep_folder = FabricRestApi.create_folder(workspace['id'], data_prep_folder_name)
data_prep_folder_id = data_prep_folder['id']

bronze_lakehouse = FabricRestApi.create_lakehouse(
    workspace['id'], 
    bronze_lakehouse_name,
    data_prep_folder_id)


adls_container_name = deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
adls_container_path = deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
adls_server = deploy_job.parameters[DeploymentJob.adls_server_parameter]
adls_path = f'/{adls_container_name}{adls_container_path}'

adls_connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
    adls_server,
    adls_path,
    workspace)

adls_connection_id = adls_connection['id']

variable_library = VariableLibrary()
variable_library.add_variable("adls_container_name", adls_container_name)
variable_library.add_variable("adls_container_path", adls_container_path)
variable_library.add_variable("adls_server", adls_server)
variable_library.add_variable("adls_path", adls_path)
variable_library.add_variable("adls_connection_id", adls_connection_id)

create_library_request = \
    ItemDefinitionFactory.get_variable_library_create_request(
        "environment_settings",
        variable_library
)

FabricRestApi.create_item(
    workspace['id'], 
    create_library_request,
    data_prep_folder_id)

shortcut_name = "sales-data"
shortcut_path = "Files"
shortcut_location_variable = '$(/**/environment_settings/adls_server)'
shortcut_subpath_variable = '$(/**/environment_settings/adls_path)'
shortcut_connection_id_variable = '$(/**/environment_settings/adls_connection_id)'

FabricRestApi.create_adls_gen2_shortcut(workspace['id'],
                                        bronze_lakehouse['id'],
                                        shortcut_name,
                                        shortcut_path,
                                        shortcut_location_variable,
                                        shortcut_subpath_variable,
                                        shortcut_connection_id_variable)

silver_lakehouse = FabricRestApi.create_lakehouse(
    workspace['id'],
    silver_lakehouse_name,
    data_prep_folder_id)

FabricRestApi.create_onelake_shortcut(
    workspace['id'],
    silver_lakehouse['id'],
    bronze_lakehouse['id'],
    shortcut_name,
    shortcut_path)

create_notebook_request = \
    ItemDefinitionFactory.get_create_item_request_from_folder(
    'Build Silver.Notebook')

notebook_redirects = {
    '{WORKSPACE_ID}': workspace['id'],
    '{LAKEHOUSE_ID}': silver_lakehouse['id'],
    '{LAKEHOUSE_NAME}': silver_lakehouse['displayName']
}

create_notebook_request = \
    ItemDefinitionFactory.update_part_in_create_request(
        create_notebook_request,
        'notebook-content.py', 
        notebook_redirects)

notebook = FabricRestApi.create_item(
    workspace['id'], 
    create_notebook_request,
    data_prep_folder_id)

FabricRestApi.run_notebook(workspace['id'], notebook)











AppLogger.log_job_complete(workspace['id'])
