"Deploy FabCon Wolution"""

from fabric_devops import StagingEnvironments, FabricRestApi, \
                          AppLogger, DeploymentJob, ItemDefinitionFactory, VariableLibrary
   
def deploy_shortcut_solution(
        target_workspace,
        deploy_job = StagingEnvironments.get_dev_environment()):
    """Deploy FabCon Solution"""

    bronze_lakehouse_name = "sales_bronze"

    AppLogger.log_job(f"Deploying FabCon Solution to [{target_workspace}]")

    deploy_job.display_deployment_parameters()

    workspace = FabricRestApi.create_workspace(target_workspace)

    bronze_lakehouse = FabricRestApi.create_lakehouse(
        workspace['id'],
        bronze_lakehouse_name)

    adls_container_name = deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
    adls_container_path = deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
    adls_server = deploy_job.parameters[DeploymentJob.adls_server_parameter]
    adls_path = f'/{adls_container_name}{adls_container_path}'

    connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
        adls_server,
        adls_path,
        workspace)

    shortcut_name = "sales-data"
    shortcut_path = "Files"
    shortcut_location = adls_server
    shortcut_subpath = adls_path

    adls_server = deploy_job.parameters[deploy_job.adls_server_parameter]
    adls_container_name = deploy_job.parameters[deploy_job.adls_container_name_parameter]
    adls_container_path = deploy_job.parameters[deploy_job.adls_container_path_parameter]
    adls_path = adls_container_name + adls_container_path

    connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
        adls_server,
        adls_path,
        workspace,
        top_level_step=True)

    variable_library = VariableLibrary()
    variable_library.add_variable("target_location", adls_server)
    variable_library.add_variable("target_subpath",  adls_container_name + adls_container_path)
    variable_library.add_variable("target_connection_id",  connection['id'])
    variable_library.add_variable("target_connection",  connection['displayName'])

    create_library_request = \
        ItemDefinitionFactory.get_variable_library_create_request(
            "SolutionConfig",
            variable_library
    )

    FabricRestApi.create_item(workspace['id'], create_library_request)


    FabricRestApi.create_adls_gen2_shortcut(
        workspace['id'],
        bronze_lakehouse['id'],
        shortcut_name,
        shortcut_path,
        shortcut_location,
        shortcut_subpath,
        connection['id'])
    
    AppLogger.log_job_complete(workspace['id'])

deploy_shortcut_solution('Adventure Works')
