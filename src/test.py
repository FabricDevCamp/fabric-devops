"Test"""

from fabric_devops import StagingEnvironments, FabricRestApi, \
                          AppLogger, DeploymentJob
   
def deploy_fabcon_solution(
        target_workspace,
        deploy_job = StagingEnvironments.get_dev_environment()):
    """Deploy FabCon Solution"""

    bronze_lakehouse_name = "sales_bronze"
    silver_lakehouse_name = "sales_silver"
    gold_warehouse_name = "sales"

    # semantic_model_folder = 'Product Sales DirectLake Model.SemanticModel'
    # report_folders = [
    #     'Product Sales Summary.Report'
    # ]


    AppLogger.log_job(f"Deploying FabCon Solution to [{target_workspace}]")

    deploy_job.display_deployment_parameters()

    workspace = FabricRestApi.create_workspace(target_workspace)

    FabricRestApi.update_workspace_description(workspace['id'], 'Custom FabCon Solution')

    bronze_lakehouse = FabricRestApi.create_lakehouse(workspace['id'], bronze_lakehouse_name)

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

    FabricRestApi.create_adls_gen2_shortcut(workspace['id'],
                                            bronze_lakehouse['id'],
                                            shortcut_name,
                                            shortcut_path,
                                            shortcut_location,
                                            shortcut_subpath,
                                            connection['id'])
    
    AppLogger.log_job_complete(workspace['id'])

    return workspace

deploy_fabcon_solution('Contoso')
