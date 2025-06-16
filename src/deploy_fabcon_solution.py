"Deploy FabCon Wolution"""

from fabric_devops import StagingEnvironments, FabricRestApi, \
                          AppLogger, DeploymentJob, ItemDefinitionFactory
   
def deploy_fabcon_solution(
        target_workspace,
        deploy_job = StagingEnvironments.get_dev_environment()):
    """Deploy FabCon Solution"""

    bronze_lakehouse_name = "sales_bronze"
    silver_lakehouse_name = "sales_silver"
    gold_warehouse_name = "sales"

    semantic_model_name = 'Product Sales DirectLake Model'
    reports = [
        { 'name': 'Product Sales Summary', 'template':'product_sales_summary.json'},
        { 'name': 'Product Sales Time Intelligence', 'template':'product_sales_time_intelligence.json'},
        { 'name': 'Product Sales Top 10 Cities', 'template':'product_sales_top_ten_cities_report.json'},
    ]

    AppLogger.log_job(f"Deploying FabCon Solution to [{target_workspace}]")

    deploy_job.display_deployment_parameters()

    workspace = FabricRestApi.create_workspace(target_workspace)

    FabricRestApi.update_workspace_description(workspace['id'], 'Custom FabCon Solution')

    data_prep_folder = FabricRestApi.create_folder(workspace['id'], 'data_prep')
    data_prep_folder_id = data_prep_folder['id']

    bronze_lakehouse = FabricRestApi.create_lakehouse(
        workspace['id'], 
        bronze_lakehouse_name,
        data_prep_folder_id)

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

    FabricRestApi.create_adls_gen2_shortcut(
        workspace['id'],
        bronze_lakehouse['id'],
        shortcut_name,
        shortcut_path,
        shortcut_location,
        shortcut_subpath,
        connection['id'])

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

    silver_sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], silver_lakehouse)

    FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], silver_sql_endpoint['database'])

    gold_warehouse = FabricRestApi.create_warehouse(
            workspace['id'],
            gold_warehouse_name
        )

    warehouse_connect_string = FabricRestApi.get_warehouse_connection_string(
        workspace['id'],
        gold_warehouse['id'])
    
    data_pipelines = [
        { 'name': 'Create Warehouse Tables', 'template':'CreateWarehouseTables.json'},
        { 'name': 'Create Warehouse Stored Procedures', 'template':'CreateWarehouseStoredProcs.json'},
        { 'name': 'Refresh Warehouse Tables', 'template':'RefreshWarehouseTables.json'}
    ]
    
    for data_pipeline in data_pipelines:
        template_file = f"DataPipelines//{data_pipeline['template']}"
        template_content = ItemDefinitionFactory.get_template_file(template_file)
        template_content = template_content.replace('{WORKSPACE_ID}', workspace['id']) \
                                            .replace('{LAKEHOUSE_ID}', silver_lakehouse['id']) \
                                            .replace('{WAREHOUSE_ID}', gold_warehouse['id']) \
                                            .replace('{WAREHOUSE_CONNECT_STRING}', warehouse_connect_string) \
                                            .replace('{CONNECTION_ID}', connection['id']) \
                                            .replace('{CONTAINER_NAME}', adls_container_name) \
                                            .replace('{CONTAINER_PATH}', adls_container_path)                                                       

        pipeline_create_request = ItemDefinitionFactory.get_data_pipeline_create_request(
            data_pipeline['name'],
            template_content)
        
        pipeline = FabricRestApi.create_item(
            workspace['id'], 
            pipeline_create_request,
            data_prep_folder_id)
        
        FabricRestApi.run_data_pipeline(workspace['id'], pipeline)

    create_model_request = \
        ItemDefinitionFactory.get_directlake_model_create_request(
                semantic_model_name,
                'sales_model_DirectLake.bim',
                warehouse_connect_string,
                gold_warehouse['id'])

    model = FabricRestApi.create_item(workspace['id'], create_model_request)

    FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], gold_warehouse)

    for report_data in reports:
        create_report_request = ItemDefinitionFactory.get_report_create_request(
            model['id'],
            report_data['name'],
            report_data['template'])
            
        FabricRestApi.create_item(workspace['id'], create_report_request)

    AppLogger.log_job_complete(workspace['id'])

    return workspace

deploy_fabcon_solution('Contoso')
