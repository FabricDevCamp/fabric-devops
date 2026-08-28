"""Deploy solution to new workspace"""

import subprocess
from fabric_devops import FabricRestApi, ItemDefinitionFactory, DeploymentManager

subprocess.run('cls', check=True, shell=True)

target_workspace = 'Peanuts'

FabricRestApi.create_workspace(target_workspace)

# AppLogger.log_job(f"Deploying Medallion Solution to [{target_workspace}] using Fabric REST APIs")
workspace = FabricRestApi.create_workspace(target_workspace)
FabricRestApi.update_workspace_description(workspace.id, 'Deployment Plan Solution')

staging_folder = FabricRestApi.create_folder(workspace.id, 'staging')
staging_folder_id = staging_folder.id
        
DeploymentManager.create_variable_library_with_adls_connection(workspace, staging_folder_id)

bronze_lakehouse_name = "sales_bronze"
silver_lakehouse_name = "sales_silver"
gold_lakehouse_name = "sales"
     
bronze_lakehouse = FabricRestApi.create_lakehouse(
    workspace.id,
    bronze_lakehouse_name,
    staging_folder_id
)

shortcut_name = "sales-data"
shortcut_path = "Files"
adls_shortcut_location_variable = "$(/**/environment_settings/adls_server)"
adls_shortcut_subpath_variable = "$(/**/environment_settings/adls_shortcut_subpath)"
adls_connection_id_variable = "$(/**/environment_settings/adls_connection_id)"

FabricRestApi.create_adls_gen2_shortcut(
    workspace.id,
    bronze_lakehouse.id,
    shortcut_name,
    shortcut_path,
    adls_shortcut_location_variable,
    adls_shortcut_subpath_variable,
    adls_connection_id_variable
)

silver_lakehouse = FabricRestApi.create_lakehouse(
    workspace.id,
    silver_lakehouse_name,
    staging_folder_id)
   
create_notebook_request = \
    ItemDefinitionFactory.get_create_item_request_from_folder(
        'Deployment Plan Solution', 
        'staging/Build 01 Silver Tables.Notebook'
    )

notebook_redirects = {
    '00000000-0000-0000-0000-000000000000': workspace.id,
    '11111111-1111-1111-1111-111111111111': silver_lakehouse.id,
}

create_notebook_request = \
    ItemDefinitionFactory.update_part_in_create_request(
    create_notebook_request,
    'notebook-content.py', 
    notebook_redirects)

notebook_build_silver = FabricRestApi.create_item(
    workspace.id, 
    create_notebook_request,
    staging_folder_id)

silver_sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace.id, silver_lakehouse)

FabricRestApi.refresh_sql_endpoint_metadata(workspace.id, silver_sql_endpoint['database'])

gold_lakehouse = FabricRestApi.create_lakehouse(
    workspace.id,
    gold_lakehouse_name
)
        
create_notebook_request = ItemDefinitionFactory.get_create_item_request_from_folder(
    'Deployment Plan Solution',
    'staging/Build 02 Gold Tables.Notebook'            
)
      
notebook_redirects = {
    '00000000-0000-0000-0000-000000000000': workspace.id,
    '11111111-1111-1111-1111-111111111111': gold_lakehouse.id,
}        
        
create_notebook_request = ItemDefinitionFactory.update_part_in_create_request(
    create_notebook_request,
    'notebook-content.py', 
    notebook_redirects
)

notebook_build_gold = FabricRestApi.create_item(
    workspace.id, 
    create_notebook_request,
    staging_folder_id
)

FabricRestApi.run_notebook(workspace.id, notebook_build_gold)

gold_sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace.id, gold_lakehouse)

FabricRestApi.refresh_sql_endpoint_metadata(workspace.id, gold_sql_endpoint['database'])

create_model_request = ItemDefinitionFactory.get_create_item_request_from_folder(
    'Deployment Plan Solution',
    'Product Sales DirectLake Model.SemanticModel'
)
            
model_redirects = {
    'abc-xyz.datawarehouse.fabric.microsoft.com': gold_sql_endpoint['server']
}

create_model_request = \
    ItemDefinitionFactory.update_part_in_create_request(
        create_model_request,
        'definition/expressions.tmdl',
        model_redirects
)

model = FabricRestApi.create_item(workspace.id, create_model_request)

FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model.id, gold_lakehouse)

report_folders = [
    'Product Sales Summary.Report',
    'Product Sales Time Intelligence.Report',
    'Product Sales Top 11 Cities.Report'
]

for report_folder in report_folders:
    create_report_request = ItemDefinitionFactory.get_create_report_request_from_folder(
        'Deployment Plan Solution',
        report_folder,
        model.id
    )

    FabricRestApi.create_item(workspace.id, create_report_request)

 
create_notebook_request = ItemDefinitionFactory.get_create_item_request_from_folder(
    'Deployment Plan Solution',
    'staging/Connection Model to Lakehouse.Notebook'            
)
      
notebook_redirects = {
    '00000000-0000-0000-0000-000000000000': workspace.id,
    '11111111-1111-1111-1111-111111111111': gold_lakehouse.id,
}        
        
create_notebook_request = ItemDefinitionFactory.update_part_in_create_request(
    create_notebook_request,
    'notebook-content.py', 
    notebook_redirects
)

notebook_connect_model_to_lakehouse = FabricRestApi.create_item(
    workspace.id, 
    create_notebook_request,
    staging_folder_id
)


create_plan_request = ItemDefinitionFactory.get_create_item_request_from_folder(
    'Deployment Plan Solution',
    'staging/FeatureWorkspaceInit.DeploymentPlan'       
)
      
notebook_redirects = {
    '00000000-0000-0000-0000-000000000000': workspace.id,
    '11111111-1111-1111-1111-111111111111': gold_lakehouse.id,
}        
        
create_notebook_request = ItemDefinitionFactory.update_part_in_create_request(
    create_notebook_request,
    'notebook-content.py', 
    notebook_redirects
)

notebook_connect_model_to_lakehouse = FabricRestApi.create_item(
    workspace.id, 
    create_notebook_request,
    staging_folder_id
)