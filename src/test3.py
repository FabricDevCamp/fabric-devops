"""Test3"""
import json
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi, StagingEnvironments, DeploymentJob


workspace_name = 'Stanley'
workspace =  FabricRestApi.get_workspace_by_name(workspace_name)
lakehouse = FabricRestApi.get_item_by_name(workspace['id'], 'sales', "Lakehouse")
shortcuts = FabricRestApi.list_shortcuts(workspace['id'], lakehouse['id'])

print( json.dumps(shortcuts, indent=4) )

deploy_job = StagingEnvironments.get_prod_environment()

workspace_items = FabricRestApi.list_workspace_items(workspace['id'])

variable_libraries = list(filter(lambda item: item['type']=='VariableLibrary', workspace_items))
for variable_library in variable_libraries:
    FabricRestApi.set_active_valueset_for_variable_library(
        workspace['id'],
        variable_library,
        deploy_job.name
    )

adls_container_name = deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
adls_container_path = deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
adls_server = deploy_job.parameters[DeploymentJob.adls_server_parameter]
adls_path = f'/{adls_container_name}{adls_container_path}'

shortcut_name = "sales-data"
shortcut_path = "Files"

adls_shortcut_location_variable = "$(/**/environment_settings/adls_server)"
adls_shortcut_subpath_variable = "$(/**/environment_settings/adls_shortcut_subpath)"
adls_connection_id_variable = "$(/**/environment_settings/adls_connection_id)"

FabricRestApi.create_adls_gen2_shortcut(workspace['id'],
                                        lakehouse['id'],
                                        shortcut_name,
                                        shortcut_path,
                                        adls_shortcut_location_variable,
                                        adls_shortcut_subpath_variable,
                                        adls_connection_id_variable)