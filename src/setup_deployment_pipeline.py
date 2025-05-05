"""Setup Deployment Pipelines"""
import base64
import json

from fabric_devops import FabricRestApi, StagingEnvironments, DeploymentManager, \
     AppLogger, VariableLibrary, Valueset, DeploymentJob, ItemDefinitionFactory

def delete_all_deployment_pipelines():
    """Delete All Deployment Pipelines"""
    AppLogger.log_step("Deleting Pipelines")
    for pipeline in FabricRestApi.list_deployment_pipelines():
        AppLogger.log_substep(f"Deleting {pipeline['displayName']}")
        stages = FabricRestApi.list_deployment_pipeline_stages(pipeline['id'])
        for stage in stages:
            FabricRestApi.unassign_workpace_from_pipeline_stage(pipeline['id'], stage['id'])

        FabricRestApi.delete_deployment_pipeline(pipeline['id'])

def delete_all_workspaces():
    """Delete All Workspaces"""
    AppLogger.log_step("Deleting Workspaces")
    for workspace in FabricRestApi.list_workspaces():
        AppLogger.log_substep(f"Deleting {workspace['displayName']}")
        FabricRestApi.delete_workspace(workspace['id'])

def cleanup_dev_environment():
    """Clean Up Dev Environment"""
    delete_all_deployment_pipelines()
    delete_all_workspaces()

def setup_deployment_pipeline(pipeline_name):
    """Setup Deployment Pipeline"""

    dev_workspace_name = f'{pipeline_name}-dev'
    test_workspace_name = f'{pipeline_name}-test'
    prod_workspace_name = pipeline_name

    pipeline_stages = [ 'dev', 'test', 'prod' ]

    dev_workspace = FabricRestApi.get_workspace_by_name(dev_workspace_name)

    if dev_workspace is None:
        dev_env = StagingEnvironments.get_dev_environment()
        dev_workspace = DeploymentManager.deploy_variable_library_solution(dev_workspace_name, dev_env)

    pipeline = FabricRestApi.create_deployment_pipeline(pipeline_name, pipeline_stages)

    stages = FabricRestApi.list_deployment_pipeline_stages(pipeline['id'])

    for stage in stages:
        if stage['order'] == 0:
            FabricRestApi.assign_workpace_to_pipeline_stage(dev_workspace['id'],
                                                            pipeline['id'],
                                                            stage['id'])
        elif stage['order'] == 1:
            test_workspace = FabricRestApi.create_workspace(test_workspace_name)
            FabricRestApi.assign_workpace_to_pipeline_stage(test_workspace['id'],
                                                            pipeline['id'],
                                                            stage['id'])
        elif stage['order'] == 2:
            prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)
            FabricRestApi.assign_workpace_to_pipeline_stage(prod_workspace['id'],
                                                            pipeline['id'],
                                                            stage['id'])

def deploy_from_dev_to_test(pipeline_name):
    """Deploy Stage from Dev to Test"""
    pipeline = FabricRestApi.get_deployment_pipeline_by_name(pipeline_name)
    stages = FabricRestApi.list_deployment_pipeline_stages(pipeline['id'])

    source_stage_id = stages[0]['id']
    target_stage_id = stages[1]['id']

    AppLogger.log_step("Deploy from [dev] to [test]")
    FabricRestApi.deploy_to_pipeline_stage(pipeline['id'], source_stage_id, target_stage_id)
    AppLogger.log_substep("Deploy operation complete")

def create_and_bind_model_connection(workspace_name):
    """Create and Bind Model Connections"""
    workspace = FabricRestApi.get_workspace_by_name(workspace_name)
    model_name = 'Product Sales DirectLake Model'
    model = FabricRestApi.get_item_by_name(workspace['id'], model_name, 'SemanticModel')
    FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'])

def deploy_from_test_to_prod(pipeline_name):
    """Deploy Stage from Dev to Test"""
    pipeline = FabricRestApi.get_deployment_pipeline_by_name(pipeline_name)
    stages = FabricRestApi.list_deployment_pipeline_stages(pipeline['id'])

    source_stage_id = stages[1]['id']
    target_stage_id = stages[2]['id']

    AppLogger.log_step("Deploy from [test] to [prod]")
    FabricRestApi.deploy_to_pipeline_stage(pipeline['id'], source_stage_id, target_stage_id)
    AppLogger.log_substep("Deploy operation complete")

# def get_variable_list_from_variable_library(workspace_name, library_name):
#     """x"""
#     workspace = FabricRestApi.get_workspace_by_name(workspace_name)
#     variable_library = FabricRestApi.get_item_by_name(workspace['id'],
#                                                       library_name,
#                                                       'VariableLibrary')

#     variable_library_definition = FabricRestApi.get_item_definition(workspace['id'], variable_library)

#     parts = variable_library_definition['definition']['parts']

#     target_env = StagingEnvironments.get_test_environment()

#     deployment_parameters = target_env.parameters

#     # AppLogger.log_step("Deployment Parameters")
#     # for parameter_tuple in target_env.parameters.items():
#     #     print(f'{parameter_tuple[0]}: {parameter_tuple[1]}')
#     #     print( target_env.parameters[parameter_tuple[0]]  )

#     valueset_test = Valueset('test')

#     AppLogger.log_step("Variables")
#     for part in parts:
#         if part['path'] == 'variables.json':
#             payload = part['payload']
#             payload_bytes = base64.b64decode(payload)
#             payload_content = payload_bytes.decode('utf-8')
#             variables = json.loads(payload_content)['variables']
#             for variable in variables:
#                 if variable['name'] in deployment_parameters:
#                     variable_name = variable['name']
#                     variable_value = variable['value']
#                     valueset_test.add_variable_override(variable['name'])

                

#                     # what are we gonna due



#                     parameter_value = deployment_parameters[variable['name']]
#                     AppLogger.log_step(f"name: {variable_name}")
#                     AppLogger.log_substep(f"value: {variable_value}")
#                     AppLogger.log_substep(f"param: {parameter_value}")
#                     AppLogger.log_substep('')
#             break

def start_from_scratch():    
    """XXX"""
    cleanup_dev_environment()
    DEPLOYMENT_PIPELINE_NAME = 'Apollo'
    setup_deployment_pipeline(DEPLOYMENT_PIPELINE_NAME)
    deploy_from_dev_to_test(DEPLOYMENT_PIPELINE_NAME)


def update_variable_library_and_run_data_pipeline(workspace_name, library_name, deployment_job: DeploymentJob):
    """x"""
    workspace = FabricRestApi.get_workspace_by_name(workspace_name)
    variable_library_item = FabricRestApi.get_item_by_name(workspace['id'],
                                                      library_name,
                                                      'VariableLibrary')
    deployment_parameters = deployment_job.parameters

    variable_library_definition = FabricRestApi.get_item_definition(workspace['id'], 
                                                                    variable_library_item)

    parts = variable_library_definition['definition']['parts']

    variables = None

    for part in parts:
        if part['path'] == 'variables.json':
            payload = part['payload']
            payload_bytes = base64.b64decode(payload)
            payload_content = payload_bytes.decode('utf-8')
            variables = json.loads(payload_content)['variables']
            break

    if variables is not None:
        variable_library  = VariableLibrary(variables)

        valueset_test = Valueset('test')
        
        for variable in variables:
            if variable['name'] in deployment_parameters:
                variable_name = variable['name']
                variable_override = deployment_parameters[variable_name] 
                valueset_test.add_variable_override(variable_name, variable_override)

        # set additional overrides with workspace-specific ids
        lakehouse_id_parameter = 'lakehouse_id'
        notebook_id_build_silver_parameter = 'notebook_id_build_silver'
        notebook_id_build_gold_parameter  = 'notebook_id_build_gold'
        adls_connection_id_parameter = 'adls_connection_id'
  

        lakehouse = FabricRestApi.get_item_by_name(workspace['id'], 'sales', 'Lakehouse')
        valueset_test.add_variable_override(lakehouse_id_parameter, lakehouse['id'])

        notebook_build_silver = FabricRestApi.get_item_by_name(workspace['id'], 'Build 01 Silver Layer', 'Notebook')
        valueset_test.add_variable_override(notebook_id_build_silver_parameter, notebook_build_silver['id'])
        
        notebook_build_gold = FabricRestApi.get_item_by_name(workspace['id'], 'Build 02 Gold Layer', 'Notebook')
        valueset_test.add_variable_override(notebook_id_build_gold_parameter, notebook_build_gold['id'])

        adls_server = deployment_job.parameters[DeploymentJob.adls_server_parameter]
        adls_container_name = deployment_job.parameters[DeploymentJob.adls_container_name_parameter]
        adls_container_path = deployment_job.parameters[DeploymentJob.adls_container_path_parameter]
        adls_server_path = adls_container_name + adls_container_path

        connection = FabricRestApi.create_azure_storage_connection_with_account_key(
            adls_server, 
            adls_server_path,
            workspace)
        
        valueset_test.add_variable_override(adls_connection_id_parameter, connection['id'])

        variable_library.add_valueset(valueset_test)

        update_request = ItemDefinitionFactory.get_update_variable_library_request(variable_library)
               
        FabricRestApi.update_item_definition(workspace['id'], variable_library_item, update_request)

        FabricRestApi.set_active_valueset_for_variable_library(
            workspace['id'],
            variable_library_item,
            'test')
        
        # run data pipeline
        data_pipeline_name = 'Create Lakehouse Tables'
        data_pipeline = FabricRestApi.get_item_by_name(workspace['id'], data_pipeline_name, 'DataPipeline')
        FabricRestApi.run_data_pipeline(workspace['id'], data_pipeline)

    else:
         AppLogger.log_error("Ouch")


#start_from_scratch()

update_variable_library_and_run_data_pipeline(
    'Apollo-test', 
    'SolutionConfig', 
    StagingEnvironments.get_test_environment())

# BY Hand - set deployment rule for semantic model to lakehouse and then deploy to stage

#create_and_bind_model_connection("Apollo-test")

# deploy_from_test_to_prod(DEPLOYMENT_PIPELINE_NAME)
# #create_and_bind_model_connection("Apollo")

# 