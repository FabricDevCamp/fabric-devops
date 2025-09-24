from fabric_devops import FabricRestApi, EntraIdTokenManager, EnvironmentSettings

WORKSPACE_NAME = "Custom User Data Function Solution"
UDF_NAME = "udf_playground"
workspace = FabricRestApi.get_workspace_by_name(WORKSPACE_NAME)
udf = FabricRestApi.get_item_by_name(workspace['id'], UDF_NAME, 'UserDataFunction')

FabricRestApi.call_user_defined_function(
    workspace['id'],
    udf['id'],
    'write_text_file',
    {
        "message": "Roger that. It's a big 10-4"
    }   
)