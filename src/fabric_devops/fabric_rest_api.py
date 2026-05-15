"""Fabric REST API Wrapper Class"""

import json
from json.decoder import JSONDecodeError
import os
import time
import requests
from azure.identity import DefaultAzureCredential
from microsoft_fabric_api import FabricClient

from .app_logger import AppLogger
from .environment_settings import EnvironmentSettings

class FabricRestApi:
    """Fabric REST API Wrapper Class"""
    
    #region class-level fields

    # used for creating DefaultAzureCredential connections with SPN creds
    AZURE_TENANT_ID = os.getenv('AZURE_TENANT_ID')
    AZURE_CLIENT_ID = os.getenv('AZURE_CLIENT_ID')
    AZURE_CLIENT_SECRET = os.getenv('AZURE_CLIENT_SECRET')
    
    credential = DefaultAzureCredential()
    fabric_client = FabricClient(credential)

    ADMIN_USER_ID = os.getenv('ADMIN_USER_ID') 
    DEVELOPERS_GROUP_ID = os.getenv('DEVELOPERS_GROUP_ID')
    FABRIC_CAPACITY_ID = os.getenv('FABRIC_CAPACITY_ID')
    
    # used for creating ADLS connections using SAS token
    AZURE_STORAGE_SAS_TOKEN = os.getenv('AZURE_STORAGE_SAS_TOKEN')
    
    ADO_ORGANIZATION = os.getenv('ADO_ORGANIZATION')

    #endregion
    
    #region capacity functions
    
    @classmethod
    def list_capacities(cls):
        """list capacities accessible to caller"""
        return cls.fabric_client.core.capacities.list_capacities()

    @classmethod
    def display_capacities(cls):
        """Display capacities in console"""
        capacities = cls.list_capacities()
        AppLogger.log_step("Capacities:")
        for capacity in capacities:
            AppLogger.log_substep(f"Capacity: {capacity.display_name}, ID: {capacity.id}")

    #endregion

    #region workspace functions

    @classmethod
    def list_workspaces(cls):
        """list workspaces accessible to caller"""
        return cls.fabric_client.core.workspaces.list_workspaces()

    @classmethod
    def display_workspaces(cls):
        """Display workspaces in console"""
        workspaces = cls.list_workspaces()
        AppLogger.log_step("Workspaces:")
        for workspace in workspaces:
            AppLogger.log_substep(f"Workspace: {workspace.display_name}, ID: {workspace.id}")        
        
    @classmethod
    def get_workspace_info(cls, workspace_id):
        """Get Workspace information by ID"""
        return cls.fabric_client.core.workspaces.get_workspace(workspace_id)

    @classmethod
    def get_workspace_by_name(cls, display_name):
        """Get Workspace item by display name"""
        workspaces = cls.list_workspaces()
        for workspace in workspaces:
            if workspace.display_name == display_name:
                return workspace
        return None
    
    @classmethod
    def get_workspace_info_by_name(cls, display_name):
        """Get Workspace information by ID"""
        workspace = cls.get_workspace_by_name(display_name)        
        return cls.fabric_client.core.workspaces.get_workspace(workspace.id)

    @classmethod
    def create_workspace(cls, display_name, capacity_id = None, reuse_existing_workspace = False):
        """Create a new Fabric workspace"""
        AppLogger.log_step(f'Creating workspace [{display_name}]')

        if capacity_id is None:
            capacity_id = cls.FABRIC_CAPACITY_ID

        existing_workspace = cls.get_workspace_by_name(display_name)
        if existing_workspace is not None:
            if reuse_existing_workspace:
                AppLogger.log_substep("Found existing workspace with the same name")
                return existing_workspace
            else: 
                AppLogger.log_substep("Deleting existing workspace with the same name")
                cls.delete_workspace(existing_workspace.id)
        
        create_request = {
            "display_name": display_name,
            "capacity_id": capacity_id
        }

        workspace = cls.fabric_client.core.workspaces.create_workspace(
            create_workspace_request=create_request
        )
        
        workspace_id = workspace.id
        AppLogger.log_substep(f'Workspace created with Id of [{workspace_id}]')
        
        # add workspace role assignment for admin user
        AppLogger.log_substep('Adding workspace role assignment for admin user')
        cls.add_workspace_role_assignment_for_user(
            workspace_id=workspace_id,
            user_id=cls.ADMIN_USER_ID,
            role_name='Admin'
        )        

        return workspace

    @classmethod
    def get_or_create_workspace(cls, display_name, capacity_id = None):
        """Get or create workspace"""

        existing_workspace = cls.get_workspace_by_name(display_name)
        if existing_workspace is not None:
            AppLogger.log_step(f"Getting existing workspace {display_name}")
            return existing_workspace
                
        return cls.create_workspace(display_name, capacity_id)

    @classmethod
    def update_workspace_description(cls, workspace_id, description = None):
        """Update Workspace properties"""
        
        update_request = {
            'description': description
        }
                    
        cls.fabric_client.core.workspaces.update_workspace(
            workspace_id=workspace_id, 
            update_workspace_request=update_request
        )

    @classmethod
    def add_workspace_role_assignment_for_user(cls, workspace_id, user_id, role_name):
        """Add workspace user"""
   
        add_role_request = {
            "role": role_name,
            "principal": {
                'id': user_id,
                'type': 'User'                
            }            
        }

        cls.fabric_client.core.workspaces.add_workspace_role_assignment(        
            workspace_id=workspace_id,
            workspace_role_assignment_request=add_role_request
        )

    @classmethod
    def add_workspace_role_assignment_for_group(cls, workspace_id, group_id, role_name):
        """Add workspace group"""

        add_role_request = {
            "role": role_name,
            "principal": {
                'id': group_id,
                'type': 'Group'                
            }            
        }

        cls.fabric_client.core.workspaces.add_workspace_role_assignment(        
            workspace_id=workspace_id,
            workspace_role_assignment_request=add_role_request
        )

    @classmethod
    def add_workspace_role_assignment_for_spn(cls, workspace_id, spn_id, role_name):
        """Add workspace service principal"""

        add_role_request = {
            "role": role_name,
            "principal": {
                'id': spn_id,
                'type': 'ServicePrincipal'                
            }            
        }

        cls.fabric_client.core.workspaces.add_workspace_role_assignment(        
            workspace_id=workspace_id,
            workspace_role_assignment_request=add_role_request
        )

    @classmethod
    def provision_workspace_identity(cls, workspace_id, workspace_role = 'Admin'):
        """Provision Workspace Identity"""        
        workspace_identity = cls.fabric_client.core.workspaces.provision_identity(workspace_id=workspace_id)        
        service_principal_id = workspace_identity.service_principal_id
        cls.add_workspace_role_assignment_for_spn(workspace_id, service_principal_id, workspace_role)

    @classmethod
    def workspace_has_provisioned_identity(cls, workspace_id):
        """Check if Workspace has a provisioned identity"""
        workspace_info = cls.get_workspace_info(workspace_id)
        return (workspace_info.workspace_identity is not None)

    @classmethod
    def deprovision_workspace_identity(cls, workspace_id):
        """Deprovision Workspace Identity"""        
        cls.fabric_client.core.workspaces.deprovision_identity(workspace_id=workspace_id)        

    @classmethod
    def delete_workspace(cls, workspace_id):
        """Delete Workspace"""

        # cascade delete workspace-specific connections        
        connections = cls.list_connections()
        for connection in connections:
            if (connection.display_name is not None) and (workspace_id in connection.display_name):
                cls.delete_connection(connection.id)
        
        # delete workspace
        cls.fabric_client.core.workspaces.delete_workspace(workspace_id)

    #endregion
    
    #region connection functions

    @classmethod
    def get_gatewys(cls):
        """Get Gateways"""
        return cls.fabric_client.core.gateways.list_gateways()

    @classmethod
    def list_connections(cls):
        """List all connections accessible to caller"""
        return cls.fabric_client.core.connections.list_connections()

    @classmethod
    def get_connection_by_name(cls, display_name):
        """Get Connection By Name"""
        connections = cls.list_connections()
        for connection in connections:
            if connection.display_name == display_name:
                return connection
        return None

    @classmethod
    def display_connections(cls):
        """Display all connections accessible to caller"""
        connections = cls.list_connections()
        AppLogger.log_step("Connections:")
        for conn in connections:
            AppLogger.log_substep(f" - {conn.display_name}, ID: {conn.id}")

    @classmethod
    def list_supported_connection_types(cls):
        """list all connection types supported by Fabric"""
        return cls.fabric_client.core.connections.list_supported_connection_types()

    @classmethod
    def get_connection_type_metadata(cls, type_name):
        """list all connection types supported by Fabric"""
        supported_types = cls.fabric_client.core.connections.list_supported_connection_types()
        for conn_type in supported_types:
            if conn_type.type == type_name:
                return conn_type
            
        return None

    @classmethod
    def display_supported_connection_types(cls):
        """Display all connection types supported by Fabric"""
        connection_types = cls.list_supported_connection_types()
        AppLogger.log_step("Supported Connection Types:")        
        for conn_type in connection_types:
            AppLogger.log_substep(f"{conn_type.type}")

    @classmethod
    def display_connection_type_metadata(cls, type_name):
        """list all connection types supported by Fabric"""
        target_type = cls.get_connection_type_metadata(type_name)
        AppLogger.log_step(json.dumps(target_type.as_dict(), indent=4))

    @classmethod
    def create_connection(cls, create_connection_request):
        """ Create new connection"""
        AppLogger.log_substep(f"Creating connection {create_connection_request['displayName']} ...")

        existing_connections = cls.list_connections()
        for connection in existing_connections:
            if connection.display_name is not None and connection.display_name  == create_connection_request['displayName']:
                AppLogger.log_substep(f"Using existing Connection with id [{connection.id}]")
                return connection

        connection = cls.fabric_client.core.connections.create_connection(
            create_connection_request=create_connection_request
        )

        AppLogger.log_substep(f"Connection created with id [{connection.id}]")

        # add admin user as co-owner of connection
        cls.add_connection_role_assignment_for_user(
            connection.id,
            cls.ADMIN_USER_ID,
            'Owner'
        )
     
        return connection

    @classmethod
    def delete_connection(cls, connection_id):
        """delete connections"""
        cls.fabric_client.core.connections.delete_connection(connection_id)

    @classmethod
    def add_connection_role_assignment_for_user(cls, connection_id, admin_user_id, connection_role):
        """Add connection role assignment for user"""
        
        assignment_request = {
            'principal': {
                'id': admin_user_id,
                'type': "User"
            },
            'role': connection_role
        }
        
        return cls.fabric_client.core.connections.add_connection_role_assignment(
            connection_id=connection_id,
            add_connection_role_assignment_request=assignment_request
        )

    @classmethod
    def create_anonymous_web_connection(cls, web_url):
        """Create new Web connection using Anonymous credentials"""
        display_name = f'Web-Anonymous-[{web_url}]'

        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'Web',
                'creationMethod': 'Web',
                'parameters': [ 
                    {
                        'value': web_url,
                        'dataType': 'Text',
                        'name': 'url'
                    }
                ]
            },
            'credentialDetails': {
                'credentials': {
                    'credentialType': 'Anonymous' 
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)

    @classmethod
    def create_azure_storage_connection_with_sas_token(cls, server, path):
        """Create Azure Storage connections"""
        display_name = ""
        
        display_name = f"ADLS-SAS-[{server}{path}]"
        
        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'AzureDataLakeStorage',
                'creationMethod': 'AzureDataLakeStorage',
                'parameters': [ 
                    { 'value': server, 'dataType': 'Text', 'name': 'server' },
                    { 'value': path, 'dataType': 'Text', 'name': 'path' }
                ]
            },
            'credentialDetails': {
                'credentials': {
                    'token': cls.AZURE_STORAGE_SAS_TOKEN,
                    'credentialType': 'SharedAccessSignature'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)

    @classmethod
    def create_sql_connection_with_service_principal(cls, server, database,
                                                          workspace = None, lakehouse = None):
        """Create SQL Connection"""
        display_name = ''
        if workspace is not None:
            display_name = f"Workspace[{workspace.id}]-" + display_name
            if lakehouse is not None:
                display_name = display_name + f"Lakehouse[{lakehouse.display_name}]-SqlEndpoint"
            else:
                display_name = display_name + 'SQL'
        else:
            display_name = f'SQL-{server}:{database}'

        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'SQL',
                'creationMethod': 'Sql',
                'parameters': [ 
                    { 'value': server, 'dataType': 'Text', 'name': 'server' },
                    { 'value': database, 'dataType': 'Text', 'name': 'database' }
                ]
            },
            'credentialDetails': {
                'credentials': {
                    'tenantId': cls.AZURE_TENANT_ID,
                    'servicePrincipalClientId': cls.AZURE_CLIENT_ID,
                    'servicePrincipalSecret': cls.AZURE_CLIENT_SECRET,
                    'credentialType': 'ServicePrincipal'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)

    @classmethod
    def create_azure_storage_connection_with_service_principal(cls, server, path,
                                                                workspace = None, lakehouse = None):
        """Create Azure Storage connections"""

        display_name = ''
        if workspace is not None:
            display_name = f"Workspace[{workspace.id}]-" + display_name
            if lakehouse is not None:
                display_name = display_name + f"Lakehouse[{lakehouse.display_name}]-Onelake"
        else:
            display_name = f'ADLS-SPN-{server}:{path}'

        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'AzureDataLakeStorage',
                'creationMethod': 'AzureDataLakeStorage',
                'parameters': [ 
                    { 'value': server, 'dataType': 'Text', 'name': 'server' },
                    { 'value': path, 'dataType': 'Text', 'name': 'path' }
                ]
            },
            'credentialDetails': {
                'credentials': {
                    'tenantId': cls.AZURE_TENANT_ID,
                    'servicePrincipalClientId': cls.AZURE_CLIENT_ID,
                    'servicePrincipalSecret': cls.AZURE_CLIENT_SECRET,
                    'credentialType': 'ServicePrincipal'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)

    @classmethod
    def list_workspace_connections(cls, workspace_id):
        """Get connections associated with a specifc workspace"""
        workspace_connections = []
        connections = cls.list_connections()
        for connection in connections:
            if connection.displayName is not None and workspace_id in connection.displayName:
                workspace_connections.append(connection)
        return workspace_connections

    #endregion
    
    #region folder functions
    
    @classmethod
    def list_folders(cls, workspace_id):
        """List Folder"""
        return cls.fabric_client.core.folders.list_folders(workspace_id=workspace_id)

    @classmethod
    def create_folder(cls, workspace_id, folder_name, paremt_folder_id = None):
        """Create Folder"""

        AppLogger.log_step(f'Creating folder {folder_name}')
        
        create_request = {
            'displayName': folder_name,
            'parentFolderId': paremt_folder_id
         }

        folder = cls.fabric_client.core.folders.create_folder(workspace_id, create_request)
        AppLogger.log_substep(f"folder created with Id [{folder.id}]")
        
        return folder

    #endregion
    
    #region item functions

    @classmethod
    def list_workspace_items(cls, workspace_id, item_type = None):
        """Get items in workspace"""
        return cls.fabric_client.core.items.list_items(workspace_id, type=item_type)

    @classmethod
    def get_item_by_name(cls, workspace_id, display_name, item_type):
        """Get Item by Name"""
        items = cls.list_workspace_items(workspace_id, item_type)
        for item in items:
            if item.display_name == display_name:
                return item
        return None

    @classmethod
    def create_item_no_sdk(cls, workspace_id, create_item_request, folder_id = None):
        """Create Item using create-item-request"""
        if folder_id is not None:
            create_item_request['folderId'] = folder_id

        AppLogger.log_step(
            f"Creating [{create_item_request['displayName']}.{create_item_request['type']}]...")
        endpoint = f'workspaces/{workspace_id}/items'
        item = cls._execute_post_request(endpoint, create_item_request)
        AppLogger.log_substep(f"{item['type']} created with id [{item['id']}]")
        return item

    @classmethod
    def create_item(cls, workspace_id, create_item_request, folder_id = None):
        """Create Item"""

        AppLogger.log_step(
            f"Creating [{create_item_request['displayName']}.{create_item_request['type']}]...")
        
        if folder_id is not None:
            create_item_request['folderId'] = folder_id
        
        item = cls.fabric_client.core.items.create_item(workspace_id, create_item_request)
        AppLogger.log_substep(f"{item.type} created with id [{item.id}]")

        return item

    @classmethod
    def get_item_definition(cls, workspace_id, item, export_format = None):
        """Get Item Definition"""
        # no ability to pass format parameter
        return cls.fabric_client.core.items.get_item_definition(workspace_id, item.id)

    @classmethod
    def update_item_definition(cls, workspace_id, item, update_item_definition_request):
        """Update Item Definition using update-item--definition-request"""
        return cls.fabric_client.core.items.update_item_definition(
            workspace_id,
            item.id,
            update_item_definition_request
        )


    #endregion

    #region variable library functions

    @classmethod
    def set_active_valueset_for_variable_library(cls, workspace_id, library, valueset):
        """Set active valueset for variable library"""
        
        # leave default settings for dev environment
        if valueset == 'dev':
            return

        AppLogger.log_substep(
            "Setting active valueset for variable library " + \
            f"[{library.display_name}] to [{valueset}]...")

        update_request = {
            'displayName': library.display_name,
            'properties': {                
                'activeValueSetName': valueset
            }
        }
        
        cls.fabric_client.variablelibrary.items.update_variable_library(
            workspace_id=workspace_id,
            variable_library_id=library.id,
            update_variable_library_request=update_request
        )
 
    @classmethod
    def set_active_valueset_for_variable_library_no_sdk(cls, workspace_id, library, valueset):
        """Set active valueset for variable library"""
        if valueset == 'dev':
            valueset = None
            
        AppLogger.log_substep(
            "Setting active valueset for variable library " + \
            f"[{library.display_name}] to [{valueset}]...")

        rest_url = f"workspaces/{workspace_id}/VariableLibraries/{library.id}"
        post_body = {
            'properties': {                
                'activeValueSetName': valueset
            }
        }
        cls._execute_patch_request(rest_url, post_body)
        
 
    #endregion
 
    #region lakehouse and shortcut functions
 
    @classmethod
    def create_lakehouse(cls, workspace_id, display_name, folder_id = None, enable_schemas = False):
        """Create Lakehouse"""
        create_item_request = {
            'displayName': display_name, 
            'type': 'Lakehouse' 
        }

        if enable_schemas:
            create_item_request['creationPayload'] = { 'enableSchemas': True }
            
        return cls.create_item(workspace_id, create_item_request, folder_id)

    @classmethod
    def get_lakehouse(cls, workspace_id, lakehouse_id):
        """Get lakehouse properties"""
        rest_url = f'workspaces/{workspace_id}/lakehouses/{lakehouse_id}'
        return cls.fabric_client.lakehouse.items.get_lakehouse(
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id
        )

    @classmethod
    def get_sql_endpoint_for_lakehouse(cls, workspace_id, lakehouse):
        """Get SQL endpoint properties for lakehouse"""

        lakehouse = cls.get_lakehouse(workspace_id, lakehouse.id)
        while lakehouse.properties.sql_endpoint_properties.provisioning_status != 'Success':
            wait_time = 10
            time.sleep(wait_time)
            lakehouse = cls.get_lakehouse(workspace_id, lakehouse.id)

        server = lakehouse.properties.sql_endpoint_properties.connection_string
        database = lakehouse.properties.sql_endpoint_properties.id

        return {
            'server': server,
            'database': database
        }

    @classmethod
    def refresh_sql_endpoint_metadata(cls, workspace_id, sql_endpoint_id):
        """Refresh SL Endpoint"""
        AppLogger.log_substep("Updating SQL Endpoint metadata...")
        cls.fabric_client.sqlendpoint.items.refresh_sql_endpoint_metadata(
                        workspace_id=workspace_id,
                        sql_endpoint_id=sql_endpoint_id,
                        sql_endpoint_refresh_metadata_request={}
                    ).value
            
        AppLogger.log_substep(f"SQL Endpoint metadata refresh job completed successfully")

    @classmethod
    def list_shortcuts(cls, workspace_id, lakehouse_id):
        """List Shortcuts"""
        return cls.fabric_client.core.one_lake_shortcuts.list_shortcuts(workspace_id, lakehouse_id)

    @classmethod
    def create_onelake_shortcut(cls, 
                                workspace_id, 
                                target_lakehouse_id,
                                source_lakehouse_id,
                                name, 
                                path,
                                ):
        """Create ADLS Gen2 Shortcut"""
        AppLogger.log_step('Creating OneLake shortcut using ADLS connection...')
        create_request = {
            'name': name,
            'path': f'/{path}',
            'target': {
                'onelake': {
                    'itemId': source_lakehouse_id,
                    'path': f'{path}/{name}',
                    'workspaceId': workspace_id
                }
            }
        }
        
        cls.fabric_client.core.one_lake_shortcuts.create_shortcut(
            workspace_id, 
            target_lakehouse_id,
            create_request
        )
                
        AppLogger.log_substep(f'Shortcut [{path}/{name}] successfullly created')
    
    @classmethod
    def reset_shortcut_cache(cls, workspace_id):
        """Reset Shortcut Cache"""
        AppLogger.log_substep("Resetting shortcut cache...")
        reset_operartion = cls.fabric_client.core.one_lake_shortcuts.begin_reset_shortcut_cache(workspace_id)
        reset_operartion.wait()
        AppLogger.log_substep("Shortcut cache reset successfully")
       
    @classmethod
    def create_adls_gen2_shortcut(cls, workspace_id, lakehouse_id, name, path,
                                    location, subpath, connection_id):
        """Create ADLS Gen2 Shortcut"""
        AppLogger.log_substep(f'Creating ADLS Gen2 shortcut [{path}/{name}] using ADLS connection...')
        create_request = {
            'name': name,
            'path': path,
            'target': {
                'adlsGen2': {
                'location': location,
                'subpath': subpath,
                'connectionId': connection_id
                }
            }
        }
        cls.fabric_client.core.one_lake_shortcuts.create_shortcut(
            workspace_id, 
            lakehouse_id,
            create_request,
            shortcut_conflict_policy='CreateOrOverwrite'
        )

    @classmethod
    def reset_adls_gen2_shortcut(cls, workspace_id, lakehouse_id, shortcut):
        """Reset ADLS Gen2 Shortcut"""
        cls.create_adls_gen2_shortcut(
            workspace_id,
            lakehouse_id,
            shortcut.name,
            shortcut.path,
            "$(/**/environment_settings/adls_server)",
            "$(/**/environment_settings/adls_shortcut_subpath)",
            "$(/**/environment_settings/adls_connection_id)"
        )
 
    #endregion

    #region job functions

    @classmethod
    def run_notebook_with_sdk(cls, workspace_id, notebook):
        """Run notebook and wait for job completion"""
        AppLogger.log_substep(f"Running notebook [{notebook.display_name}]...")

        response = cls.fabric_client.core.job_scheduler.run_on_demand_item_job(
            workspace_id=workspace_id,
            item_id=notebook.id,
            job_type='RunNotebook'            
        )

        AppLogger.log_substep("Notebook run job completed successfully")
        return response


    @classmethod
    def run_data_pipeline_with_sdk(cls, workspace_id, pipeline):
        """Run pipeline and wait for job completion"""
        AppLogger.log_substep(f"Running data pipeline [{pipeline.display_name}]...")
        
        response = cls.fabric_client.core.job_scheduler.run_on_demand_item_job(
            workspace_id=workspace_id,
            item_id=pipeline.id,
            job_type='Pipeline'
        )
        
        AppLogger.log_substep("Data pipeline run job completed successfully")
        return response

    #endregion
 
    #region semantic model functions
 
    @classmethod
    def bind_semantic_model_to_connection(cls, workspace_id, semantic_model_id, connection_id):
        """Bind semantic model to connection"""
        # Fabric REST API does not yet support binding semantic models to connections, so using Power BI REST API for this operation
        PowerBiRestApi.bind_semantic_model_to_connection(workspace_id, semantic_model_id, connection_id)

    @classmethod
    def refresh_semantic_model(cls, workspace_id, semantic_model_id):
        """Refresh semantic model"""
        # Fabric REST API does not yet support refreshing semantic models, so using Power BI REST API for this operation
        PowerBiRestApi.refresh_semantic_model(workspace_id, semantic_model_id)

    @classmethod
    def list_datasources_for_semantic_model(cls, workspace_id, semantic_model_id):
        """Get Datasource for semantic model using Power BI REST API"""
        # Fabric REST API does not yet support listing datasources for semantic models, so using Power BI REST API for this operation
        return PowerBiRestApi.list_datasources_for_semantic_model(workspace_id, semantic_model_id)

    @classmethod
    def get_sql_endpoint_from_semantic_model(cls, workspace_id, semantic_model_id):
        """Get Web datasource URL from semantic model"""
        data_sources = cls.list_datasources_for_semantic_model(workspace_id, semantic_model_id)
        for data_source in data_sources:
            if data_source['datasourceType'] == 'Sql':
                return data_source['connectionDetails']
        return None

    @classmethod
    def create_and_bind_semantic_model_connecton(cls, workspace,
                                                 semantic_model_id, lakehouse = None):
        """Create connection and bind it to semantic model"""
        datasources = PowerBiRestApi.list_datasources_for_semantic_model(workspace.id, semantic_model_id)
        for datasource in datasources:

            if datasource['datasourceType'].lower() == 'sql':
                AppLogger.log_substep('Creating SQL connection for semantic model')
                server = datasource['connectionDetails']['server']
                database = datasource['connectionDetails']['database']
                connection = cls.create_sql_connection_with_service_principal(server,
                                                                              database,
                                                                              workspace,
                                                                              lakehouse)
                AppLogger.log_substep('Binding semantic model to SQL connection')
                cls.bind_semantic_model_to_connection(workspace.id,
                                                        semantic_model_id,
                                                        connection.id)

            elif datasource['datasourceType'].lower() == 'web':
                AppLogger.log_substep('Creating Web connection for semantic model')
                web_url    = datasource['connectionDetails']['url']
                connection = cls.create_anonymous_web_connection(web_url)
                AppLogger.log_substep('Binding semantic model to Web connection')
                cls.bind_semantic_model_to_connection(workspace.id,
                                                        semantic_model_id,
                                                         connection.id)
                cls.refresh_semantic_model(workspace.id, semantic_model_id)

            elif datasource['datasourceType'] == 'AzureDataLakeStorage':
                AppLogger.log_substep('Creating AzureDataLakeStorage connection for semantic model')
                server    = datasource['connectionDetails']['server']
                path      = datasource['connectionDetails']['path']
                connection = cls.create_azure_storage_connection_with_sas_token(server, path)
                AppLogger.log_substep('Binding semantic model to Azure Gen2 storage')
                cls.bind_semantic_model_to_connection(workspace.id, semantic_model_id, connection.id)
                cls.refresh_semantic_model(workspace.id, semantic_model_id)

    #endregion
    
    #region Batch import/export support
    
    @classmethod
    def export_item_definitions(cls, workspace_id):
        """Get Item Definition"""
        endpoint = f"workspaces/{workspace_id}/exportItemDefinitions?beta=true"
        post_body    = {
            'mode': 'All'
        }
        return cls._execute_post_request(endpoint, post_body)
    
    #endregion
    
    #region deployment pipelines
          
    @classmethod
    def list_deployment_pipelines(cls):
        """Get all deployment pipelines accessible to caller"""
        return cls.fabric_client.core.deployment_pipelines.list_deployment_pipelines()

    @classmethod
    def get_deployment_pipeline_by_name(cls, display_name):
        """Get Deployment Pipeline item by display name"""
        pipelines = cls.list_deployment_pipelines()
        for pipeline in pipelines:
            if pipeline.display_name == display_name:
                return pipeline
        return None

    @classmethod
    def list_deployment_pipeline_stages(cls, pipeline_id):
        """List all deployment pipeline stages"""
        return cls.fabric_client.core.deployment_pipelines.list_deployment_pipeline_stages(pipeline_id)

    @classmethod
    def delete_deployment_pipeline(cls, pipeline_id):
        """Delete Deployment Pipeline"""
        cls.fabric_client.core.deployment_pipelines.delete_deployment_pipeline(pipeline_id)

    @classmethod
    def display_deployment_pipelines(cls):
        """Display Deployment Pipeline"""
        AppLogger.log_step('Deployment Pipelines:')
        pipelines = cls.list_deployment_pipelines()
        for pipeline in pipelines:
            AppLogger.log_substep(f"{pipeline.id} - {pipeline.display_name}")

    @classmethod
    def create_deployment_pipeline(cls, display_name, stages):
        """Create Deployment Pipeline"""
        AppLogger.log_step(f'Creating Deployment Pipeline [{display_name}]')
        pipeline_stages = []
        for stage in stages:
            pipeline_stages.append({
                'displayName': stage,
                'description': f'stage for {stage}',
                'isPublic': False
            })
        create_request = {
            'displayName': display_name,
            'description': 'great example',
            'stages': pipeline_stages
        }
        
        pipeline = cls.fabric_client.core.deployment_pipelines.create_deployment_pipeline(create_request)
        
        AppLogger.log_substep(f"Pipeline create with id [{pipeline.id}]")

        AppLogger.log_substep('Adding deployment pipeline role of [Admin] for admin user')
        cls.add_deployment_pipeline_role_assignment(pipeline.id,
                                                    cls.ADMIN_USER_ID,
                                                    'User',
                                                    'Admin')

        return pipeline

    @classmethod
    def add_deployment_pipeline_role_assignment(cls, pipeline_id, principal_id, principal_type, role):
        """Add Deployment Pipeline Role Assignment"""
        add_request = {
            'principal': {
                'id': principal_id,
                'type': principal_type
            },
            'role': role
        }
        return cls.fabric_client.core.deployment_pipelines.add_deployment_pipeline_role_assignment(
            pipeline_id, 
            add_request
        )

    @classmethod
    def assign_workpace_to_pipeline_stage(cls, workspace_id, pipeline_id, stage_id):
        """Assign workspace to pipeline stage """
        assign_request = { 'workspaceId': workspace_id }
        cls.fabric_client.core.deployment_pipelines.assign_workspace_to_stage(
            pipeline_id,
            stage_id,
            assign_request
        )

    @classmethod
    def unassign_workpace_from_pipeline_stage(cls, pipeline_id, stage_id):
        """Assign workspace to pipeline stage"""
        cls.fabric_client.core.deployment_pipelines.unassign_workspace_from_stage(
            pipeline_id,
            stage_id
        )

    @classmethod
    def deploy_to_pipeline_stage_with_sdk(cls, pipeline_id, source_stage_id, target_stage_id, note = None):
        """Deploy to pipeline stage"""
        deploy_request = {
            'sourceStageId': source_stage_id,
            'targetStageId': target_stage_id
        }
        if note is not None:
            deploy_request['note'] = note
        else:
            deploy_request['note'] = 'Demo of automating deployment using APIs'

        AppLogger.log_substep('Calling deploy_stage_content')
        cls.fabric_client.core.deployment_pipelines.deploy_stage_content(
            pipeline_id,
            deploy_request
        )
        

    @classmethod
    def deploy_to_pipeline_stage(cls, pipeline_id, source_stage_id, target_stage_id,    note = None):
        """Deploy to pipeline stage"""
        endpoint = f'deploymentPipelines/{pipeline_id}/deploy'
        deploy_request = {
            'sourceStageId': source_stage_id,
            'targetStageId': target_stage_id
        }
        if note is not None:
            deploy_request['note'] = note
        else:
            deploy_request['note'] = 'Demo of automating deployment using APIs'

        AppLogger.log_substep('Calling deploy_stage_content')
        # cls.fabric_client.core.deployment_pipelines.deploy_stage_content(
        #     pipeline_id,
        #     deploy_request
        # )
        
        cls._execute_post_request(endpoint, deploy_request)
    
    #endregion
    
    #region generic Git integration functions

    @classmethod
    def initialize_git_connection(cls, workspace_id, initialize_connection_request):
        """Initialize GIT Connection"""
        return cls.fabric_client.core.git.initialize_connection(
            workspace_id=workspace_id,
            git_initialize_connection_request=initialize_connection_request            
        )

    @classmethod
    def get_git_status(cls, workspace_id):
        """Get GIT Connection Status"""
        return cls.fabric_client.core.git.get_status(workspace_id=workspace_id)

    @classmethod
    def get_git_connection(cls, workspace_id):
        """Get GIT Connection"""
        return cls.fabric_client.core.git.get_connection(workspace_id=workspace_id)

    @classmethod
    def disconnect_workspace_from_git(cls, workspace_id):
        """Disconnect Workspace from GIT Repository"""
        return cls.fabric_client.core.git.disconnect(workspace_id)

    @classmethod
    def commit_workspace_to_git(cls, workspace_id, commit_to_git_request = None, commit_message = "commit workspace changes back to repo"):
        """Commit Workspace to GIT Repository"""
         
        AppLogger.log_substep("Committing Workspace content to GIT repository")
                
        if commit_to_git_request is None:
            currest_status = cls.get_git_status(workspace_id)
            changes = currest_status.changes
            if len(changes) == 0:
                AppLogger.log_substep("There are no changes to push")
                return None
            commit_to_git_request = {
                    'mode': 'All',
                    'workspaceHead': currest_status.workspace_head,
                    'remoteCommitHash': currest_status.remote_commit_hash,
                    'comment': commit_message
            }
            
        cls.fabric_client.core.git.commit_to_git(
            workspace_id, 
            commit_to_git_request)
        
        AppLogger.log_substep('GIT sync process completed successfully')

    @classmethod
    def update_workspace_from_git(cls, workspace_id, update_from_git_request):
        """Update Workspace from GIT Repository"""
        AppLogger.log_substep("Committing GIT repsitory content to workspace items")
        return cls.fabric_client.core.git.update_from_git(workspace_id, update_from_git_request)

    @classmethod
    def get_my_git_credentials(cls, workspace_id):
        """Get My GIT Credential"""
        return cls.fabric_client.core.git.get_my_git_credentials(workspace_id)

    @classmethod
    def update_my_git_credentials(cls, workspace_id, update_git_credentials_request):
        """Update My GIT Credentials"""
        endpoint = f"workspaces/{workspace_id}/git/myGitCredentials"
        return cls.fabric_client.core.git.update_my_git_credentials(
            workspace_id,
            update_git_credentials_request
        )    
    
    #endregion
    
    #region Azure Dev Ops specific functions

    @classmethod
    def _create_ado_source_control_connection(cls, url):
        """Create ADO repo connections with SPN"""

        display_name = f"GIT-ADO-SPN-[{url}]"

        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'AzureDevOpsSourceControl',
                'creationMethod': 'AzureDevOpsSourceControl.Contents',
                'parameters': [ 
                    { 'name': 'url', 'dataType': 'Text', 'value': url }
                ]
            },
             'credentialDetails': {
                'credentials': {
                    'tenantId': cls.AZURE_TENANT_ID,
                    'servicePrincipalClientId': cls.AZURE_CLIENT_ID,
                    'servicePrincipalSecret': cls.AZURE_CLIENT_SECRET,
                    'credentialType': 'ServicePrincipal'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)
    
    @classmethod
    def _get_ado_repo_connection(cls, project_name):
        """Get Azure DevOps Repo Connection"""
        
        ado_repo_url = f'https://dev.azure.com/{cls.ADO_ORGANIZATION}/{project_name}/_git/{project_name}/'

        connections = FabricRestApi.list_connections()

        for connection in connections:
            if connection.connection_details.type == 'AzureDevOpsSourceControl' and \
                 connection.connection_details.path == ado_repo_url:
                return connection

        return FabricRestApi._create_ado_source_control_connection(ado_repo_url)

    @classmethod
    def _create_workspace_connection_to_ado_repo(cls, workspace_id, project_name, connection_id,
                                                 branch = 'main', git_folder = '/workspace'):
        """Connect Workspace Connection to Azure DevOps Repository"""

        connect_request = {
            "gitProviderDetails": {
                "organizationName": EnvironmentSettings.ADO_ORGANIZATION,
                "projectName": project_name,
                "gitProviderType": "AzureDevOps",
                "repositoryName": project_name,
                "branchName": branch,
                "directoryName": git_folder
            },
            "myGitCredentials": {
                "source": "ConfiguredConnection",
                "connectionId": connection_id            
            }
        }

        return cls.fabric_client.core.git.connect(workspace_id, connect_request)

    @classmethod
    def connect_workspace_to_ado_repo(cls, workspace, project_name, branch = 'main', git_folder = '/workspace'):
        """Connect Workspace to Azure Dev Ops Repository"""
        AppLogger.log_substep(f"Connecting workspace [{workspace.display_name}] " + \
                                f"to branch[{branch}] in Azure DevOps repo[{project_name}]")

        connection = cls._get_ado_repo_connection(project_name)
        connection_id = connection.id

        cls._create_workspace_connection_to_ado_repo(workspace.id, project_name, connection_id, branch, git_folder)
        
        AppLogger.log_substep("Workspace connection created successfully")

        init_request = {
            'initializationStrategy': 'PreferWorkspace'
        }

        init_response = cls.initialize_git_connection(workspace.id, init_request)

        required_action = init_response.required_action
    
        if required_action == 'CommitToGit':
            commit_to_git_request = {
                'mode': 'All',
                'workspaceHead': init_response.workspace_head,
                'comment': 'Initial commit from workspace'
            }
            cls.commit_workspace_to_git(
                workspace.id, 
                commit_to_git_request,
                'Initial commit of workspace items to GIT')

        if required_action == 'UpdateFromGit':
            update_from_git_request = {
                "workspaceHead": init_response.workspace_head,
                "remoteCommitHash": init_response.remote_commit_hash,
                "conflictResolution": {
                    "conflictResolutionType": "Workspace",
                    "conflictResolutionPolicy": "PreferWorkspace"
                },
                "options": {
                    "allowOverrideItems": True
                }
                            
            }
            cls.update_workspace_from_git(workspace.id, update_from_git_request)

        AppLogger.log_substep("Workspace connection successfully created and synchronized")

    #endregion

    #region GitHub specific functions
    
    @classmethod
    def _create_github_source_control_connection(cls, url):
        """Create GitHub connections with Personal Access Token"""

        display_name = f"GIT-GitHub-PAT-[{url}]"

        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'GitHubSourceControl',
                'creationMethod': 'GitHubSourceControl.Contents',
                'parameters': [ 
                    { 'name': 'url', 'dataType': 'Text', 'value': url }
                ]
            },
            'credentialDetails': {
                'credentials': {
                    'key': EnvironmentSettings.PERSONAL_ACCESS_TOKEN_GITHUB,
                    'credentialType': 'Key'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)

    @classmethod
    def _get_github_source_control_connection(cls, repo_name):
        """Get GitHub Repo Connection"""
        
        github_org = EnvironmentSettings.ORGANIZATION_GITHUB

        github_repo_url = f'https://github.com/{github_org}/{repo_name}'

        connections = FabricRestApi.list_connections()

        for connection in connections:
            if connection.connection_details.type == 'GitHubSourceControl' and \
                connection.connection_details.path == github_repo_url:
                return connection

        return cls._create_github_source_control_connection(github_repo_url)
    
    @classmethod
    def _create_workspace_connection_to_github_repo(cls, workspace_id, repo_name, connection_id,
                                                    branch = 'main', git_folder = '/workspace'):
        """Connect Workspace to GIT Repository"""

        connect_request = {
            "gitProviderDetails": {
                "ownerName": EnvironmentSettings.ORGANIZATION_GITHUB,
                "gitProviderType": "GitHub",
                "repositoryName": repo_name,
                "branchName": branch,
                "directoryName": git_folder
            },
            "myGitCredentials": {
                "source": "ConfiguredConnection",
                "connectionId": connection_id            
            }
        }
        
        return cls.fabric_client.core.git.connect(workspace_id, connect_request)

    @classmethod
    def connect_workspace_to_github_repo(cls, workspace, repo_name, branch = 'main'):
        """Connect Workspace to GitHub Repository"""

        AppLogger.log_substep(f"Connecting workspace [{workspace.display_name}] " + \
                              f"to branch [{branch}] in GitHub repository [{repo_name}]")
               
        connection = cls._get_github_source_control_connection(repo_name)
        connection_id = connection.id

        cls._create_workspace_connection_to_github_repo(workspace.id, repo_name, connection_id, branch)

        AppLogger.log_substep("Workspace connection created successfully")

        init_request = {
            'initializationStrategy': 'PreferWorkspace'
        }

        init_response = cls.initialize_git_connection(workspace.id, init_request)

        required_action = init_response.required_action
        if required_action == 'CommitToGit':
            commit_to_git_request = {
                'mode': 'All',
                'workspaceHead': init_response.workspace_head,
                'comment': 'Initial commit'
            }
            cls.commit_workspace_to_git(
                workspace.id, 
                commit_to_git_request,
                'Initial commt from workspace')

        if required_action == 'UpdateFromGit':
            update_from_git_request = {
                "workspaceHead": init_response.workspace_head,
                "remoteCommitHash": init_response.remote_commit_hash,
                "conflictResolution": {
                    "conflictResolutionType": "Workspace",
                    "conflictResolutionPolicy": "PreferWorkspace"
                },
                "options": {
                    "allowOverrideItems": True
                }
                            
            }
            cls.update_workspace_from_git(workspace.id, update_from_git_request)

        AppLogger.log_substep("Workspace connection successfully created and synchronized")

    #endregion

    #region  direct calls to Fabric REST API for gaps and bugs where the SDK does not work
    
    @classmethod
    def run_notebook(cls, workspace_id, notebook):
        """Run notebook and wait for job completion"""
        AppLogger.log_substep(f"Running notebook [{notebook.display_name}]...")
        rest_url = f"workspaces/{workspace_id}/items/{notebook.id}" + \
                    "/jobs/instances?jobType=RunNotebook"
        response = cls._execute_post_request_for_job_scheduler(rest_url)
        AppLogger.log_substep("Notebook run job completed successfully")
        return response

    @classmethod
    def run_data_pipeline(cls, workspace_id, pipeline):
        """Run notebook and wait for job completion"""
        AppLogger.log_substep(f"Running data pipeline [{pipeline.display_name}]...")

        rest_url = f"workspaces/{workspace_id}/items/{pipeline.id}" + \
                    "/jobs/instances?jobType=Pipeline"
        response = cls._execute_post_request_for_job_scheduler(rest_url)
        AppLogger.log_substep("Data pipeline run job completed successfully")
        return response

    @classmethod
    def refresh_sql_endpoint_metadata_no_sdk(cls, workspace_id, sql_endpoint_id):
        """Refresh SL Endpoint"""
        AppLogger.log_substep("Updating SQL Endpoint metadata (NO SDK)...")
        endpoint = \
            f"workspaces/{workspace_id}/sqlEndpoints/{sql_endpoint_id}/refreshMetadata?preview=True"
        cls._execute_post_request(endpoint, {})        

    @classmethod
    def _execute_post_request(cls, endpoint, post_body=''):
        """Execute POST request with support for Long-running Operations (LRO)"""
        rest_url = 'https://api.fabric.microsoft.com/v1/' + endpoint        
        fabric_rest_api_scope = 'https://api.fabric.microsoft.com/.default'
        access_token = cls.credential.get_token(fabric_rest_api_scope).token
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}

        response = requests.post(url=rest_url, json=post_body, headers=request_headers, timeout=60)

        if response.status_code in { 200, 201, 204 }:
            try:
                return response.json()
            except JSONDecodeError:
                return None

        if response.status_code == 202:
            operation_state_url = response.headers.get('Location')
            wait_time = 10 # int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            response = requests.get(url=operation_state_url, headers=request_headers, timeout=60)
            operation_state = response.json()
            while operation_state['status'] != 'Succeeded' and \
                    operation_state['status'] != 'Failed':
                time.sleep(wait_time)
                response = requests.get(url=operation_state_url,
                                        headers=request_headers,
                                        timeout=60)
                operation_state = response.json()

            if operation_state['status'] == 'Succeeded':
                if 'Location' in response.headers:
                    operation_result_url = response.headers.get('Location')
                    response = requests.get(url=operation_result_url,
                                            headers=request_headers,
                                            timeout=60)
                    if response.status_code == 200:
                        return response.json()
                    else:
                        AppLogger.log_error(f"Error - {response.status_code}")
                        return None
                else:
                    return None
            else:
                AppLogger.log_error(f"Error - {operation_state}")

        elif response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            return cls._execute_post_request(endpoint, post_body)
     
        elif response.status_code == 404: # handle NOT FOUND error
            AppLogger.log_substep("Handling 404 by waiting and trying again")
            print( response.content )
            time.sleep(10)
            response = requests.post(url=rest_url, json=post_body, headers=request_headers, timeout=60)
            
            if response.status_code in { 200, 201, 204 }:
                AppLogger.log_substep("POST call succeeded on second attempt")
                try:
                    return response.json()
                except JSONDecodeError:
                    return None
            else:
                print('Error - 404 NOT FOUND')
                print( response )
                AppLogger.log_error(f"Error -  {response}")
                raise RuntimeError("Error - ")
        
        else:
            AppLogger.log_error(
                f'Error executing POST request: {response.status_code} - {response.text}')
            raise RuntimeError(f'Error executing POST request: {response.status_code} - {response.text}')

    @classmethod
    def _execute_patch_request(cls, endpoint, post_body):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = 'https://api.fabric.microsoft.com/v1/' + endpoint        
        fabric_rest_api_scope = 'https://api.fabric.microsoft.com/.default'
        access_token = cls.credential.get_token(fabric_rest_api_scope).token
        request_headers = {'Content-Type':'application/json',
                           'Authorization': f'Bearer {access_token}'}
        response = requests.patch(url=rest_url, json=post_body, headers=request_headers, timeout=60)
        if response.status_code in {200, 204}:
            return response.json()

        if response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            return cls._execute_patch_request(endpoint, post_body)
        else:
            AppLogger.log_error(
                f'Error executing PATCH request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_post_request_for_job_scheduler(cls, endpoint, post_body=''):
        """Execute POST request with support for Om-demand Job with Job Scheduler"""
        rest_url = 'https://api.fabric.microsoft.com/v1/' + endpoint        
        fabric_rest_api_scope = 'https://api.fabric.microsoft.com/.default'
        access_token = cls.credential.get_token(fabric_rest_api_scope).token
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}
        response = requests.post(url=rest_url, headers=request_headers, json=post_body, timeout=60)

        if response.status_code == 202:
            operation_state_url = response.headers.get('Location')
            wait_time = 10 # int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            response = requests.get(url=operation_state_url, headers=request_headers, timeout=60)
            operation_state = response.json()
            while operation_state['status'] == 'NotStarted' or \
                    operation_state['status'] == 'InProgress' or \
                    (operation_state['status'] == 'Failed' and \
                     operation_state['failureReason']['errorCode'] == 'RequestExecutionFailed'    ):
                time.sleep(wait_time)
                response = requests.get(url=operation_state_url,
                                        headers=request_headers,
                                        timeout=60)
                operation_state = response.json()

            if operation_state['status'] == 'Completed':
                return

            if operation_state['status'] == 'Failed':
                AppLogger.log_error('On-demand job Failed')
                print('----------------------------')
                print(operation_state)
                print('----------------------------')
                print(response)
                print('----------------------------')
                raise RuntimeError('On-demand job started but Failed')

            if operation_state['status'] == 'Cancelled':
                AppLogger.log_error('On-demand job was cancelled')

            if operation_state['status'] == 'Deduped':
                AppLogger.log_error('On-demand job was depuped')

        elif response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            cls._execute_post_request_for_job_scheduler(endpoint, post_body)
        else:
            AppLogger.log_error(
                f'Error executing POST request: {response.status_code} - {response.text}')

    #endregion

class PowerBiRestApi:
    """Power BI REST API Wrapper Class used for operations not yet supported by Fabric REST API"""

    # used for creating DefaultAzureCredential connections with SPN creds
    AZURE_TENANT_ID = os.getenv('AZURE_TENANT_ID')
    AZURE_CLIENT_ID = os.getenv('AZURE_CLIENT_ID')
    AZURE_CLIENT_SECRET = os.getenv('AZURE_CLIENT_SECRET')
    
    credential = DefaultAzureCredential()
    fabric_client = FabricClient(credential)
    
    powerbi_rest_api_scope = 'https://api.fabric.microsoft.com/.default'
    powerbi_rest_api_base_url = 'https://api.powerbi.com/v1.0/myorg/'
    
    @classmethod
    def _execute_get_request_to_powerbi(cls, endpoint):
        """Execute GET Request on Power BI REST API Endpoint"""
        rest_url = cls.powerbi_rest_api_base_url + endpoint
        scope = cls.powerbi_rest_api_scope
        access_token = cls.credential.get_token(scope).token
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}
        response = requests.get(url=rest_url, headers=request_headers, timeout=60)
        if response.status_code in { 200, 202 }:
            return response.json()
        elif response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            AppLogger.log_substep(f'Waiting {wait_time} seconds due to 429 TOO MANY REQUESTS error')
            time.sleep(wait_time)
            return cls._execute_get_request_to_powerbi(endpoint)
        else:
            AppLogger.log_error(
                f'Error executing GET request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_post_request_to_powerbi(cls, endpoint, post_body=''):
        rest_url = cls.powerbi_rest_api_base_url + endpoint
        scope = cls.powerbi_rest_api_scope
        access_token = cls.credential.get_token(scope).token
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}
        response = requests.post(url=rest_url, headers=request_headers, json=post_body, timeout=60)
        return response

    @classmethod
    def get_gateways(cls):
        """get geteways"""
        rest_url    = 'gateways'
        return cls._execute_get_request_to_powerbi(rest_url)['value']


    @classmethod
    def list_datasources_for_semantic_model(cls, workspace_id, semantic_model_id):
        """Get Datasource for semantic model using Power BI REST API"""
        rest_url    = f'groups//{workspace_id}//datasets//{semantic_model_id}//datasources'
        return cls._execute_get_request_to_powerbi(rest_url)['value']

    @classmethod
    def get_adls_dataource_from_semantic_model(cls, workspace_id, semantic_model_id):
        """Get ADLS datasource from semantic model"""
        data_sources = cls.list_datasources_for_semantic_model(workspace_id, semantic_model_id)
        for datasource in data_sources:                        
            if datasource['datasourceType'] == 'AzureDataLakeStorage':
                server    = datasource['connectionDetails']['server']
                path      = datasource['connectionDetails']['path']
                return f'{server}{path}'


    @classmethod
    def get_web_url_from_semantic_model(cls, workspace_id, semantic_model_id):
        """Get Web datasource URL from semantic model"""
        data_sources = cls.list_datasources_for_semantic_model(workspace_id, semantic_model_id)
        for data_source in data_sources:
            if data_source['datasourceType'] == 'Web':
                return data_source['connectionDetails']['url']

    @classmethod
    def bind_semantic_model_to_connection(cls, workspace_id, semantic_model_id, connection_id):
        """Bind semantic model to connection"""
        rest_url    = f'groups//{workspace_id}//datasets//{semantic_model_id}//Default.BindToGateway'
        post_body    = {
            'gatewayObjectId': '00000000-0000-0000-0000-000000000000',
            'datasourceObjectIds': [ connection_id ]
        }
        cls._execute_post_request_to_powerbi(rest_url, post_body)

    @classmethod
    def refresh_semantic_model(cls, workspace_id, semantic_model_id):
        """Refresh semantic model"""
        AppLogger.log_substep('Refreshing semantic model...')
        rest_url    = f'groups//{workspace_id}//datasets//{semantic_model_id}//refreshes'
        post_body = { 'notifyOption': 'NoNotification', 'type': 'Automatic' }
        response = cls._execute_post_request_to_powerbi(rest_url, post_body)
        refresh_id = response.headers.get('x-ms-request-id')
        rest_url_refresh_details = \
            f'groups/{workspace_id}/datasets/{semantic_model_id}/refreshes/{refresh_id}'
        refresh_details = cls._execute_get_request_to_powerbi(rest_url_refresh_details)

        while 'status' not in refresh_details or refresh_details == 'Unknown':
            time.sleep(6)
            refresh_details = cls._execute_get_request_to_powerbi(rest_url_refresh_details)

        AppLogger.log_substep("Refresh operation complete")
