"""Module to manage calls to Fabric REST APIs"""

import time
import datetime
import requests
import msal

from fabric_devops.app_logger import AppLogger
from fabric_devops.app_settings import AppSettings

class FabricRestApi:
    """Wrapper class for calling Fabric REST APIs"""

#region Low-level details about authentication and HTTP requests and responses

    _access_token = None
    _access_token_expiration = None

    @classmethod
    def _get_fabric_access_token(cls):
        """Acquire Entra Id Access Token for calling Fabric REST APIs"""
        if (cls._access_token is None) or (datetime.datetime.now() > cls._access_token_expiration):
            app = msal.ConfidentialClientApplication(
                AppSettings.FABRIC_CLIENT_ID,
                authority=AppSettings.AUTHORITY,
                client_credential=AppSettings.FABRIC_CLIENT_SECRET)

            result = app.acquire_token_for_client(scopes=AppSettings.FABRIC_PERMISSION_SCOPES)
            cls._access_token = result['access_token']
            cls._access_token_expiration = datetime.datetime.now() + \
                                           datetime.timedelta(0,  int(result['expires_in']))
        return cls._access_token

    @classmethod
    def _execute_get_request(cls, endpoint):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = AppSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                           'Authorization': f'Bearer {access_token}'}
        response = requests.get(url=rest_url, headers=request_headers, timeout=60)
        if response.status_code == 200:
            return response.json()
        if response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            AppLogger.log_step(f'Waiting {wait_time} seconds due to 429 TOO MANY REQUESTS error')
            time.sleep(wait_time)
            return cls._execute_get_request(endpoint)
        else:
            AppLogger.log_error(
                f'Error executing GET request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_post_request(cls, endpoint, post_body=''):
        """Execute POST request with support for Long-running Operations (LRO)"""
        rest_url = AppSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                           'Authorization': f'Bearer {access_token}'}
        response = requests.post(url=rest_url, json=post_body, headers=request_headers, timeout=60)

        if response.status_code in { 200, 201 }:
            return response.json()

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
            if 'Location' in response.headers:
                operation_result_url = response.headers.get('Location')
                response = requests.get(url=operation_result_url,
                                        headers=request_headers,
                                        timeout=60)
                if response.status_code == 200:
                    return response.json()
                else:
                    return None
            else:
                return None

        elif response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            return cls._execute_post_request(endpoint, post_body)
        else:
            AppLogger.log_error(
                f'Error executing POST request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_post_request_for_job_scheduler(cls, endpoint, post_body=''):
        """Execute POST request with support for Om-demand Job with Job Scheduler"""

        rest_url = AppSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
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
                  operation_state['status'] == 'InProgress':
                time.sleep(wait_time)
                response = requests.get(url=operation_state_url,
                                        headers=request_headers,
                                        timeout=60)
                operation_state = response.json()

            if operation_state['status'] == 'Completed':
                return

            if operation_state['status'] == 'Failed':
                AppLogger.log_error('On-demand job Failed')

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

    @classmethod
    def _execute_patch_request(cls, endpoint, post_body):
        """Execute GET Request on Fabric REST API Endpoint"""

        rest_url = AppSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                           'Authorization': f'Bearer {access_token}'}
        response = requests.patch(url=rest_url, json=post_body, headers=request_headers, timeout=60)
        if response.status_code == 200:
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
    def _execute_delete_request(cls, endpoint):
        """Execute DELETE Request on Fabric REST API Endpoint"""

        rest_url = AppSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers= {'Content-Type':'application/json',
                          'Authorization': f'Bearer {access_token}'}
        response = requests.delete(url=rest_url, headers=request_headers, timeout=60)
        if response.status_code == 200:
            return None
        if response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            cls._execute_delete_request(endpoint)
            return None
        else:
            AppLogger.log_error(
                f'Error executing DELETE request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_get_request_to_powerbi(cls, endpoint):
        """Execute GET Request on Power BI REST API Endpoint"""

        rest_url = AppSettings.POWER_BI_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
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
        rest_url = AppSettings.POWER_BI_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                           'Authorization': f'Bearer {access_token}'}
        return requests.post(url=rest_url, headers=request_headers, json=post_body, timeout=60)

#endregion

    @classmethod
    def get_workspaces(cls):
        """Get all workspaces accessible to caller"""
        return cls._execute_get_request('workspaces')['value']

    @classmethod
    def display_workspaces(cls):
        """Display all workspaces accessible to caller"""
        AppLogger.log_step('Workspaces:')
        workspaces = cls.get_workspaces()
        for workspace in enumerate(workspaces):
            AppLogger.log_substep(workspace['displayName'])

    @classmethod
    def get_workspace_by_name(cls, display_name):
        """Get Workspace item by display name"""
        workspaces =  cls.get_workspaces()
        for workspace in workspaces:
            if workspace['displayName'] == display_name:
                return workspace
        return None

    @classmethod
    def create_workspace(cls, display_name):
        """Create a new Fabric workspace"""
        AppLogger.log_step(f'Creating workspace [{display_name}]')

        existing_workspace = cls.get_workspace_by_name(display_name)
        if existing_workspace is not None:
            AppLogger.log_substep('Deleting existing workspace with same name')
            cls.delete_workspace(existing_workspace['id'])

        post_body = {
            'displayName': display_name,
            'description': 'a demo workspace',
            'capacityId': AppSettings.FABRIC_CAPACITY_ID
        }

        AppLogger.log_substep('Calling Create-Workspace API...')
        workspace = cls._execute_post_request('workspaces', post_body)
        workspace_id = workspace['id']
        AppLogger.log_substep(f'Workspace created with Id of [{workspace_id}]')

        AppLogger.log_substep('Adding workspace role of [Admin] for admin user')
        cls.add_workspace_user(workspace_id, AppSettings.ADMIN_USER_ID, 'Admin')

        return workspace

    @classmethod
    def delete_workspace(cls, workspace_id):
        """Delete Workspace"""

        workspace_connections  = cls.get_workspace_connections(workspace_id)
        for connection in workspace_connections:
            AppLogger.log_substep(f"Deleting connection {connection['displayName']}")
            cls.delete_connection(connection['id'])

        endpoint = f'workspaces/{workspace_id}'
        cls._execute_delete_request(endpoint)

    @classmethod
    def add_workspace_user(cls, workspace_id, user_id, role_assignment):
        """Add workspace role for user"""
        endpoint =  'workspaces/' + workspace_id + '/roleAssignments'
        post_body = {
          'role': role_assignment,
          'principal': {
            'id': user_id,
            'type': 'User'
          }
        }
        return cls._execute_post_request(endpoint, post_body)

    @classmethod
    def get_connections(cls):
        """Get all connections accessible to caller"""
        return cls._execute_get_request('connections')['value']

    @classmethod
    def display_connections(cls):
        """Get all connections accessible to caller"""
        connections = cls.get_connections()
        AppLogger.log_step('Connections')
        for connection in connections:
            AppLogger.log_substep(
                f"{connection['id']} - " + \
                 "{connection['connectionDetails']['type']} - {connection['displayName']}")            

    @classmethod
    def create_connection(cls, create_connection_request):
        """ Create new connection"""
        AppLogger.log_step(
            f"Creating [{create_connection_request['connectionDetails']['type']}] " + \
            f"connection named {create_connection_request['displayName']}...")

        existing_connections = cls.get_connections()
        for connection in existing_connections:
            if 'displayName' in connection and \
                connection['displayName'] == create_connection_request['displayName']:
                return connection

        connection = cls._execute_post_request('connections', create_connection_request)

        AppLogger.log_substep(
            "Connection created with path " + \
            f"[{connection['connectionDetails']['path']}]S")

        cls.add_connection_role_assignment_for_user(connection['id'],
                                                    AppSettings.ADMIN_USER_ID,
                                                    'Owner')

        return connection

    @classmethod
    def add_connection_role_assignment_for_user(cls, connection_id, admin_user_id, connection_role):
        """Add connection role assignment for user"""
        rest_url = f'connections//{connection_id}//roleAssignments'
        post_body = {
            'principal': {
                'id': admin_user_id,
                'type': "User"
            },
            'role': connection_role
        }
        return cls._execute_post_request(rest_url, post_body)

    @classmethod
    def create_anonymous_web_connection(cls, web_url, workspace = None):
        """Create new Web connection using Anonymous credentials"""
        display_name = 'Web'
        if workspace is not None:
            display_name = f"Workspace[{workspace['id']}]-" + display_name

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
    def create_sql_connection_with_service_principal(cls, server, database,
                                                     workspace = None, lakehouse = None):
        """Create SQL Connection"""
        display_name = ''
        if workspace is not None:
            display_name = f"Workspace[{workspace['id']}]-" + display_name
            if lakehouse is not None:
                display_name = display_name + f"Lakehouse[{lakehouse['displayName']}]"
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
                    'tenantId': AppSettings.FABRIC_TENANT_ID,
                    'servicePrincipalClientId': AppSettings.FABRIC_CLIENT_ID,
                    'servicePrincipalSecret': AppSettings.FABRIC_CLIENT_SECRET,
                    'credentialType': 'ServicePrincipal'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)

    @classmethod
    def create_azure_storage_connection_with_account_key(cls, server, path, workspace = None):
        """Create Azure Storage connections"""

        display_name = 'ADLS'
        if workspace is not None:
            display_name = f"Workspace[{workspace['id']}]-" + display_name

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
                    'key': AppSettings.AZURE_STORAGE_ACCOUNT_KEY,
                    'credentialType': 'Key'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)

    @classmethod
    def get_connection(cls, connection_id):
        """ Get connection using connection Id"""
        return cls._execute_get_request(f'connections//{connection_id}')

    @classmethod
    def delete_connection(cls, connection_id):
        """Delete connection using connection Id"""
        rest_url = f"connections/{connection_id}"
        return cls._execute_delete_request(rest_url)

    @classmethod
    def get_workspace_connections(cls, workspace_id):
        """Get connections associated with a specifc workspace"""
        workspace_connections = []
        connections = cls.get_connections()
        for connection in connections:
            if connection['displayName'] is not None and workspace_id in connection['displayName']:
                workspace_connections.append(connection)
        return workspace_connections

    @classmethod
    def create_item(cls, workspace_id, create_item_request = None):
        """Create Item using create-item-request"""
        AppLogger.log_step(
            f"Creating [{create_item_request['displayName']}.{create_item_request['type']}]...")
        endpoint = f'workspaces/{workspace_id}/items'
        item = cls._execute_post_request(endpoint, create_item_request)
        AppLogger.log_substep(f"{item['type']} created with id [{item['id']}]")
        return item

    @classmethod
    def create_lakehouse(cls, workspace_id, display_name):
        """Create Lakehouse"""
        create_item_request = {
            'displayName': display_name, 
            'type': 'Lakehouse' 
        }
        return cls.create_item(workspace_id, create_item_request)

    @classmethod
    def get_workspace_items(cls, workspace_id):
        """Get items in workspace"""
        endpoint = f'workspaces/{workspace_id}/items'
        return cls._execute_get_request(endpoint)['value']

    @classmethod
    def get_item_definition(cls, workspace_id, item_id):
        """Get item definition from workspace item"""
        endpoint = f'workspaces/{workspace_id}/items/{item_id}/getDefinition'
        return cls._execute_post_request(endpoint)

    @classmethod
    def run_notebook(cls, workspace_id, notebook):
        """Run notebook and wait for job completion"""
        AppLogger.log_substep(f"Running notebook [{notebook['displayName']}]...")
        rest_url = f"workspaces/{workspace_id}/items/{notebook['id']}" + \
                    "/jobs/instances?jobType=RunNotebook"
        response = cls._execute_post_request_for_job_scheduler(rest_url)
        AppLogger.log_substep("Notebook run job completed successfully")
        return response

    @classmethod
    def run_data_pipeline(cls, workspace_id, item_id):
        """Run notebook and wait for job completion"""
        endpoint = f'workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType=Pipeline'
        return cls._execute_post_request_for_job_scheduler(endpoint)

    @classmethod
    def get_lakehouse(cls, workspace_id, lakehouse_id):
        """Get lakehouse properties"""
        rest_url = f'workspaces/{workspace_id}/lakehouses/{lakehouse_id}'
        return cls._execute_get_request(rest_url)

    @classmethod
    def get_sql_endpoint_for_lakehouse(cls, workspace_id, lakehouse):
        """Get SQL endpoint properties for lakehouse"""

        AppLogger.log_step(
            f"Getting SQL Endpoint info for lakehouse [{lakehouse['displayName']}]...")

        lakehouse = cls.get_lakehouse(workspace_id, lakehouse['id'])
        while lakehouse['properties']['sqlEndpointProperties']['provisioningStatus'] != 'Success':
            wait_time = 10
            time.sleep(wait_time)
            lakehouse = cls.get_lakehouse(workspace_id, lakehouse['id'])

        server = lakehouse['properties']['sqlEndpointProperties']['connectionString']
        database = lakehouse['properties']['sqlEndpointProperties']['id']

        AppLogger.log_substep(f"server: {server}")
        AppLogger.log_substep(f"database: {database}")

        return {
            'server': server,
            'database': database
        }

    @classmethod
    def get_datasources_for_semantic_model(cls, workspace_id, semantic_model_id):
        """Get Datasource for semantic model using Power BI REST API"""
        rest_url  = f'groups//{workspace_id}//datasets//{semantic_model_id}//datasources'
        return cls._execute_get_request_to_powerbi(rest_url)['value']

    @classmethod
    def get_web_url_from_semantic_model(cls, workspace_id, semantic_model_id):
        """Get Web datasource URL from semantic model"""
        data_sources = cls.get_datasources_for_semantic_model(workspace_id, semantic_model_id)
        for data_source in data_sources:
            if data_source['datasourceType'] == 'Web':
                return data_source['connectionDetails']['url']

        return None

    @classmethod
    def bind_semantic_model_to_connection(cls, workspace_id, semantic_model_id, connection_id):
        """Bind semantic model to connection"""
        rest_url  = f'groups//{workspace_id}//datasets//{semantic_model_id}//Default.BindToGateway'
        post_body  = {
            'gatewayObjectId': '00000000-0000-0000-0000-000000000000',
            'datasourceObjectIds': [ connection_id ]
        }
        cls._execute_post_request_to_powerbi(rest_url, post_body)

    @classmethod
    def refresh_semantic_model(cls, workspace_id, semantic_model_id):
        """Refresh semantic model"""
        AppLogger.log_substep('Refreshing semantic model')
        rest_url  = f'groups//{workspace_id}//datasets//{semantic_model_id}//refreshes'
        post_body = { 'notifyOption': 'NoNotification', 'type': 'Automatic' }
        response = cls._execute_post_request_to_powerbi(rest_url, post_body)
        refresh_id = response.headers.get('x-ms-request-id')
        rest_url_refresh_details = \
            f'groups/{workspace_id}/datasets/{semantic_model_id}/refreshes/{refresh_id}'
        refresh_details = cls._execute_get_request_to_powerbi(rest_url_refresh_details)

        while 'status' not in refresh_details or refresh_details['status'] == 'Unknown':
            time.sleep(6)
            refresh_details = cls._execute_get_request_to_powerbi(rest_url_refresh_details)

        AppLogger.log_substep("Semantic model refresh operation complete")

    @classmethod
    def create_and_bind_semantic_model_connecton(cls, workspace,
                                                 semantic_model_id, lakehouse = None):
        """Create connection and bind it to semantic model"""
        datasources = cls.get_datasources_for_semantic_model(workspace['id'], semantic_model_id)
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
                cls.bind_semantic_model_to_connection(workspace['id'],
                                                      semantic_model_id,
                                                      connection['id'])

            elif datasource['datasourceType'].lower() == 'web':
                AppLogger.log_substep('Creating Web connection for semantic model')
                web_url  = datasource['connectionDetails']['url']
                connection = cls.create_anonymous_web_connection(web_url)
                AppLogger.log_substep('Binding semantic model to Web connection')
                cls.bind_semantic_model_to_connection(workspace['id'],
                                                      semantic_model_id,
                                                       connection['id'])
                AppLogger.log_substep('Refreshing semantic model')
                cls.refresh_semantic_model(workspace['id'], semantic_model_id)

    @classmethod
    def create_adls_gen2_shortcut(cls, workspace_id, lakehouse_id, name, path,
                                  location, subpath, connection_id):
        """Create ADLS Gen2 Shortcut"""
        AppLogger.log_step('Creating OneLake shortcut using ADLS connection...')

        rest_url = f'workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts'
        post_body = {
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
        cls._execute_post_request(rest_url, post_body)
        AppLogger.log_substep(f'Shortcut [{path}/{name}] successfullly created')
