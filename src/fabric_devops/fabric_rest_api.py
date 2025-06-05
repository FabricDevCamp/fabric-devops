"""Module to manage calls to Fabric REST APIs"""

import time
from json.decoder import JSONDecodeError

import requests

from .app_logger import AppLogger
from .environment_settings import EnvironmentSettings
from .entra_id_token_manager import EntraIdTokenManager

class FabricRestApi:
    """Wrapper class for calling Fabric REST APIs"""

#region Low-level details about authentication and HTTP requests and responses

    @classmethod
    def _execute_get_request(cls, endpoint):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = EntraIdTokenManager.get_fabric_access_token()
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
        rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = EntraIdTokenManager.get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                           'Authorization': f'Bearer {access_token}'}

        response = requests.post(url=rest_url, json=post_body, headers=request_headers, timeout=60)

        if response.status_code in { 200, 201 }:
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
        else:
            AppLogger.log_error(
                f'Error executing POST request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_post_request_for_job_scheduler(cls, endpoint, post_body=''):
        """Execute POST request with support for Om-demand Job with Job Scheduler"""
        rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = EntraIdTokenManager.get_fabric_access_token()
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
        rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = EntraIdTokenManager.get_fabric_access_token()
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
    def _execute_delete_request(cls, endpoint):
        """Execute DELETE Request on Fabric REST API Endpoint"""
        rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = EntraIdTokenManager.get_fabric_access_token()
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

        rest_url = EnvironmentSettings.POWER_BI_REST_API_BASE_URL + endpoint
        access_token = EntraIdTokenManager.get_fabric_access_token()
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
        rest_url = EnvironmentSettings.POWER_BI_REST_API_BASE_URL + endpoint
        access_token = EntraIdTokenManager.get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                           'Authorization': f'Bearer {access_token}'}
        return requests.post(url=rest_url, headers=request_headers, json=post_body, timeout=60)

    @classmethod
    def _execute_patch_request_to_powerbi(cls, endpoint, post_body):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = EnvironmentSettings.POWER_BI_REST_API_BASE_URL + endpoint
        access_token = EntraIdTokenManager.get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                           'Authorization': f'Bearer {access_token}'}
        response = requests.patch(url=rest_url, json=post_body, headers=request_headers, timeout=60)
        if response.status_code in {200, 204}:
            try:
                return response.json()
            except JSONDecodeError:
                return None

        if response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            return cls._execute_patch_request_to_powerbi(endpoint, post_body)
        else:
            AppLogger.log_error(
                f'Error executing PATCH request: {response.status_code} - {response.text}')
            return None


#endregion

    @classmethod
    def authenticate(cls):
        """authenticate"""
        EntraIdTokenManager.get_fabric_access_token()

    @classmethod
    def list_capacities(cls):
        """Get all workspaces accessible to caller"""
        return cls._execute_get_request('capacities')['value']

    @classmethod
    def display_capacities(cls):
        """Display all capacities accessible to caller"""
        AppLogger.log_step('Capacities:')
        capacities = cls.list_capacities()
        for capacity in capacities:
            AppLogger.log_substep(capacity['displayName'])

    @classmethod
    def list_deployment_pipelines(cls):
        """Get all deployment pipelines accessible to caller"""
        return cls._execute_get_request('deploymentPipelines')['value']

    @classmethod
    def get_deployment_pipeline_by_name(cls, display_name):
        """Get Deployment Pipeline item by display name"""
        pipelines =  cls.list_deployment_pipelines()
        for pipeline in pipelines:
            if pipeline['displayName'] == display_name:
                return pipeline
        return None

    @classmethod
    def list_deployment_pipeline_stages(cls, pipeline_id):
        """List all deployment pipeline stages"""
        endpoint = f"deploymentPipelines/{pipeline_id}/stages"
        return cls._execute_get_request(endpoint)['value']

    @classmethod
    def delete_deployment_pipeline(cls, pipeline_id):
        """Delete Deployment Pipeline"""
        endpoint = f"deploymentPipelines/{pipeline_id}"
        return cls._execute_delete_request(endpoint)

    @classmethod
    def display_deployment_pipelines(cls):
        """Display Deployment Pipeline"""
        AppLogger.log_step('Deployment Pipelines:')
        pipelines = cls.list_deployment_pipelines()
        for pipeline in pipelines:
            AppLogger.log_substep(f"{pipeline['id']} - {pipeline['displayName']}")

    @classmethod
    def create_deployment_pipeline(cls, display_name, stages):
        """Create Deployment Pipeline"""
        AppLogger.log_step(f'Creating Deployment Pipelines [{display_name}]')
        pipeline_stages = []
        for stage in stages:
            pipeline_stages.append({
                'displayName': stage,
                'description': 'it works',
                'isPublic': False
            })
        create_request = {
            'displayName': display_name,
            'description': 'great example',
            'stages': pipeline_stages
        }
        endpoint = "deploymentPipelines"
        pipeline = cls._execute_post_request(endpoint, create_request)

        AppLogger.log_substep(f"Pipeline create with id [{pipeline['id']}]")

        if EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL:
            AppLogger.log_substep('Adding deployment pipeline role of [Admin] for admin user')
            cls.add_deployment_pipeline_role_assignment(pipeline['id'],
                                                        EnvironmentSettings.ADMIN_USER_ID,
                                                        'User',
                                                        'Admin')
        else:
            AppLogger.log_substep(
                'Adding deployment pipeline role of [Admin] for service principal')

            cls.add_deployment_pipeline_role_assignment(pipeline['id'],
                                                        EnvironmentSettings.SERVICE_PRINCIPAL_OBJECT_ID,
                                                        'ServicePrincipal',
                                                        'Admin')
        return pipeline

    @classmethod
    def add_deployment_pipeline_role_assignment(cls, pipeline_id,
                                                principal_id, principal_type, role):
        """Add Deployment Pipeline Role Assignment"""
        endpoint = f"deploymentPipelines/{pipeline_id}/roleAssignments"
        add_request = {
            'principal': {
                'id': principal_id,
                'type': principal_type
            },
            'role': role
        }
        return cls._execute_post_request(endpoint, add_request)

    @classmethod
    def assign_workpace_to_pipeline_stage(cls, workspace_id, pipeline_id, stage_id):
        """Assign workspace to pipeline stage """
        endpoint = f"deploymentPipelines/{pipeline_id}/stages/{stage_id}/assignWorkspace"
        assign_request = { 'workspaceId': workspace_id }
        cls._execute_post_request(endpoint, assign_request)

    @classmethod
    def unassign_workpace_from_pipeline_stage(cls, pipeline_id, stage_id):
        """Assign workspace to pipeline stage"""
        endpoint = f"deploymentPipelines/{pipeline_id}/stages/{stage_id}/unassignWorkspace"
        cls._execute_post_request(endpoint)

    @classmethod
    def deploy_to_pipeline_stage(cls, pipeline_id, source_stage_id, target_stage_id,  note = None):
        """Deploy to pipeline stage"""
        endpoint = f'deploymentPipelines/{pipeline_id}/deploy'
        deploy_request = {
            'sourceStageId': source_stage_id,
            'targetStageId': target_stage_id
        }
        if note is not None:
            deploy_request['note'] = note
        cls._execute_post_request(endpoint, deploy_request)

    @classmethod
    def list_workspaces(cls):
        """Get all workspaces accessible to caller"""
        all_workspaces = cls._execute_get_request('workspaces')['value']
        return filter(lambda workspace: workspace['type'] == 'Workspace', all_workspaces)

    @classmethod
    def display_workspaces(cls):
        """Display all workspaces accessible to caller"""
        AppLogger.log_step('Workspaces:')
        workspaces = cls.list_workspaces()
        for workspace in workspaces:
            AppLogger.log_substep(f"{workspace['id']} - {workspace['displayName']}")

    @classmethod
    def get_workspace_by_name(cls, display_name):
        """Get Workspace item by display name"""
        workspaces =  cls.list_workspaces()
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
            'capacityId': EnvironmentSettings.FABRIC_CAPACITY_ID
        }

        AppLogger.log_substep('Calling Create-Workspace API...')
        workspace = cls._execute_post_request('workspaces', post_body)
        workspace_id = workspace['id']
        AppLogger.log_substep(f'Workspace created with Id of [{workspace_id}]')

        if EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL:
            AppLogger.log_substep('Adding workspace role of [Admin] for admin user')
            cls.add_workspace_user(workspace_id, EnvironmentSettings.ADMIN_USER_ID, 'Admin')
        else:
            AppLogger.log_substep('Adding workspace role of [Admin] for service principal')
            cls.add_workspace_spn(workspace_id, EnvironmentSettings.SERVICE_PRINCIPAL_OBJECT_ID, 'Admin')

        return workspace

    @classmethod
    def delete_workspace(cls, workspace_id):
        """Delete Workspace"""

        workspace_connections  = cls.list_workspace_connections(workspace_id)
        for connection in workspace_connections:
            AppLogger.log_substep(f"Deleting connection {connection['displayName']}")
            cls.delete_connection(connection['id'])

        endpoint = f'workspaces/{workspace_id}'
        cls._execute_delete_request(endpoint)

    @classmethod
    def update_workspace_description(cls, workspace_id, description):
        """Update Workspace Description"""
        endpoint = f'workspaces/{workspace_id}'
        body = { 'description': description }
        cls._execute_patch_request(endpoint, body)

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
    def add_workspace_spn(cls, workspace_id, spn_id, role_assignment):
        """Add workspace role for user"""
        endpoint =  'workspaces/' + workspace_id + '/roleAssignments'
        post_body = {
          'role': role_assignment,
          'principal': {
            'id': spn_id,
            'type': 'ServicePrincipal'
          }
        }
        return cls._execute_post_request(endpoint, post_body)

    @classmethod
    def list_connections(cls):
        """Get all connections accessible to caller"""
        return cls._execute_get_request('connections')['value']

    @classmethod
    def get_connection_by_name(cls, display_name):
        """Get Connection By Name"""
        connections = cls.list_connections()
        for connection in connections:
            if connection['displayName'] == display_name:
                return connection
        return None

    @classmethod
    def display_connections(cls):
        """Get all connections accessible to caller"""
        connections = cls.list_connections()
        AppLogger.log_step('Connections')
        for connection in connections:
            AppLogger.log_substep(
                f"{connection['id']} - " + \
                f"{connection['connectionDetails']['type']} - {connection['displayName']}")            

    @classmethod
    def create_connection(cls, create_connection_request, top_level_step = True):
        """ Create new connection"""
        log_message = f"Creating {create_connection_request['connectionDetails']['type']} " + \
                      f"connection named {create_connection_request['displayName']} ..."

        if top_level_step:
            AppLogger.log_step(log_message)
        else:
            AppLogger.log_substep(log_message)

        existing_connections = cls.list_connections()
        for connection in existing_connections:
            if 'displayName' in connection and \
                connection['displayName'] == create_connection_request['displayName']:
                return connection

        connection = cls._execute_post_request('connections', create_connection_request)

        AppLogger.log_substep(
            f"Connection created with id [{connection['id']}]")

        if EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL:
            AppLogger.log_substep('Adding connection role of [Owner] for user')
            cls.add_connection_role_assignment_for_user(connection['id'],
                                                        EnvironmentSettings.ADMIN_USER_ID,
                                                        'Owner')
        else:
            AppLogger.log_substep('Adding connection role of [Owner] for SPN')
            cls.add_connection_role_assignment_for_spn(connection['id'],
                                                       EnvironmentSettings.SERVICE_PRINCIPAL_OBJECT_ID,
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
    def add_connection_role_assignment_for_spn(cls, connection_id, spn_object_id, connection_role):
        """Add connection role assignment for user"""
        rest_url = f'connections//{connection_id}//roleAssignments'
        post_body = {
            'principal': {
                'id': spn_object_id,
                'type': "ServicePrincipal"
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
                    'tenantId': EnvironmentSettings.FABRIC_TENANT_ID,
                    'servicePrincipalClientId': EnvironmentSettings.FABRIC_CLIENT_ID,
                    'servicePrincipalSecret': EnvironmentSettings.FABRIC_CLIENT_SECRET,
                    'credentialType': 'ServicePrincipal'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)

    @classmethod
    def create_azure_storage_connection_with_account_kex(cls, server, path,
                                                         workspace = None,
                                                         top_level_step = False):
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
                    'key': EnvironmentSettings.AZURE_STORAGE_ACCOUNT_KEY,
                    'credentialType': 'Key'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request, top_level_step=top_level_step)

    @classmethod
    def create_azure_storage_connection_with_sas_token(cls, server, path,
                                                       workspace = None,
                                                       top_level_step = True):
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
                    'token': EnvironmentSettings.AZURE_STORAGE_SAS_TOKEN,
                    'credentialType': 'SharedAccessSignature'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request, top_level_step=top_level_step)

    @classmethod
    def patch_oauth_connection_to_kqldb(cls, workspace, semantic_model, query_service_uri):
        """Patch AzureDataExplorer connections using OAuth credentials"""

        datasources = FabricRestApi.list_datasources_for_semantic_model(workspace['id'], semantic_model['id'])

        for datasource in datasources:
            if datasource['datasourceType'] == 'Extension' and \
               datasource['connectionDetails']['kind'] == 'AzureDataExplorer':

                datasource_id = datasource['datasourceId']
                gateway_id = datasource['gatewayId']                
                rest_url = f'gateways/{gateway_id}/datasources/{datasource_id}'
                
                AppLogger.log_step(f'Patching credentials for {query_service_uri}')

                access_token = EntraIdTokenManager.get_kqldb_access_token(query_service_uri)

                part1 = r'{"credentialData":[{"name":"accessToken","value":"'
                part2 = r'"}]}'

                stringified_credentials = part1 + access_token + part2

                update_datasource_request = {
                    'credentialDetails': {
                        'credentialType': 'OAuth2',
                        'credentials': stringified_credentials,
                        'encryptedConnection': 'NotEncrypted',
                        'encryptionAlgorithm': 'None',
                        'privacyLevel': 'None',
                        'useCallerAADIdentity': 'False'
                    }
                }
       
                cls._execute_patch_request_to_powerbi(rest_url, update_datasource_request)
                AppLogger.log_substep("OAuth creds successfully patched")

    @classmethod
    def create_github_connection(cls, url, workspace = None, top_level_step = False):
        """Create GitHub connections with Personal Access Token"""

        display_name = 'GitHub'
        if workspace is not None:
            display_name = f"Workspace[{workspace['id']}]-" + display_name
        else:
            display_name = display_name + " - " + url

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

        return cls.create_connection(create_connection_request, top_level_step=top_level_step)

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
    def list_workspace_connections(cls, workspace_id):
        """Get connections associated with a specifc workspace"""
        workspace_connections = []
        connections = cls.list_connections()
        for connection in connections:
            if connection['displayName'] is not None and workspace_id in connection['displayName']:
                workspace_connections.append(connection)
        return workspace_connections

    @classmethod
    def list_folders(cls, workspace_id):
        """List Folder"""
        endpoint = f'workspaces/{workspace_id}/folders'
        return cls._execute_get_request(endpoint)['value']

    @classmethod
    def create_folder(cls, workspace_id, folder_name, parent_folder_id = None):
        """Create Folder"""
        endpoint = f'workspaces/{workspace_id}/folders'
        body = { 'displayName': folder_name }

        if parent_folder_id is not None:
            body['parentFolderId'] = parent_folder_id

        AppLogger.log_step(f'Creating folder {folder_name}')

        folder = cls._execute_post_request(endpoint, body)
        AppLogger.log_substep(f"folder creatrd with Id [{folder['id']}]")
        
        return folder

    @classmethod
    def create_item(cls, workspace_id, create_item_request, folder_id = None):
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
    def update_item(cls, workspace_id, item, update_item_request):
        """Update Item Definition using update-item--definition-request"""
        AppLogger.log_step(f"Updating [{item['displayName']}.{item['type']}]...")
        endpoint = f"workspaces/{workspace_id}/items/{item['id']}"
        item = cls._execute_post_request(endpoint, update_item_request)
        AppLogger.log_substep(f"{item['type']} updated")
        return item

    @classmethod
    def update_item_definition(cls, workspace_id, item, update_item_definition_request):
        """Update Item Definition using update-item--definition-request"""
        AppLogger.log_step(
            f"Updating [{item['displayName']}.{item['type']}]...")
        endpoint = f"workspaces/{workspace_id}/items/{item['id']}/updateDefinition"
        item = cls._execute_post_request(endpoint, update_item_definition_request)
        AppLogger.log_substep("Item updated")
        return item

    @classmethod
    def get_item_definition(cls, workspace_id, item, export_format = None):
        """Get Item Definition"""
        endpoint = f"workspaces/{workspace_id}/items/{item['id']}/getDefinition"
        if export_format is not None:
            endpoint = endpoint + f'?format={export_format}'
        return cls._execute_post_request(endpoint)

    @classmethod
    def create_lakehouse(cls, workspace_id, display_name, folder_id = None):
        """Create Lakehouse"""
        create_item_request = {
            'displayName': display_name, 
            'type': 'Lakehouse' 
        }
        return cls.create_item(workspace_id, create_item_request, folder_id)

    @classmethod
    def create_warehouse(cls, workspace_id, display_name, folder_id = None):
        """Create Warehouse"""
        create_item_request = {
            'displayName': display_name, 
            'type': 'Warehouse' 
        }
        return cls.create_item(workspace_id, create_item_request, folder_id)


    @classmethod
    def list_workspace_items(cls, workspace_id, item_type = None):
        """Get items in workspace"""
        endpoint = f'workspaces/{workspace_id}/items'

        if item_type is not None:
            endpoint = endpoint + f'?type={item_type}'

        return cls._execute_get_request(endpoint)['value']

    @classmethod
    def get_item_by_name(cls, workspace_id, display_name, item_type):
        """Get Item by Name"""
        items = cls.list_workspace_items(workspace_id, item_type)
        for item in items:
            if item['displayName'] == display_name:
                return item
        return None

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
    def run_data_pipeline(cls, workspace_id, pipeline):
        """Run notebook and wait for job completion"""
        AppLogger.log_substep(f"Running data pipeline [{pipeline['displayName']}]...")

        rest_url = f"workspaces/{workspace_id}/items/{pipeline['id']}" + \
                    "/jobs/instances?jobType=Pipeline"
        response = cls._execute_post_request_for_job_scheduler(rest_url)
        AppLogger.log_substep("Data pipeline run job completed successfully")
        return response

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
    def get_warehouse(cls, workspace_id, warehouse_id):
        """Get warehouse properties"""
        rest_url = f'workspaces/{workspace_id}/warehouses/{warehouse_id}'
        return cls._execute_get_request(rest_url)

    @classmethod
    def get_warehouse_connection_string(cls, workspace_id, warehouse_id):
        """Get warehouse properties"""
        rest_url = f'workspaces/{workspace_id}/warehouses/{warehouse_id}'
        warehouse = cls._execute_get_request(rest_url)
        return warehouse['properties']['connectionString']

    @classmethod
    def get_eventhouse(cls, workspace_id, eventhouse_id):
        """Get warehouse properties"""
        rest_url = f'workspaces/{workspace_id}/eventhouses/{eventhouse_id}'
        return cls._execute_get_request(rest_url)


    @classmethod
    def list_datasources_for_semantic_model(cls, workspace_id, semantic_model_id):
        """Get Datasource for semantic model using Power BI REST API"""
        rest_url  = f'groups//{workspace_id}//datasets//{semantic_model_id}//datasources'
        return cls._execute_get_request_to_powerbi(rest_url)['value']

    @classmethod
    def get_web_url_from_semantic_model(cls, workspace_id, semantic_model_id):
        """Get Web datasource URL from semantic model"""
        data_sources = cls.list_datasources_for_semantic_model(workspace_id, semantic_model_id)
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
        AppLogger.log_substep('Refreshing semantic model...')
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

        AppLogger.log_substep("Refresh operation complete")

    @classmethod
    def create_and_bind_semantic_model_connecton(cls, workspace,
                                                 semantic_model_id, lakehouse = None):
        """Create connection and bind it to semantic model"""
        datasources = cls.list_datasources_for_semantic_model(workspace['id'], semantic_model_id)
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
                connection = cls.create_anonymous_web_connection(web_url, workspace)
                AppLogger.log_substep('Binding semantic model to Web connection')
                cls.bind_semantic_model_to_connection(workspace['id'],
                                                      semantic_model_id,
                                                       connection['id'])
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

    @classmethod
    def set_active_valueset_for_variable_library(cls, workspace_id, library, valueset):
        """Set active valueset for variable library"""
        AppLogger.log_step(
            "Setting active valueset for variable library " + \
            f"[{library['displayName']}] to [{valueset}]...")

        rest_url = f"workspaces/{workspace_id}/VariableLibraries/{library['id']}"
        post_body = {
            'properties': {                
                'activeValueSetName': valueset
            }
        }
        cls._execute_patch_request(rest_url, post_body)
        AppLogger.log_substep('Active valueset set successfullly')

    
    @classmethod
    def get_github_repo_connection(cls, repo_name, workspace = None):
        """Get GitHub Repo Connection"""

        github_repo_url = f'https://github.com/fabricdevcampdemos/{repo_name}'

        connections = FabricRestApi.list_connections()

        for connection in connections:
            if connection['connectionDetails']['type'] == 'GitHubSourceControl' and \
               connection['connectionDetails']['path'] == github_repo_url:
                return connection

        return FabricRestApi.create_github_connection(github_repo_url, workspace)

    @classmethod
    def connect_workspace_to_github_repo(cls, workspace, branch = 'main'):
        """Connect Workspace to GitHub Repository"""

        repo_name = workspace['displayName'].replace(" ", "-")

        AppLogger.log_step(f"Connecting workspace [{workspace['displayName']}] " + \
                           f"to GitHub repo [{repo_name}]")

        connection_id = cls.get_github_repo_connection(repo_name, workspace)['id']

        cls.create_github_repository_connection(workspace['id'], repo_name, connection_id, branch)

        init_request = {
            'initializationStrategy': 'PreferWorkspace'
        }

        init_response = cls.initialize_git_connection(workspace['id'], init_request)

        required_action = init_response['requiredAction']

        if required_action == 'CommitToGit':
            commit_to_git_request = {
                'mode': 'All',
                'workspaceHead': init_response['workspaceHead'],
                'comment': 'Saul Goodman'
            }
            cls.commit_workspace_to_git(workspace['id'], commit_to_git_request)

        if required_action == 'UpdateFromGit':
            update_from_git_request = {
                "workspaceHead": init_response['workspaceHead'],
                "remoteCommitHash": init_response['remoteCommitHash'],
                "conflictResolution": {
                    "conflictResolutionType": "Workspace",
                    "conflictResolutionPolicy": "PreferWorkspace"
                },
                "options": {
                    "allowOverrideItems": True
                }
                            
            }
            cls.update_workspace_from_git(workspace['id'], update_from_git_request)

    @classmethod
    def create_github_repository_connection(cls, workspace_id, repo_name, connection_id, branch = 'main'):
        """Connect Workspace to GIT Repository"""
        endpoint = f"workspaces/{workspace_id}/git/connect"

        connect_request = {
            "gitProviderDetails": {
                "ownerName": "fabricdevcampdemos",
                "gitProviderType": "GitHub",
                "repositoryName": repo_name,
                "branchName": branch,
                "directoryName": "/workspace"
            },
            "myGitCredentials": {
                "source": "ConfiguredConnection",
                "connectionId": connection_id            
            }
        }

        return cls._execute_post_request(endpoint, connect_request)

    @classmethod
    def initialize_git_connection(cls, workspace_id, initialize_connection_request):
        """Initialize GIT Connection"""
        endpoint = f"workspaces/{workspace_id}/git/initializeConnection"
        return cls._execute_post_request(endpoint, initialize_connection_request)

    @classmethod
    def get_git_status(cls, workspace_id):
        """Get GIT Connection Status"""
        endpoint = f"workspaces/{workspace_id}/git/status"
        return cls._execute_get_request(endpoint)

    @classmethod
    def get__git_connection(cls, workspace_id):
        """Get GIT Connection"""
        endpoint = f"workspaces/{workspace_id}/git/connection"
        return cls._execute_get_request(endpoint)

    @classmethod
    def disconnect_workspace_from_git(cls, workspace_id):
        """Disconnect Workspace from GIT Repository"""
        endpoint = f"workspaces/{workspace_id}/git/disconnect"
        return cls._execute_post_request(endpoint)

    @classmethod
    def commit_workspace_to_git(cls, workspace_id, commit_to_git_request):
        """Commit Workspace to GIT Repository"""
        endpoint = f"workspaces/{workspace_id}/git/commitToGit"
        return cls._execute_post_request(endpoint, commit_to_git_request)

    @classmethod
    def update_workspace_from_git(cls, workspace_id, update_from_git_request):
        """Update Workspace from GIT Repository"""
        endpoint = f"workspaces/{workspace_id}/git/updateFromGit"
        return cls._execute_post_request(endpoint, update_from_git_request)

    @classmethod
    def get_my_git_credentials(cls, workspace_id):
        """Get My GIT Credential"""
        endpoint = f"workspaces/{workspace_id}/git/myGitCredentials"
        return cls._execute_get_request(endpoint)

    @classmethod
    def update_my_git_credentials(cls, workspace_id, update_git_credentials_request):
        """Update My GIT Credentials"""
        endpoint = f"workspaces/{workspace_id}/git/myGitCredentials"
        return cls._execute_patch_request(endpoint, update_git_credentials_request)
        
