"""Demo 06 - Execute Fabric REST API to Create Workspace"""
import time
import json
import msal
import requests

from demo_utils import EnvironmentSettings, AppLogger

def get_access_token():
    """Get Access Token for Service Principal"""
    # create MSAL application for service principal authentication
    app = msal.ConfidentialClientApplication(
            EnvironmentSettings.FABRIC_CLIENT_ID,
            authority=EnvironmentSettings.AUTHORITY,
            client_credential=EnvironmentSettings.FABRIC_CLIENT_SECRET)

    # define Fabric REST API permission scope for service principal
    permission_scope = EnvironmentSettings.FABRIC_REST_API_RESOURCE_ID + "//.default"

    authentication_result = app.acquire_token_for_client([permission_scope])
    return authentication_result['access_token']

def execute_fabric_get_request(endpoint):
    """Execute Fabric Get Request"""
    rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
    request_headers = {
        'Content-Type':'application/json',
        'Authorization': f'Bearer {get_access_token()}'
    }

    response = requests.get(url=rest_url, headers=request_headers, timeout=60)

    if response.status_code == 200:
        return response.json()
    else:
        AppLogger.log_error(f"Error executing get with status code of {response.status_code}")

def execute_fabric_post_request(endpoint, body):
    """Execute POST request with support for Long-running Operations (LRO)"""

    rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
    request_headers = {
        'Content-Type':'application/json',
        'Authorization': f'Bearer {get_access_token()}'
    }

    response = requests.post(url=rest_url, json=body, headers=request_headers, timeout=60)

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
        return execute_fabric_post_request(endpoint, post_body)

    else:
        error_info = json.loads(response.text)
        AppLogger.log_error(
            "Error executing POST request: " + \
            f"{response.status_code} - {error_info['errorCode']} - {error_info['message']}")            
        return None

def execute_fabric_delete_request(endpoint):
    """Execute DELETE Request on Fabric REST API Endpoint"""
    rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
    request_headers= {
        'Content-Type':'application/json',
        'Authorization': f'Bearer {get_access_token()}'
    }
    response = requests.delete(url=rest_url, headers=request_headers, timeout=60)
    if response.status_code == 200:
        return None
    if response.status_code == 429: # handle TOO MANY REQUESTS error
        wait_time = int(response.headers.get('Retry-After'))
        time.sleep(wait_time)
        execute_fabric_delete_request(endpoint)
        return None
    else:
        AppLogger.log_error(
            f'Error executing DELETE request: {response.status_code} - {response.text}')
        return None

# Begin Demo Script to Create new workspace
WORKSPACE_NAME = "Contoso Sales"

AppLogger.clear_console()
AppLogger.log_step(f"Creating new workspace with dispay name [{WORKSPACE_NAME}]")

# (1) delete existing workspace with same display name if it exists
existing_workspaces = execute_fabric_get_request('workspaces')['value']
for existing_workspace in existing_workspaces:
    if existing_workspace['displayName'] == WORKSPACE_NAME:
        AppLogger.log_substep("Deleting existing workspace with the same disply name")
        DELETE_ENDPOINT = f"workspaces/{existing_workspace['id']}"
        execute_fabric_delete_request(DELETE_ENDPOINT)
        break

# (2) create new workspace
post_body = {
        'displayName': WORKSPACE_NAME,
        'description': 'a demo workspace',
        'capacityId': EnvironmentSettings.FABRIC_CAPACITY_ID
}
AppLogger.log_substep("Calling Create Workspace API to create new workspace")
workspace = execute_fabric_post_request('workspaces', post_body)
workspace_id = workspace['id']
AppLogger.log_substep(f'Workspace created with Id of [{workspace_id}]')

# (3) add workspace role assignment for admin user
AppLogger.log_substep('Adding Admin User to Workspace Role of Admin')
role_assignment_endpoint =  'workspaces/' + workspace_id + '/roleAssignments'
post_body = {
    'role': 'Admin',
    'principal': {
    'id': EnvironmentSettings.ADMIN_USER_ID,
    'type': 'User'
    }
}
execute_fabric_post_request(role_assignment_endpoint, post_body)

# (4) display workspace web URL to navigate to workspace in Fabric UI
WORKSPACE_WEB_URL = \
    f'https://app.powerbi.com/groups/{workspace_id}/list?experience=fabric-developer'
AppLogger.log_substep(f'Workspace accessible at {WORKSPACE_WEB_URL}')

AppLogger.log_step_complete()