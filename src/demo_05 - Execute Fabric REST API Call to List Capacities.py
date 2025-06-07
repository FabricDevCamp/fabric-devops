"""Demo 05 - Execute Fabric REST API Call to List Capacities"""

import msal
import requests

from fabric_devops import EnvironmentSettings, AppLogger

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

    AppLogger.log_error(f"Error executing get with status code of {response.status_code}")
    return None

# run demo
AppLogger.clear_console()
capacities_response = execute_fabric_get_request('capacities')
capacities = capacities_response['value']

AppLogger.log_step("Capacities List")
for capacity in capacities:
    AppLogger.log_substep(f"{capacity['id']}: [sku={capacity['sku']}] {capacity['displayName']}")

AppLogger.log_step_complete()
