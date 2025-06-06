"""Demo 01 - Get Access Token for Service Principal"""

import msal

from fabric_devops import EnvironmentSettings, AppLogger

# create MSAL application for service principal authentication
app = msal.ConfidentialClientApplication(
        EnvironmentSettings.FABRIC_CLIENT_ID,
        authority=EnvironmentSettings.AUTHORITY,
        client_credential=EnvironmentSettings.FABRIC_CLIENT_SECRET)

# define Fabric REST API permission scope for service principal
PERMISSION_SCOPE = EnvironmentSettings.FABRIC_REST_API_RESOURCE_ID + "//.default"

AppLogger.log_substep(
    f'Aquiring access token for service principal with scope [{PERMISSION_SCOPE}]')

authentication_result = app.acquire_token_for_client([PERMISSION_SCOPE])

access_token = authentication_result['access_token']

AppLogger.log_step("Access token acquired")
AppLogger.log_raw_text(access_token)
