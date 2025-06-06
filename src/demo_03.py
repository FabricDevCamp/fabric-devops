"""Demo 03 - Get Access Token for User using Device Code"""

import msal

from fabric_devops import EnvironmentSettings, AppLogger

app = msal.PublicClientApplication(
    EnvironmentSettings.CLIENT_ID_AZURE_POWERSHELL_APP,
    authority="https://login.microsoftonline.com/organizations",
    client_credential=None
)

PERMISSION_SCOPE = EnvironmentSettings.FABRIC_REST_API_RESOURCE_ID + "//user_impersonation"

AppLogger.log_step(
    f'Aquiring access token for user with scope [{PERMISSION_SCOPE}]')

AppLogger.log_step("Authenticating user using device flow...")

flow = app.initiate_device_flow(scopes=[PERMISSION_SCOPE])
user_code = flow['user_code']
authentication_url =  flow['verification_uri']
AppLogger.log_substep(
    f'Log in at {authentication_url} and enter user-code of {user_code}')
authentication_result = app.acquire_token_by_device_flow(flow)
AppLogger.log_substep('User token has been acquired using device code')
access_token = authentication_result['access_token']

AppLogger.log_raw_text(access_token)
