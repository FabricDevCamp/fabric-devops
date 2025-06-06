"""Demo 02 - Get Access Token for Interactive User"""

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

authentication_result = app.acquire_token_interactive([PERMISSION_SCOPE])
access_token = authentication_result['access_token']

AppLogger.log_raw_text(access_token)
