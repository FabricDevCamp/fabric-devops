
import datetime
import msal

from .app_logger import AppLogger
from .app_settings import AppSettings

class EntraIdTokenManager():
    """EntraIdTokenManager"""

    _access_token = None
    _access_token_expiration = None

    @classmethod
    def get_fabric_access_token(cls):
        """"Get Fabric Access Token"""

        if (cls._access_token is None) or (datetime.datetime.now() > cls._access_token_expiration):

            authentication_reult = None
            if AppSettings.RUN_AS_SERVICE_PRINCIPAL:
                authentication_reult = cls._get_fabric_authentication_result_for_service_principal()
            else:
                if AppSettings.RUNNING_IN_GITHUB:
                    authentication_reult = cls._get_fabric_authentication_result_for_user_with_device_code()
                else:
                    authentication_reult = cls._get_fabric_authentication_result_for_user_interactive()

            cls._access_token = authentication_reult['access_token']
            cls._access_token_expiration = datetime.datetime.now() + \
                                        datetime.timedelta(0,  int(authentication_reult['expires_in']))
            
        return cls._access_token

    @classmethod
    def _get_fabric_authentication_result_for_service_principal(cls):
        """Acquire Entra Id Access Token for calling Fabric REST APIs"""

        app = msal.ConfidentialClientApplication(
            AppSettings.FABRIC_CLIENT_ID,
            authority=AppSettings.AUTHORITY,
            client_credential=AppSettings.FABRIC_CLIENT_SECRET)

        return app.acquire_token_for_client(scopes=AppSettings.FABRIC_PERMISSION_SCOPES)

    @classmethod
    def _get_fabric_authentication_result_for_user_interactive(cls):
        """Authenticate the user interactively"""

        client_id = AppSettings.CLASS_ID_POWERSHELL_APP
        authority = "https://login.microsoftonline.com/organizations"

        app = msal.PublicClientApplication(client_id,
                                            authority=authority,
                                            client_credential=None)

        return app.acquire_token_interactive(
                scopes=['https://api.fabric.microsoft.com/user_impersonation'])

    @classmethod
    def _get_fabric_authentication_result_for_user_with_device_code(cls):
        """Acquire Entra Id Access Token for calling Fabric REST APIs"""
        client_id = AppSettings.CLASS_ID_POWERSHELL_APP
        authority = "https://login.microsoftonline.com/organizations"
        
        app = msal.PublicClientApplication(client_id,
                                           authority=authority,
                                           client_credential=None)
            
        AppLogger.log_step("Authenticating user using device flow...")

        flow = app.initiate_device_flow(
            scopes=['https://api.fabric.microsoft.com/user_impersonation'])

        user_code = flow['user_code']
        authentication_url =  flow['verification_uri']

        AppLogger.log_substep(
            f'Log in at {authentication_url} and enter user-code of {user_code}')

        authentication_result = app.acquire_token_by_device_flow(flow)

        AppLogger.log_substep('User token has been acquired')

        return authentication_result

