"""Entra Id Token Manager"""

import os
import msal

from .app_logger import AppLogger
from .app_settings import AppSettings


class EntraIdTokenManager():
    """EntraIdTokenManager"""

    #region Low-level details about authentication and token acquisition

    _token_cache_folder = './/HeyHey//'
    _token_cache_file = 'token-cache.bin'
    _token_cache = None

    @classmethod
    def _persist_token_cache(cls):


        if not os.path.exists(cls._token_cache_folder):
            os.makedirs(cls._token_cache_folder)

        cache_file_path = os.path.join(
            cls._token_cache_folder, 
            cls._token_cache_file)

        cache_file = open(cache_file_path, 'w', encoding='utf-8')
        cache_file.write(cls._token_cache.serialize())

    @classmethod
    def _get_token_cache(cls):

        cls._token_cache = msal.SerializableTokenCache()
        
        if not os.path.exists(cls._token_cache):
            os.makedirs(cls._token_cache_folder)

        cache_file_path = os.path.join(
            cls._token_cache_folder, 
            cls._token_cache_file)

        if os.path.exists(cache_file_path):
            cls._token_cache.deserialize(open(cache_file_path, "r", encoding="utf-8").read())

        return cls._token_cache

    @classmethod
    def _get__authentication_result_for_service_principal(cls, scopes):
        """Acquire Entra Id Access Token for calling Fabric REST APIs"""

        app = msal.ConfidentialClientApplication(
            AppSettings.FABRIC_CLIENT_ID,
            authority=AppSettings.AUTHORITY,
            client_credential=AppSettings.FABRIC_CLIENT_SECRET,
            token_cache=cls._get_token_cache())

        AppLogger.log_substep(f'Aquiring SPN token with scopes {scopes}')

        authentication_result = app.acquire_token_for_client(scopes)

        cls._persist_token_cache()

        return authentication_result

    @classmethod
    def _get_authentication_result_for_user_interactive(cls, scopes):
        """Authenticate the user interactively"""
        client_id = AppSettings.CLIENT_ID_AZURE_POWERSHELL_APP
        authority = "https://login.microsoftonline.com/organizations"

        app = msal.PublicClientApplication(client_id,
                                            authority=authority,
                                            client_credential=None,
                                            token_cache=cls._get_token_cache())

        authentication_result = None
        try:
            accounts = app.get_accounts()
            authentication_result = \
                app.acquire_token_silent(scopes, account=accounts[0])
        except IndexError:
            authentication_result = app.acquire_token_interactive(scopes)
            
        cls._persist_token_cache()

        return authentication_result

    @classmethod
    def _get_authentication_result_for_user_with_device_code(cls, scopes):
        """Acquire Entra Id Access Token for calling Fabric REST APIs"""
        client_id = AppSettings.CLIENT_ID_AZURE_POWERSHELL_APP
        authority = "https://login.microsoftonline.com/organizations"

        token_cache = cls._get_token_cache()

        app = msal.PublicClientApplication(client_id,
                                           authority=authority,
                                           client_credential=None,
                                           token_cache=token_cache)

        AppLogger.log_step("Authenticating user using device flow...")

        AppLogger.log_substep(f'Aquiring user token with scopes {scopes}')

        flow = app.initiate_device_flow(scopes=scopes)

        user_code = flow['user_code']
        authentication_url =  flow['verification_uri']

        AppLogger.log_substep(
            f'Log in at {authentication_url} and enter user-code of {user_code}')

        authentication_result = app.acquire_token_by_device_flow(flow)

        AppLogger.log_substep('User token has been acquired using device code')

        cls._persist_token_cache()

        return authentication_result

    #endregion

    @classmethod
    def get_fabric_access_token(cls):
        """"Get Fabric Access Token"""
        if AppSettings.RUN_AS_SERVICE_PRINCIPAL:
            scopes = [ AppSettings.FABRIC_REST_API_RESOURCE_ID + "//.default" ]
            authentication_reult = \
                cls._get_authentication_result_for_service_principal(scopes)
        else:
            scopes = [ AppSettings.FABRIC_REST_API_RESOURCE_ID + "//user_impersonation" ]
            if AppSettings.RUNNING_IN_GITHUB:
                authentication_reult = \
                    cls._get_authentication_result_for_user_with_device_code(scopes)
            else:
                authentication_reult = \
                    cls._get_authentication_result_for_user_interactive(scopes)

        return authentication_reult['access_token']

    @classmethod
    def get_ado_access_token(cls):
        """"Get Access Token for Azure Dev """
        ado_resource_id = '499b84ac-1321-427f-aa17-267ca6975798'
        if AppSettings.RUN_AS_SERVICE_PRINCIPAL:
            scopes = [ ado_resource_id + "//.default" ]
            authentication_reult = \
                cls._get_authentication_result_for_service_principal(scopes)
        else:
            scopes = [ ado_resource_id + "//user_impersonation" ]
            if AppSettings.RUNNING_IN_GITHUB:
                authentication_reult = \
                    cls._get_authentication_result_for_user_with_device_code(scopes)
            else:
                authentication_reult = \
                    cls._get_authentication_result_for_user_interactive(scopes)

        return authentication_reult['access_token']

    @classmethod
    def get_kqldb_access_token(cls, query_service_url):
        """"Get Access Token for KQL DB"""
        if AppSettings.RUN_AS_SERVICE_PRINCIPAL:
            scopes = [ query_service_url + "//.default" ]
            authentication_reult = \
                cls._get_authentication_result_for_service_principal(scopes)
        else:
            scopes = [ query_service_url + "//user_impersonation" ]
            if AppSettings.RUNNING_IN_GITHUB:
                authentication_reult = \
                    cls._get_authentication_result_for_user_with_device_code(scopes)
            else:
                authentication_reult = \
                    cls._get_authentication_result_for_user_interactive(scopes)

        return authentication_reult['access_token']
