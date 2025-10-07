"""Entra Id Token Manager"""

import datetime
import os
import msal

from .app_logger import AppLogger
from .environment_settings import EnvironmentSettings

class EntraIdTokenManager():
    """EntraIdTokenManager"""

    #region Low-level details about authentication and token acquisition

    # in-memory token cache
    _token_cache = dict()

    # persistent token cache - only used for local user authentication in VSCode
    _persistent_token_cache_folder = './/.token_cache//'
    _persistent_token_cache_file = 'token-cache.bin'
    _persistent_token_cache = None

    @classmethod
    def _save__persistent_token_cache(cls):

        if not os.path.exists(cls._persistent_token_cache_folder):
            os.makedirs(cls._persistent_token_cache_folder)

        persistent_cache_file_path = os.path.join(
            cls._persistent_token_cache_folder,
            cls._persistent_token_cache_file)

        persistent_cache_file = open(persistent_cache_file_path, 'w', encoding='utf-8')
        token_cache_content = cls._persistent_token_cache.serialize()
        persistent_cache_file.write(token_cache_content)

    @classmethod
    def _get_persistent_token_cache(cls):

        cls._persistent_token_cache = msal.SerializableTokenCache()

        if not os.path.exists(cls._persistent_token_cache_folder):
            os.makedirs(cls._persistent_token_cache_folder)

        persistent_cache_file_path = os.path.join(
            cls._persistent_token_cache_folder,
            cls._persistent_token_cache_file)

        if os.path.exists(persistent_cache_file_path):
            cls._persistent_token_cache.deserialize(
                open(persistent_cache_file_path, "r", encoding="utf-8").read()
            )

        return cls._persistent_token_cache

    @classmethod
    def _get_access_token_for_service_principal(cls, scope):
        """Acquire Entra Id Access Token for calling Fabric REST APIs"""

        if (scope in cls._token_cache) and \
           (datetime.datetime.now() < cls._token_cache[scope]['access_token_expiration']):
            return cls._token_cache[scope]['access_token']
                
        app = msal.ConfidentialClientApplication(
            EnvironmentSettings.FABRIC_CLIENT_ID,
            authority=EnvironmentSettings.AUTHORITY,
            client_credential=EnvironmentSettings.FABRIC_CLIENT_SECRET)

        authentication_result = app.acquire_token_for_client([scope])

        cls._token_cache[scope] = {
            'access_token': authentication_result['access_token'],
            'access_token_expiration': datetime.datetime.now() + \
                                       datetime.timedelta(0,  int(authentication_result['expires_in']))
        }

        return authentication_result['access_token']

    @classmethod
    def _get_access_token_for_user(cls, scope):
        """Authenticate the user interactively"""

        client_id = EnvironmentSettings.CLIENT_ID_AZURE_POWERSHELL_APP
        authority = "https://login.microsoftonline.com/organizations"

        app = msal.PublicClientApplication(client_id,
                                            authority=authority,
                                            client_credential=None,
                                            token_cache=cls._get_persistent_token_cache())

        authentication_result = None
        try:
            accounts = app.get_accounts()
            authentication_result = \
                app.acquire_token_silent([scope], account=accounts[0])
            if authentication_result is None:
                authentication_result = app.acquire_token_interactive([scope])
    
        except IndexError:
            authentication_result = app.acquire_token_interactive([scope])
            
        cls._save__persistent_token_cache()

        return authentication_result['access_token']

    @classmethod
    def _get_access_token_for_user_with_device_code(cls, scope):
        """Acquire Entra Id Access Token for calling Fabric REST APIs"""
        
        if (scope in cls._token_cache) and \
           (datetime.datetime.now() < cls._token_cache[scope]['access_token_expiration']):
            return cls._token_cache[scope]['access_token']

        client_id = EnvironmentSettings.CLIENT_ID_AZURE_POWERSHELL_APP
        authority = "https://login.microsoftonline.com/organizations"

        app = msal.PublicClientApplication(client_id,
                                           authority=authority,
                                           client_credential=None)

        AppLogger.log_step("Authenticating user using device flow...")
        flow = app.initiate_device_flow(scopes=[scope])
        user_code = flow['user_code']
        authentication_url =  flow['verification_uri']
        AppLogger.log_substep(
            f'Log in at {authentication_url} and enter user-code of {user_code}')
        authentication_result = app.acquire_token_by_device_flow(flow)
        AppLogger.log_substep('User token has been acquired using device code')

        cls._token_cache[scope] = {
            'access_token': authentication_result['access_token'],
            'access_token_expiration': datetime.datetime.now() + \
                                       datetime.timedelta(0,  int(authentication_result['expires_in']))
        }

        return authentication_result['access_token']

    #endregion

    @classmethod
    def get_fabric_access_token(cls):
        """"Get Fabric Access Token"""

        if EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL:
            scope_for_service_principal = EnvironmentSettings.FABRIC_REST_API_RESOURCE_ID + "//.default"
            return cls._get_access_token_for_service_principal(scope_for_service_principal)
        
        scope_for_user = EnvironmentSettings.FABRIC_REST_API_RESOURCE_ID + "//user_impersonation"

        if EnvironmentSettings.RUNNING_LOCALLY:
            return cls._get_access_token_for_user(scope_for_user)

        if EnvironmentSettings.RUNNING_IN_GITHUB:
            return cls._get_access_token_for_user_with_device_code(scope_for_user)

    @classmethod
    def get_sql_access_token(cls):
        """"Get Fabric Access Token"""

        if EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL:
            scope_for_service_principal = "https://database.windows.net/.default"
            return cls._get_access_token_for_service_principal(scope_for_service_principal)
        
        scope_for_user = "https://database.windows.net/user_impersonation"

        if EnvironmentSettings.RUNNING_LOCALLY:
            return cls._get_access_token_for_user(scope_for_user)

        if EnvironmentSettings.RUNNING_IN_GITHUB:
            return cls._get_access_token_for_user_with_device_code(scope_for_user)


    @classmethod
    def get_ado_access_token(cls):
        """"Get Access Token for Azure Dev """
        ado_resource_id = '499b84ac-1321-427f-aa17-267ca6975798'
        if EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL:
            scope_for_service_principal = ado_resource_id + "/.default"
            return cls._get_access_token_for_service_principal(scope_for_service_principal)
        else:
            scope_for_user = ado_resource_id + "/user_impersonation"
            if EnvironmentSettings.RUNNING_IN_GITHUB:
                return cls._get_access_token_for_user_with_device_code(scope_for_user)
            else:
                return cls._get_access_token_for_user(scope_for_user)
                

    @classmethod
    def get_kqldb_access_token(cls, query_service_url):
        """"Get Access Token for KQL DB"""
        if EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL:
            scope_for_service_principal = query_service_url + "//.default"
            return cls._get_access_token_for_service_principal(scope_for_service_principal)
        else:
            scope_for_user = query_service_url + "//user_impersonation"
            if EnvironmentSettings.RUNNING_IN_GITHUB:
                return cls._get_access_token_for_user_with_device_code(scope_for_user)
            else:
                return cls._get_access_token_for_user(scope_for_user)

    @classmethod
    def get_azure_access_token(cls):
        """"Get Access Token for Azure Dev """
        azure_resource_id = 'https://management.core.windows.net'
        if EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL:
            scope_for_service_principal = azure_resource_id + "/.default"
            return cls._get_access_token_for_service_principal(scope_for_service_principal)
        else:
            scope_for_user = azure_resource_id + "/user_impersonation"
            if EnvironmentSettings.RUNNING_IN_GITHUB:
                return cls._get_access_token_for_user_with_device_code(scope_for_user)
            else:
                return cls._get_access_token_for_user(scope_for_user)
                
