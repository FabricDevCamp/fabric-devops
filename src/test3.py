"""Test3"""
import json
from fabric_devops import EntraIdTokenManager, EnvironmentSettings

EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False
# token = EntraIdTokenManager._get_access_token_for_user('https://analysis.windows.net/powerbi/api/user_impersonation')

token = EntraIdTokenManager.get_fabric_access_token()
print(token)




