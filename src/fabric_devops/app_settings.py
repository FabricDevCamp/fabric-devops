"""App Setting Moudle"""

import os

class AppSettings:
    """App Settings"""
    FABRIC_CLIENT_ID = os.getenv("FABRIC_CLIENT_ID")
    FABRIC_CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET")
    FABRIC_TENANT_ID = os.getenv("FABRIC_TENANT_ID")
    FABRIC_CAPACITY_ID = os.getenv("FABRIC_CAPACITY_ID")
    ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")

    AUTHORITY = f'https://login.microsoftonline.com/{FABRIC_TENANT_ID}'
    FABRIC_PERMISSION_SCOPES = [ 'https://api.fabric.microsoft.com/.default' ]
    FABRIC_REST_API_BASE_URL = 'https://api.fabric.microsoft.com/v1/'
    POWER_BI_REST_API_BASE_URL = 'https://api.powerbi.com/v1.0/myorg/'
    AZURE_STORAGE_ACCOUNT_NAME = 'fabricdevcamp'
    AZURE_STORAGE_ACCOUNT_KEY = \
        'qubDPUHYg078+ftNb16kW8QiJQpGJcOUGZZK7r33Sy9I5dXI+1CJq4HddyRj9X20VgFW3sS6ultt+AStzTVMlw=='
    AZURE_STORAGE_CONTAINER = 'sampledata'
    AZURE_STORAGE_CONTAINER_PATH = '/ProductSales/Dev'
    AZURE_STORAGE_SERVER = f'https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net'
    AZURE_STORAGE_PATH = AZURE_STORAGE_CONTAINER + AZURE_STORAGE_CONTAINER_PATH
