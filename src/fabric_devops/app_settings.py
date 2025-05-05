"""App Settings"""

import os

class AppSettings:

    """App Settings"""

    RUN_AS_SERVICE_PRINCIPAL = False
    RUNNING_IN_GITHUB = os.getenv('GITHUB_ACTIONS') == 'true'
    FABRIC_CLIENT_ID = os.getenv("FABRIC_CLIENT_ID")
    FABRIC_CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET")
    FABRIC_TENANT_ID = os.getenv("FABRIC_TENANT_ID")
    AUTHORITY = f'https://login.microsoftonline.com/{FABRIC_TENANT_ID}'
    FABRIC_CAPACITY_ID = os.getenv("FABRIC_CAPACITY_ID")
    ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
    SERVICE_PRINCIPAL_OBJECT_ID = os.getenv('SERVICE_PRINCIPAL_OBJECT_ID')

    CLASS_ID_POWERSHELL_APP = "1950a258-227b-4e31-a9cf-717495945fc2"

    FABRIC_PERMISSION_SCOPES = [ 'https://api.fabric.microsoft.com/.default' ]
    FABRIC_REST_API_BASE_URL = 'https://api.fabric.microsoft.com/v1/'
    POWER_BI_REST_API_BASE_URL = 'https://api.powerbi.com/v1.0/myorg/'

    WEB_DATASOURCE_ROOT_URL = 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/'
    AZURE_STORAGE_ACCOUNT_NAME = 'fabricdevcamp'
    AZURE_STORAGE_ACCOUNT_KEY = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
    AZURE_STORAGE_CONTAINER = 'sampledata'
    AZURE_STORAGE_CONTAINER_PATH = '/ProductSales/Dev'
    AZURE_STORAGE_SERVER = f'https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/'
    AZURE_STORAGE_PATH = AZURE_STORAGE_CONTAINER + AZURE_STORAGE_CONTAINER_PATH
