"""App Setting Moudle"""

#import os

class AppSettings:

    """App Settings"""
    RUN_AS_SERVICE_PRINCIPAL = True
    FABRIC_CLIENT_ID = '1cc2688d-c6b1-4fe6-9246-d06003a6712f'
    FABRIC_CLIENT_SECRET = 'iPZ8Q~RarnvynjvxdONDoG0KzKuU5oyPtnK9sbU2'
    FABRIC_TENANT_ID = '5b3572e6-a387-436b-bf93-9bd7d3d54acb'
    SERVICE_PRINCIPAL_OBJECT_ID = '4a995e28-1943-4cef-bb80-6d8aefd3dfcf'
    AUTHORITY = f'https://login.microsoftonline.com/{FABRIC_TENANT_ID}'
    ADMIN_USER_ID = 'e8805a8f-201d-4f06-9e35-0d0b29faebe6'
    FABRIC_CAPACITY_ID = 'bd28859e-6c00-42d1-a008-9bb32edc4fcb'

    #FABRIC_CLIENT_ID = os.getenv("FABRIC_CLIENT_ID")
    #FABRIC_CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET")
    #FABRIC_TENANT_ID = os.getenv("FABRIC_TENANT_ID")
    #AUTHORITY = f'https://login.microsoftonline.com/{FABRIC_TENANT_ID}'
    #FABRIC_CAPACITY_ID = os.getenv("FABRIC_CAPACITY_ID")
    #ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")

    FABRIC_PERMISSION_SCOPES = [ 'https://api.fabric.microsoft.com/.default' ]
    FABRIC_REST_API_BASE_URL = 'https://api.fabric.microsoft.com/v1/'
    POWER_BI_REST_API_BASE_URL = 'https://api.powerbi.com/v1.0/myorg/'

    WEB_DATASOURCE_ROOT_URL = 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/'
    AZURE_STORAGE_ACCOUNT_NAME = 'fabricdevcamp'
    AZURE_STORAGE_ACCOUNT_KEY = \
        'qubDPUHYg078+ftNb16kW8QiJQpGJcOUGZZK7r33Sy9I5dXI+1CJq4HddyRj9X20VgFW3sS6ultt+AStzTVMlw=='
    AZURE_STORAGE_CONTAINER = 'sampledata'
    AZURE_STORAGE_CONTAINER_PATH = '/ProductSales/Dev'
    AZURE_STORAGE_SERVER = f'https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/'
    AZURE_STORAGE_PATH = AZURE_STORAGE_CONTAINER + AZURE_STORAGE_CONTAINER_PATH
