"""Environment Settings"""

import os

class EnvironmentSettings:
    """Environment Settings"""

    AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
    AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
    AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
    SERVICE_PRINCIPAL_OBJECT_ID = os.getenv('SERVICE_PRINCIPAL_OBJECT_ID')
    
    AZURE_STORAGE_SAS_TOKEN = os.getenv('AZURE_STORAGE_SAS_TOKEN')

    FABRIC_CAPACITY_ID = os.getenv("FABRIC_CAPACITY_ID")
    ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
    DEVELOPERS_GROUP_ID = os.getenv("DEVELOPERS_GROUP_ID")

    ADO_ORGANIZATION = os.getenv('ADO_ORGANIZATION')

    ORGANIZATION_GITHUB = os.getenv('ORGANIZATION_GITHUB')
    PERSONAL_ACCESS_TOKEN_GITHUB = os.getenv('PERSONAL_ACCESS_TOKEN_GITHUB')

    #  WEB_DATASOURCE_ROOT_URL = 'https://github.com/FabricDevCamp/SampleData/raw/refs/heads/main/productsales/'
    WEB_DATASOURCE_ROOT_URL = 'https://github.com/FabricDevCamp/SampleData/raw/refs/heads/main/productsales/'

    AZURE_STORAGE_ACCOUNT_NAME = 'fabricdevcampdemos'
    AZURE_STORAGE_CONTAINER = 'sampledata'
    AZURE_STORAGE_CONTAINER_PATH = '/productsales/dev'
    AZURE_STORAGE_SERVER = f'https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/'
    AZURE_STORAGE_PATH = AZURE_STORAGE_CONTAINER + AZURE_STORAGE_CONTAINER_PATH
    AZURE_STORAGE_SAS_TOKEN = os.getenv('AZURE_STORAGE_SAS_TOKEN')

    ADO_ORGANIZATION = os.getenv('ADO_ORGANIZATION')
    
    ORGANIZATION_GITHUB = os.getenv('ORGANIZATION_GITHUB')
    PERSONAL_ACCESS_TOKEN_GITHUB = os.getenv('PERSONAL_ACCESS_TOKEN_GITHUB')

    FABRIC_NONTRIAL_CAPACITY_ID = os.getenv('FABRIC_NONTRIAL_CAPACITY_ID')
