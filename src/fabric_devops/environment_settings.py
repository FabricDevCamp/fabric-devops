"""Environment Settings"""

import os

class EnvironmentSettings:
    """Environment Settings"""

    AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
    AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
    AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
    SERVICE_PRINCIPAL_OBJECT_ID = os.getenv('SERVICE_PRINCIPAL_OBJECT_ID')

    FABRIC_CAPACITY_ID = os.getenv("FABRIC_CAPACITY_ID")
    ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
    DEVELOPERS_GROUP_ID = os.getenv("DEVELOPERS_GROUP_ID")

    WEB_DATASOURCE_ROOT_URL = 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/'
    AZURE_STORAGE_ACCOUNT_NAME = 'fabricdevcamp'
    AZURE_STORAGE_CONTAINER = 'sampledata'
    AZURE_STORAGE_CONTAINER_PATH = '/ProductSales/Dev'
    AZURE_STORAGE_SERVER = f'https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/'
    AZURE_STORAGE_PATH = AZURE_STORAGE_CONTAINER + AZURE_STORAGE_CONTAINER_PATH
    AZURE_STORAGE_SAS_TOKEN = \
        r'sv=2024-11-04&ss=b&srt=co&sp=rl&se=2027-05-28T22:50:19Z&st=2025-05-19T14:50:19Z&spr=https&sig=%2FCsr%2F07zsA8EbanP5N1Dy4DAdbKSf7B63iJb1da8LC4%3D'

    ORGANIZATION_GITHUB = os.getenv('ORGANIZATION_GITHUB')
    PERSONAL_ACCESS_TOKEN_GITHUB = os.getenv('PERSONAL_ACCESS_TOKEN_GITHUB')

    FABRIC_NONTRIAL_CAPACITY_ID = '26313c90-3e11-490d-8b24-d0faff2ede65'