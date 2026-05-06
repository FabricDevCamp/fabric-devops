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

    ADO_ORGANIZATION = os.getenv('ADO_ORGANIZATION')

    ORGANIZATION_GITHUB = os.getenv('ORGANIZATION_GITHUB')
    PERSONAL_ACCESS_TOKEN_GITHUB = os.getenv('PERSONAL_ACCESS_TOKEN_GITHUB')

    #  WEB_DATASOURCE_ROOT_URL = 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/'
    # WEB_DATASOURCE_ROOT_URL = 'https://github.com/FabricDevCamp/SampleData/raw/refs/heads/main/ProductSales/'
    WEB_DATASOURCE_ROOT_URL = 'https://fabricdevcampdemos.blob.core.windows.net/sampledata/ProductSales/'

    AZURE_STORAGE_ACCOUNT_NAME = 'fabricdevcampdemos'
    AZURE_STORAGE_CONTAINER = 'sampledata'
    AZURE_STORAGE_CONTAINER_PATH = '/ProductSales/Dev'
    AZURE_STORAGE_SERVER = f'https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/'
    AZURE_STORAGE_PATH = AZURE_STORAGE_CONTAINER + AZURE_STORAGE_CONTAINER_PATH
    AZURE_STORAGE_SAS_TOKEN = os.getenv('AZURE_STORAGE_SAS_TOKEN')

    ADO_ORGANIZATION = os.getenv('ADO_ORGANIZATION')
    
    ORGANIZATION_GITHUB = os.getenv('ORGANIZATION_GITHUB')
    PERSONAL_ACCESS_TOKEN_GITHUB = os.getenv('PERSONAL_ACCESS_TOKEN_GITHUB')

    FABRIC_NONTRIAL_CAPACITY_ID = os.getenv('FABRIC_NONTRIAL_CAPACITY_ID')

    # this read-only SaS token is provided so you can tested out creating ADLS Gen2 connections
    AZURE_STORAGE_SAS_TOKEN = \
        r'sv=2024-11-04&ss=b&srt=co&sp=rl&se=2027-05-28T22:50:19Z&st=2025-05-19T14:50:19Z&spr=https&sig=%2FCsr%2F07zsA8EbanP5N1Dy4DAdbKSf7B63iJb1da8LC4%3D'
