"""App Setting Moudle"""

import os

from fabric_devops.app_logger import AppLogger

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

    @classmethod
    def init_app_settings(cls):
        """Initialize App Settings"""
        is_running_in_github = os.getenv("GITHUB_ACTIONS") == "true"

        if is_running_in_github:
            AppLogger.log_step('running in GitHub')
        else:
            AppLogger.log_step('running locally')
            #cls.FABRIC_CLIENT_ID = '1cc2688d-c6b1-4fe6-9246-d06003a6712f'
            #cls.FABRIC_CLIENT_SECRET = r'iPZ8Q~RarnvynjvxdONDoG0KzKuU5oyPtnK9sbU2'
            #cls.FABRIC_TENANT_ID = '5b3572e6-a387-436b-bf93-9bd7d3d54acb'
            #cls.FABRIC_CAPACITY_ID = 'c2b61760-8349-476e-91c7-f8079e65b7e8'
            #cls.ADMIN_USER_ID = 'e8805a8f-201d-4f06-9e35-0d0b29faebe6'
