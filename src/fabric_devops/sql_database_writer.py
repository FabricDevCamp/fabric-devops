"""SQL Database Writer for executing SQL queries on SQL datasources"""

import pyodbc

from .app_logger import AppLogger
from .fabric_rest_api import EnvironmentSettings

from .fabric_rest_api import FabricRestApi

class SqlDatabaseWriter:

    def __init__(self, workspace_id, fabric_sql_database_name):

        self.workspace_id = workspace_id
        self.fabric_sql_database_name = fabric_sql_database_name

        sql_db = FabricRestApi.get_item_by_name(workspace_id, fabric_sql_database_name, 'SQLDatabase')

        sql_db_properties = FabricRestApi.get_sql_database_properties(workspace_id, sql_db['id'])

        self.server = sql_db_properties['serverFqdn']
        self.database = sql_db_properties['databaseName']

    def _create_connection(self):
        """Create connection string for SQL Database"""

        if EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL is True:
            user_id = f'{EnvironmentSettings.FABRIC_CLIENT_ID}@{EnvironmentSettings.FABRIC_TENANT_ID}'
            pwd = EnvironmentSettings.FABRIC_CLIENT_SECRET
            connect_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{self.server};database={self.database};UID={user_id};PWD={pwd};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryServicePrincipal"
        else:
            connect_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{self.server};database={self.database};;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryInteractive"
        
        return pyodbc.connect(connect_string, autocommit=True)
    
    def execute_sql(self, sql_query):
        """Execute SQL query on the specified SQL Database"""
        connection = self._create_connection()
        cursor = connection.cursor()
        cursor.execute(sql_query)
        connection.commit()
        cursor.close()
        connection.close()