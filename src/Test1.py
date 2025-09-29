"""Test1"""

import json
import struct
import pyodbc
from fabric_devops import DeploymentManager, EnvironmentSettings, AppLogger, FabricRestApi, ItemDefinitionFactory


class SqlDatabaseWriter:

    def __init__(self, workspace_id, fabric_sql_database_name):
        self.workspace_id = workspace_id
        self.fabric_sql_database_name = fabric_sql_database_name

        sql_db = FabricRestApi.get_item_by_name(workspace_id, fabric_sql_database_name, 'SQLDatabase')

        sql_db_properties = FabricRestApi.get_sql_database_properties(workspace['id'], sql_db['id'])

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

WORKSPACE_NAME = "Acme2"
DATABASE_NAME = "SalesDB"

workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)

sql_db = FabricRestApi.create_sql_database(workspace['id'], DATABASE_NAME)

sqlDbWriter = SqlDatabaseWriter(workspace['id'], DATABASE_NAME)

sql = ItemDefinitionFactory.get_template_file("SQL/CreateSqlDatabaseTables.sql")

sqlDbWriter.execute_sql(sql)

AppLogger.log_job_complete(workspace['id'])

# db_properties = FabricRestApi.get_sql_database_properties(workspace['id'], sql_db['id'])



# sql_server = db_properties['serverFqdn']
# sql_database = db_properties['databaseName']

# connect_string = create_connection_string(sql_server, sql_database)

# connection = pyodbc.connect(connect_string)

# cursor = connection.cursor()

# print(f'Connect string:{connect_string}')
# # connection = get_connection(connect_string, sql_access_token)

# print( dir(connection) )

# create_table_sql = """
# CREATE TABLE Employees (
#     EmployeeID INT PRIMARY KEY IDENTITY(1,1),
#     FirstName VARCHAR(50) NOT NULL,
#     LastName VARCHAR(50) NOT NULL,
#     Email VARCHAR(100) UNIQUE,
#     HireDate DATE
# );
# """

# cursor.execute(create_table_sql)
# connection.commit()



# print( json.dumps(FabricRestApi.list_capacities(), indent=4) )

# def create_connection_string(server, database):
#     """Create connection string for SQL Database"""
#     user_id = f'{EnvironmentSettings.FABRIC_CLIENT_ID}@{EnvironmentSettings.FABRIC_TENANT_ID}'
#     pwd = EnvironmentSettings.FABRIC_CLIENT_SECRET
#     return f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{server};database={database};UID={user_id};PWD={pwd};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryServicePrincipal"

# def get_connection(connection_string, access_token):
#     """Get connection to SQL Database using access token"""
#     exptoken = b""
#     for i in access_token.encode("UTF-16-LE"):
#         exptoken += bytes({i})
#         exptoken += bytes(1)
#     tokenstruct = struct.pack("=i", len(exptoken)) + exptoken
#     SQL_COPT_SS_ACCESS_TOKEN = 1256
#     return pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: tokenstruct})
