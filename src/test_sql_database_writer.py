"""Test1"""

import json
import struct
import pyodbc
from fabric_devops import AppLogger, FabricRestApi, ItemDefinitionFactory, SqlDatabaseWriter



WORKSPACE_NAME = "Baby Cakes"
DATABASE_NAME = "SalesDB"

capacity_id = '26313c90-3e11-490d-8b24-d0faff2ede65'
workspace = FabricRestApi.create_workspace(WORKSPACE_NAME, capacity_id)

sql_db = FabricRestApi.create_sql_database(workspace['id'], DATABASE_NAME)

sqlDbWriter = SqlDatabaseWriter(workspace['id'], DATABASE_NAME)

AppLogger.log_substep("Creating database tables")

sql_create_tables = ItemDefinitionFactory.get_template_file("SQL/CreateSqlDatabaseTables.sql")

sqlDbWriter.execute_sql(sql_create_tables)

AppLogger.log_substep("Adding rows to Products table")
add_products = ItemDefinitionFactory.get_template_file("SQL/AddProductsTableRows.sql")
sqlDbWriter.execute_sql(add_products)

AppLogger.log_substep("Adding rows to Customers table")
add_customers = ItemDefinitionFactory.get_template_file("SQL/AddCustomersTableRows.sql")
sqlDbWriter.execute_sql(add_customers)

AppLogger.log_substep("Adding rows to Invoices table")
add_invoices = ItemDefinitionFactory.get_template_file("SQL/AddInvoicesTableRows.sql")
sqlDbWriter.execute_sql(add_invoices)

AppLogger.log_substep("Adding rows to InvoiceDetails table")
add_invoice_details = ItemDefinitionFactory.get_template_file("SQL/AddInvoiceDetailsTableRows.sql")
sqlDbWriter.execute_sql(add_invoice_details)

AppLogger.log_job_complete(workspace['id'])
