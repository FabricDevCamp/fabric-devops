"""Demo Utility Classes"""

import os

class EnvironmentSettings:
  """Environment Settings"""

  FABRIC_CLIENT_ID = os.getenv("FABRIC_CLIENT_ID")
  FABRIC_CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET")
  FABRIC_TENANT_ID = os.getenv("FABRIC_TENANT_ID")
  AUTHORITY = f'https://login.microsoftonline.com/{FABRIC_TENANT_ID}'
  FABRIC_CAPACITY_ID = os.getenv("FABRIC_CAPACITY_ID")
  ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
  SERVICE_PRINCIPAL_OBJECT_ID = os.getenv('SERVICE_PRINCIPAL_OBJECT_ID')

  CLIENT_ID_AZURE_POWERSHELL_APP = "1950a258-227b-4e31-a9cf-717495945fc2"

  FABRIC_REST_API_RESOURCE_ID = 'https://api.fabric.microsoft.com'
  FABRIC_REST_API_BASE_URL = 'https://api.fabric.microsoft.com/v1/'
  POWER_BI_REST_API_BASE_URL = 'https://api.powerbi.com/v1.0/myorg/'

  WEB_DATASOURCE_ROOT_URL = 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/'
  AZURE_STORAGE_ACCOUNT_NAME = 'fabricdevcamp'
  AZURE_STORAGE_CONTAINER = 'sampledata'
  AZURE_STORAGE_CONTAINER_PATH = '/ProductSales/Dev'
  AZURE_STORAGE_SERVER = f'https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/'
  AZURE_STORAGE_PATH = AZURE_STORAGE_CONTAINER + AZURE_STORAGE_CONTAINER_PATH
  AZURE_STORAGE_SAS_TOKEN = \
    r'sv=2024-11-04&ss=b&srt=co&sp=rl&se=2027-05-28T22:50:19Z&st=2025-05-19T14:50:19Z&spr=https&sig=%2FCsr%2F07zsA8EbanP5N1Dy4DAdbKSf7B63iJb1da8LC4%3D'


class AppLogger:
  """Logic to write log output to console and/or logs"""

  @classmethod
  def clear_console(cls):
    """Clear Console Window when running locally"""
    if os.name == 'nt':
      os.system('cls')

  @classmethod
  def log_job(cls, message):
    """start job"""
    print('', flush=True)
    print('|' + ('-' * (len(message) + 2)) + '|', flush=True)
    print(f'| {message} |', flush=True)
    print('|' + ('-' * (len(message) + 2)) + '|', flush=True)

  @classmethod
  def log_job_ended(cls, message=''):
    """log that job has ended"""
    print(' ', flush=True)
    print(f'> {message}', flush=True)
    print(' ', flush=True)

  @classmethod
  def log_job_complete(cls, workspace_id = None):
    """log that job has ended"""
    cls.log_step("Deployment job completed")
    if workspace_id is not None:
       workspace_laucnh_url = f'https://app.powerbi.com/groups/{workspace_id}/list?experience=fabric-developer'
       cls.log_substep(f'Workspace launch URL: {workspace_laucnh_url}')
    print(' ', flush=True)

  @classmethod
  def log_step(cls, message):
    """log a step"""
    print(' ', flush=True)
    print('> ' + message, flush=True)

  @classmethod
  def log_substep(cls, message):
    """log a sub step"""  
    print('  - ' + message, flush=True)

  @classmethod
  def log_step_complete(cls):
    """add linebreak to log"""
    print(' ', flush=True)

  TABLE_WIDTH = 120

  @classmethod
  def log_table_header(cls, table_title):
    """Log Table Header"""
    print(' ', flush=True)
    print(f'> {table_title}', flush=True)
    print('  ' + ('-' * (cls.TABLE_WIDTH)), flush=True)

  @classmethod
  def log_table_row(cls, column1_value, column2_value):
    """Log Table Row"""
    column1_width = 20
    column1_value_length = len(column1_value)
    column1_offset = column1_width - column1_value_length
    column2_width = cls.TABLE_WIDTH  - column1_width
    column2_value_length = len(column2_value)
    column2_offset = column2_width - column2_value_length - 5
    row = f'  | {column1_value}{" " * column1_offset}| {column2_value}{" " * column2_offset}|'
    print(row, flush=True)
    print('  ' + ('-' * (cls.TABLE_WIDTH)), flush=True)

  @classmethod
  def log_raw_text(cls, text):
    """log raw text"""
    print(text, flush=True)
    print('', flush=True)

  @classmethod
  def log_error(cls, message):
    """log error"""
    error_message = "ERROR: " + message
    print('-' * len(error_message), flush=True)
    print(error_message, flush=True)
    print('-' * len(error_message), flush=True)