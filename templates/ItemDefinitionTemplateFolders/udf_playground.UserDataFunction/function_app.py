import logging

import fabric.functions as fn

udf = fn.UserDataFunctions()

@udf.connection(argName="targetLakehouse", alias="sales")
@udf.function()
def write_text_file(targetLakehouse: fn.FabricLakehouseClient, message: str) -> str:

    file_name = "hello_world.txt"
    file_content = message
    files = targetLakehouse.connectToFiles()

    logging.info(f'Writing [{file_name}] to lakehouse Files section.')

    text_file = files.get_file_client(file_name)
    text_file.upload_data(file_content, overwrite=True)

    text_file.close()
    files.close()

    return f"File [{file_name}] written to lakehouse Files section"

@udf.connection(argName="targetLakehouse", alias="sales")
@udf.function()
def read_text_file(targetLakehouse: fn.FabricLakehouseClient) -> str:

    file_name = "hello_world.txt"
    files = targetLakehouse.connectToFiles()

    logging.info(f'Reading [{file_name}] from lakehouse')

    text_file = files.get_file_client(file_name)
    content = text_file.download_file().read().decode('utf-8-sig')

    text_file.close()
    files.close()

    return f"File content: '{content}'"

@udf.context(argName="udfContext")
@udf.function()
def get_udf_context(udfContext: fn.UserDataFunctionContext) -> str:
  
    user_name = udfContext.executing_user['PreferredUsername']
    oid = udfContext.executing_user['Oid']
    tenant_id = udfContext.executing_user['TenantId']
    invocation_id = udfContext.invocation_id

    log_message = f'User account name: {user_name}\n'
    log_message += f'User account Id: {oid}\n'
    log_message += f'Tenant Id: {tenant_id}\n'
    log_message += f'Invocation Id is  {invocation_id}\n'

    logging.info(log_message)
 
    return log_message
