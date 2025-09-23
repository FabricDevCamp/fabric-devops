"""My udf """

import datetime
import logging
import fabric.functions as fn

udf = fn.UserDataFunctions()

@udf.function()
def hello_fabric(name: str) -> str:
    """Hello Fabric"""
    logging.info('Python UDF trigger function processed a request.')

    return f"Welcome to Fabric Functions, {name}, at {datetime.datetime.now()}!"
