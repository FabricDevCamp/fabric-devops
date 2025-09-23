""" Fabric UDF to deploy sales data transforms"""
import requests
import logging
import fabric.functions as fn

udf = fn.UserDataFunctions()

@udf.connection(argName="sales", alias="sales")
@udf.function()
def ingest_csv_files(sales: fn.FabricLakehouseClient, url: str) -> str:
    """Ingest CSV files from a base URL and return the combined data as a JSON string."""

    if url is None:
        url = 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/Dev/'

    logging.info(f'Coping CSV files with sales data from {url}')

    csv_files = { "Customers.csv", "Products.csv", "Invoices.csv", "InvoiceDetails.csv" }

    connection = sales.connectToFiles()

    for csv_file in csv_files:
        csv_file_path = url + csv_file
        target_csv_path = f'/sales-data/{csv_file}'
        with requests.get(csv_file_path) as response:   
            # Save to lakehouse Files section as CSV file
            logging.info(f'Saving {csv_file} to lakehouse at {target_csv_path}')            
            csvFile = connection.get_file_client(target_csv_path)
            csv_content = response.content.decode('utf-8-sig')
            csvFile.upload_data(csv_content , overwrite=True)
            csvFile.close()
    
    connection.close()

    return 'CSV files ingested successfully'
