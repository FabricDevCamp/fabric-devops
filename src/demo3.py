import yaml

filename = './/src//parameter.yml'

with open(filename, 'r', encoding='utf-8') as file:
    config = yaml.safe_load(file)

print(config)

yaml_config = {
    'find_replace': [
        {'find_value': 'b3ec6731-c004-4382-bfd9-30dc158061eb', 
         'replace_value': {
             'TEST': '3ecf3344-21cd-4d41-abb1-4b7562da516d', 
             'PROD': 'aabc7e38-c5fc-45fe-90d5-93ca5ad75edf'
             }
        }, 
        {'find_value': 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/Dev', 
         'replace_value': {
             'TEST': 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/Test', 
             'PROD': 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/Prod'
          }, 
         'item_type': 'SemanticModel', 
         'item_name': ['Product Sales Imported Model']
         }
        ]}