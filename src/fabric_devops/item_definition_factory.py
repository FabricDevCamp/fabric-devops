"""A module to create/update/export/import item definitions"""

import base64

class ItemDefinitionFactory:
    """Logic to to create and update item definitions"""

    @classmethod
    def _create_inline_base64_part(cls, path, payload):
        """create item definition part with base64 encoding"""
        return {
            'path': path,
            'payload': base64.b64encode(payload.encode('utf-8')).decode('utf-8'),
            'payloadType': 'InlineBase64'
        }

    @classmethod
    def get_template_file(cls, path):
        """get contents of a file from templates folder"""
        file_path = f".//templates//ItemDefinitionTemplateFiles//{path}"
        file = open(file_path,'r', encoding="utf-8")
        file_content = file.read()
        file.close()
        return file_content

    @classmethod
    def get_create_notebook_request(cls, workspace_id, lakehouse, display_name, py_file):
        """Create Item Definition for a Notebook"""
        py_content = cls.get_template_file(f"Notebooks//{py_file}")
        py_content = py_content.replace('{WORKSPACE_ID}', workspace_id) \
                             .replace('{LAKEHOUSE_ID}', lakehouse['id']) \
                             .replace('{LAKEHOUSE_NAME}', lakehouse['displayName'])

        item_part = cls._create_inline_base64_part('notebook-content.py', py_content)
        item_definition = {
            'parts': [ item_part ]
        }

        return {
            'displayName': display_name,
            'type': 'Notebook',
            'definition': item_definition
        }

    @classmethod
    def get_semantic_model_create_request(cls, display_name, bim_file):
        """Create semantic model create request from BIM file"""
        pbism_content = cls.get_template_file("SemanticModels//definition.pbism")
        bim_content  = cls.get_template_file(f'SemanticModels//{bim_file}')

        return {
            'displayName': display_name,
            'type': "SemanticModel",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('definition.pbism', pbism_content),
                    cls._create_inline_base64_part('model.bim', bim_content)
                ]
            }
        }

    @classmethod
    def get_directlake_model_create_request(cls, display_name, bim_file, server, database):
        """Get Create Request for DirectLake Semantic Model"""

        pbism_content = cls.get_template_file("SemanticModels//definition.pbism")
        bim_template  = cls.get_template_file(f'SemanticModels//{bim_file}')

        bim_content = bim_template.replace('{SQL_ENDPOINT_SERVER}', server) \
                                  .replace('{SQL_ENDPOINT_DATABASE}', database)

        return {
            'displayName': display_name,
            'type': "SemanticModel",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('definition.pbism', pbism_content),
                    cls._create_inline_base64_part('model.bim', bim_content)
                ]
            }
        }

    @classmethod
    def get_report_create_request(cls, semantic_model_id, display_name, report_json_file):
        """Get Create Request for Report using Report.json file"""

        pbir_content = \
            cls.get_template_file("Reports//definition.pbir").replace('{SEMANTIC_MODEL_ID}',
                                                                      semantic_model_id)

        report_json_content  = cls.get_template_file(f'Reports//{report_json_file}')
        theme_file_content = \
            cls.get_template_file(
                "Reports//StaticResources//SharedResources//BaseThemes//CY24SU02.json")
        return {
            'displayName': display_name,
            'type': "Report",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('definition.pbir', pbir_content),
                    cls._create_inline_base64_part('report.json', report_json_content),
                    cls._create_inline_base64_part(
                        'StaticResources/SharedResources/BaseThemes/CY24SU02.json', 
                        theme_file_content )
                ]
            }
        }

    @classmethod
    def get_data_pipeline_create_request(cls, display_name, pipeline_content_json):
        """Get Create Request for Data Pipeline using pipeline-content.json file"""
        return {
            'displayName': display_name,
            'type': "DataPipeline",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('pipeline-content.json', pipeline_content_json),
                ]
            }
        }
