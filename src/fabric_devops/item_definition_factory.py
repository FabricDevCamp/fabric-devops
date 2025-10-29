"""A module to create/update/export/import item definitions"""

import base64
import json
import os
import shutil
import re

from .app_logger import AppLogger
from .fabric_rest_api import FabricRestApi
from .variable_library import VariableLibrary

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
    def _get_part_path(cls, item_folder_path, file_path):
        """get path for item definition part"""
        offset = file_path.find(item_folder_path) + len(item_folder_path) + 1
        return file_path[offset:].replace('\\', '/')

    @classmethod
    def _search_and_replace_in_payload(cls, payload, search_replace_text):
        payload_bytes = base64.b64decode(payload)
        payload_content = payload_bytes.decode('utf-8')
        for entry in search_replace_text.keys():
            payload_content = payload_content.replace(entry, search_replace_text[entry])
        return base64.b64encode(payload_content.encode('utf-8')).decode('utf-8')

    @classmethod
    def _search_and_replace_in_payload_with_regex(cls, payload, search_replace_terms):
        payload_bytes = base64.b64decode(payload)
        payload_content = payload_bytes.decode('utf-8')

        for search, replace in search_replace_terms.items():
            payload_content, count = re.subn(search, replace, payload_content)

        return base64.b64encode(payload_content.encode('utf-8')).decode('utf-8')

    @classmethod
    def get_template_file(cls, path):
        """get contents of a file from templates folder"""
        file_path = f".//templates//ItemDefinitionTemplateFiles//{path}"
        file = open(file_path,'r', encoding="utf-8")
        file_content = file.read()
        file.close()
        return file_content

    @classmethod
    def get_create_item_request_from_folder(cls, item_folder):
        """generate create item request from folder"""
        folder_path = f".//templates//ItemDefinitionTemplateFolders//{item_folder}"
        platform_file_path = f'{folder_path}//.platform'
        file = open(platform_file_path,'r', encoding="utf-8")
        file_content = json.loads(file.read())
        file.close()
        item_type = file_content['metadata']['type']
        item_display_name = file_content['metadata']['displayName']

        item_definition_parts = []

        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                part_path = cls._get_part_path(item_folder, file_path)
                file = open(file_path,'r', encoding="utf-8")
                part_content = file.read()
                item_definition_parts.append(cls._create_inline_base64_part(part_path, part_content))

        return {
            'displayName': item_display_name,
            'type': item_type,
            'definition': {
                'parts': item_definition_parts
            }

        }

    @classmethod
    def update_item_definition_part(cls, item_definition, part_path, search_replace_text):
        """Update Item Definition Part"""
        item_part = next((part for part in item_definition['parts'] if part['path'] == part_path), None)
        if item_part is not None:
            item_definition['parts'].remove(item_part)
            item_part['payload'] = cls._search_and_replace_in_payload(item_part['payload'], search_replace_text)
            item_definition['parts'].append(item_part)
        return item_definition

    @classmethod
    def update_item_definition_part_with_regex(cls, item_definition, part_path, search_replace_terms):
        """Update Item Definition Part"""
        item_part = next((part for part in item_definition['parts'] if part['path'] == part_path), None)
        if item_part is not None:
            item_definition['parts'].remove(item_part)
            item_part['payload'] = cls._search_and_replace_in_payload_with_regex(item_part['payload'], search_replace_terms)
            item_definition['parts'].append(item_part)
        return item_definition

    @classmethod
    def get_create_notebook_request_from_folder(cls, item_folder, workspace_id, lakehouse):
        """generate create item request from folder"""
        create_request = cls.get_create_item_request_from_folder(item_folder)
        notebook_redirects = {
            '{WORKSPACE_ID}': workspace_id,
            '{LAKEHOUSE_ID}': lakehouse['id'],
            '{LAKEHOUSE_NAME}': lakehouse['displayName']
        }
        return cls.update_part_in_create_request(create_request,
                                                 'notebook-content.py',
                                                 notebook_redirects,)

    @classmethod
    def get_create_report_request_from_folder(cls, item_folder, model_id):
        """generate create item request from folder"""
        create_request = cls.get_create_item_request_from_folder(item_folder)
        return cls.update_create_report_request_with_semantic_model(create_request,
                                                                    model_id)

    @classmethod
    def get_create_report_request_from_pbir_folder(cls, item_folder, model_id):
        """generate create item request from folder"""
        create_request = cls.get_create_item_request_from_folder(item_folder)
        return cls.update_create_pbir_report_request_with_semantic_model(create_request,
                                                                         model_id)
       

    @classmethod
    def update_part_in_create_request(cls, create_item_request, part_path, search_replace_text):
        """Update Item Definition Part"""
        item_definition = create_item_request['definition']
        item_part = next((part for part in item_definition['parts'] if part['path'] == part_path), None)
        if item_part is not None:
            item_definition['parts'].remove(item_part)
            item_part['payload'] = cls._search_and_replace_in_payload(item_part['payload'], search_replace_text)
            item_definition['parts'].append(item_part)
        
        return {
            'displayName': create_item_request['displayName'],
            'type': create_item_request['type'],
            'definition': item_definition
        }


    @classmethod
    def get_notebook_create_request(cls, workspace_id, lakehouse, display_name, py_file):
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
    def get_semantic_model_create_request_from_definition(cls, display_name, bim_definition):
        """Create semantic model create request from BIM file"""
        pbism_content = cls.get_template_file("SemanticModels//definition.pbism")

        return {
            'displayName': display_name,
            'type': "SemanticModel",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('definition.pbism', pbism_content),
                    cls._create_inline_base64_part('model.bim', bim_definition)
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
    def update_report_definition_with_semantic_model_id(cls, item_definition, target_model_id):
        """Update Item Definition Part"""
        item_part = next((part for part in item_definition['parts'] if part['path'] == 'definition.pbir'), None)
        if item_part is not None:
            item_definition['parts'].remove(item_part)
            file_template = cls.get_template_file(r"Reports//definition.pbir")
            file_content = file_template.replace('{SEMANTIC_MODEL_ID}', target_model_id)
            item_part = cls._create_inline_base64_part('definition.pbir', file_content)
            item_definition['parts'].append(item_part)

        return item_definition

    @classmethod
    def update_create_report_request_with_semantic_model(cls, create_report_request, target_model_id):
        """Update Item Definition Part"""
        item_definition = create_report_request['definition']
        item_part = next((part for part in item_definition['parts'] if part['path'] == 'definition.pbir'), None)
        if item_part is not None:
            item_definition['parts'].remove(item_part)
            file_template = cls.get_template_file(r"Reports//definition.pbir")
            file_content = file_template.replace('{SEMANTIC_MODEL_ID}', target_model_id)
            item_part = cls._create_inline_base64_part('definition.pbir', file_content)
            item_definition['parts'].append(item_part)

        return {
            'displayName': create_report_request['displayName'],
            'type': "Report",
            'definition': item_definition
        }

    @classmethod
    def update_create_pbir_report_request_with_semantic_model(cls, create_report_request, target_model_id):
        """Update Item Definition Part"""
        item_definition = create_report_request['definition']
        item_part = next((part for part in item_definition['parts'] if part['path'] == 'definition.pbir'), None)
        if item_part is not None:
            item_definition['parts'].remove(item_part)
            file_template = cls.get_template_file(r"Reports//definition_pbir.pbir")
            file_content = file_template.replace('{SEMANTIC_MODEL_ID}', target_model_id)
            item_part = cls._create_inline_base64_part('definition.pbir', file_content)
            item_definition['parts'].append(item_part)

        return {
            'displayName': create_report_request['displayName'],
            'type': "Report",
            'definition': item_definition
        }

    @classmethod
    def get_data_pipeline_create_request(cls, display_name, pipeline_definition):
        """Get Create Request for Data Pipeline using pipeline-content.json file"""
        return {
            'displayName': display_name,
            'type': "DataPipeline",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('pipeline-content.json', pipeline_definition),
                ]
            }
        }

    @classmethod
    def get_eventhouse_create_request(cls, display_name, eventhouse_properties = None):                
        """Get Eventstream Create Request"""
        if eventhouse_properties is None:
            eventhouse_properties = cls.get_template_file(
                "Eventhouses//EventhouseProperties.json")
        return {
            'displayName': display_name,
            'type': "Eventhouse",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('EventhouseProperties.json', 
                                                   eventhouse_properties),
                ]
            }
        }

    @classmethod
    def get_kql_database_create_request(cls, display_name, eventhouse):
        """Get KQL Database Create Request"""

        database_schema = cls.get_template_file(
                "KqlDatabases//DatabaseSchema.kql")
        database_template = cls.get_template_file(
                "KqlDatabases//DatabaseProperties.json")
        database_properties = database_template.replace('{EVENTHOUSE_ID}', eventhouse['id'])
        
        return {
            'displayName': display_name,
            'type': "KQLDatabase",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('DatabaseProperties.json', database_properties),
                    cls._create_inline_base64_part('DatabaseSchema.kql', database_schema),
                ]
            }
        }

    @classmethod
    def get_eventstream_create_request(cls, display_name, workspace_id, eventhouse_id, kql_database):
        """Get Eventstream Create Request"""
        eventstream_properties = cls.get_template_file(
                "Eventstreams//eventstreamProperties.json")

        eventstream_template = cls.get_template_file(
                "Eventstreams//eventstream.json")
        
        eventstream_json = eventstream_template.replace('{WORKSPACE_ID}', workspace_id)\
                                               .replace('{KQL_DATABASE_ID}', kql_database['id'])\
                                               .replace('{KQL_DATABASE_NAME}', kql_database['displayName'])\
                                               .replace('{EVENTHOUSE_ID}', eventhouse_id)                             
        return {
            'displayName': display_name,
            'type': "Eventstream",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('eventstreamProperties.json', eventstream_properties),
                    cls._create_inline_base64_part('eventstream.json', eventstream_json),
                ]
            }
        }

    @classmethod
    def get_kql_dashboard_create_request(cls, display_name, workspace_id, kql_database, query_service_uri):
        """Get KQL Dashboard Create Request"""
        realtime_dashboard_template = cls.get_template_file("KqlDashboards//RealTimeDashboard.json")

        realtime_dashboard_json = realtime_dashboard_template.replace('{WORKSPACE_ID}', workspace_id)\
                                                             .replace('{KQL_DATABASE_ID}', kql_database['id'])\
                                                             .replace('{KQL_DATABASE_NAME}', kql_database['displayName'])\
                                                             .replace('{QUERY_SERVICE_URI}', query_service_uri)
        
        return {
            'displayName': display_name,
            'type': "KQLDashboard",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('RealTimeDashboard.json', realtime_dashboard_json),
                ]
            }
        }

    @classmethod
    def get_kql_queryset_create_request(cls, display_name, kql_database, query_service_uri, queryset_template):
        """Get KQL Queryset Create Request"""
        queryset_template = cls.get_template_file(f"KqlQuerysets//{queryset_template}")

        queryset_json = queryset_template.replace('{KQL_DATABASE_ID}', kql_database['id'])\
                                         .replace('{KQL_DATABASE_NAME}', kql_database['displayName'])\
                                         .replace('{QUERY_SERVICE_URI}', query_service_uri)
        
        return {
            'displayName': display_name,
            'type': "KQLQueryset",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('RealTimeQueryset.json', queryset_json),
                ]
            }
        }

    @classmethod
    def get_variable_library_create_request(cls, display_name, variable_library: VariableLibrary):
        """Get Create Request for Variable Library file"""

        variables_json = variable_library.get_variable_json()

        parts = [
            cls._create_inline_base64_part('variables.json', variables_json)
        ]

        for valueset in variable_library.valuesets:
            path = f"valueSets/{valueset.name}.json"
            content = variable_library.get_valueset_json(valueset.name)
            parts.append( cls._create_inline_base64_part(path, content ) )


        settings_json = cls.get_template_file("VariableLibraries//settings.json")
        
        if len(variable_library.valueSetsOrder) > 0:
            search = '"valueSetsOrder": []'
            sets = json.dumps(variable_library.valueSetsOrder)
            replace = f'"valueSetsOrder": {sets}'
            settings_json.replace(search, replace)
        
        parts.append( cls._create_inline_base64_part('settings.json', settings_json ) )

        return {
            'displayName': display_name,
            'type': "VariableLibrary",
            'definition': {
                'parts': parts
            }
        }

    @classmethod
    def get_update_variable_library_request(cls, variable_library: VariableLibrary):
        """Get Create Request for Variable Library file"""

        variables_json = variable_library.get_variable_json()

        parts = [
            cls._create_inline_base64_part('variables.json', variables_json)
        ]

        for valueset in variable_library.valuesets:
            path = f"valueSets/{valueset.name}.json"
            content = variable_library.get_valueset_json(valueset.name)
            parts.append( cls._create_inline_base64_part(path, content ) )


        settings_json = cls.get_template_file("VariableLibraries//settings.json")
        parts.append( cls._create_inline_base64_part('settings.json', settings_json ) )

        return {
            'definition': {
                'parts': parts
            }
        }

    @classmethod
    def export_item_definitions_from_workspace(cls, workspace_name):
        """Export Item Definiitons from Workspace"""

        AppLogger.log_step(f"Exporting Workspace Item Definitions from {workspace_name}")

        workspace = FabricRestApi.get_workspace_by_name(workspace_name)

        export_response = FabricRestApi.export_item_definitions(workspace['id'])

        cls._write_exported_workspace_to_exports_folder(workspace_name, 'exports.json', export_response)

    @classmethod
    def export_item_definitions_from_workspace_oldway(cls, workspace_name, item_type = None):
        """Export Item Definiitons from Workspace"""

        workspace = FabricRestApi.get_workspace_by_name(workspace_name)

        items = FabricRestApi.list_workspace_items(workspace['id'])

        AppLogger.log_step(f"Exporting Workspace Item Definitions from {workspace_name}")
        
        cls._delete_exports_folder_contents(workspace_name)
        
        for item in items:
            
            items_to_ignore = [ 'SQLEndpoint' ]
            if item['type'] in items_to_ignore:
                continue

            if item_type is not None and item['type'] != item_type:
                continue

            try:
                AppLogger.log_substep(f"Exporting [{item['displayName']}.{item['type']}]")
                item_definition = FabricRestApi.get_item_definition(workspace['id'], item)
                item_folder = f"{item['displayName']}.{item['type']}"
                for part in item_definition['definition']['parts']:
                    part_path = part['path']
                    part_content = part['payload']
                    cls._write_file_to_exports_folder(workspace_name, item_folder, part_path, part_content)
            except:
                AppLogger.log_substep(f"Could not export {item['displayName']}.{item['type']}")

    @classmethod
    def _delete_exports_folder_contents(cls, workspace_name):
        """Delete Exports Folder"""
        folder_path = f".//exports//WorkspaceItemDefinitions//{workspace_name}"
        
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)

    @classmethod
    def _write_file_to_exports_folder(cls, workspace_name, item_name, file_path , file_content, convert_from_base64 = True):
        """Write file to exports folder"""
        if convert_from_base64:
            file_content_bytes = base64.b64decode(file_content)
            file_content = file_content_bytes.decode('utf-8')
 
        #file_path = file_path.replace('/', '\\')
        folder_path = f".//exports//WorkspaceItemDefinitions//{workspace_name}/{item_name}/"
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        full_path = folder_path + file_path
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w', encoding='utf-8') as file:
            file.write(file_content)

    @classmethod
    def _write_exported_workspace_to_exports_folder(cls, workspace_name, file_path , file_content):
        """Write file to exports folder"""
 
        #file_path = file_path.replace('/', '\\')
        folder_path = f".//exports//ExportFromApi//{workspace_name}/"
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        full_path = folder_path + file_path
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w', encoding='utf-8') as file:
            file.write(json.dumps(file_content))