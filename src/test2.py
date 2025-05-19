"""Test 2"""

import os

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger, AppSettings, \
                          VariableLibrary

def deploy_realtime_solution():
    """Deploy Real Time Solution"""

    AppLogger.log_job("Deploying Real Time Solution")

    workspace_name = "Custom Realtime Solution"
    eventhouse_name = "Eventhouse01"
    kql_database_name = "KqlDb01"
    eventstream_name = "Eventstream01"
    realtime_dashboard_name = "RealtimeDashboard01"
    semantic_model_name = 'Product Sales Imported Model'
    report_name = 'Product Sales Summary'

    workspace = FabricRestApi.create_workspace(workspace_name)

    create_eventhouse_request = \
        ItemDefinitionFactory.get_eventhouse_create_request(eventhouse_name)
    
    eventhouse_item = FabricRestApi.create_item(workspace['id'], create_eventhouse_request)

    eventhouse = FabricRestApi.get_eventhouse(workspace['id'], eventhouse_item['id'])

    query_service_uri = eventhouse['properties']['queryServiceUri']
    ingestion_service_uri  = eventhouse['properties']['ingestionServiceUri']


    create_kql_database_request = \
        ItemDefinitionFactory.get_kql_database_create_request(kql_database_name, 
                                                              eventhouse)
    
    kql_database = FabricRestApi.create_item(workspace['id'], create_kql_database_request)

    create_eventstream_request = \
        ItemDefinitionFactory.get_eventstream_create_request(eventstream_name,
                                                             workspace['id'],
                                                             eventhouse['id'],
                                                             kql_database)
    
    eventstream = FabricRestApi.create_item(workspace['id'], create_eventstream_request)

    realtime_dashboard_create_request = ItemDefinitionFactory.get_kql_dashboard_create_request(
        realtime_dashboard_name,
        workspace['id'],
        kql_database,
        query_service_uri)
    
    realtime_dashboard = FabricRestApi.create_item(workspace['id'], realtime_dashboard_create_request)

    create_queryset1_create_request = ItemDefinitionFactory.get_kql_queryset_create_request(
        'Queryset1', kql_database, query_service_uri, 'RealTimeQueryset1.json'
    )

    queryset1 = FabricRestApi.create_item(workspace['id'], create_queryset1_create_request)

    create_queryset2_create_request = ItemDefinitionFactory.get_kql_queryset_create_request(
        'Queryset2', kql_database, query_service_uri, 'RealTimeQueryset2.json'
    )

    queryset2 = FabricRestApi.create_item(workspace['id'], create_queryset2_create_request)



    # web_url = FabricRestApi.get_web_url_from_semantic_model(workspace['id'], model['id'])

    # AppLogger.log_substep(f'Creating anonymous Web connection to {web_url} ')

    # connection = FabricRestApi.create_anonymous_web_connection(web_url, workspace)

    # FabricRestApi.bind_semantic_model_to_connection(workspace['id'], model['id'], connection['id'])

    # FabricRestApi.refresh_semantic_model(workspace['id'], model['id'])

    # create_report_request = \
    #     ItemDefinitionFactory.get_report_create_request(model['id'],
    #                                                     report_name,
    #                                                     'product_sales_summary.json')

    # FabricRestApi.create_item(workspace['id'], create_report_request)

    AppLogger.log_job_ended("Solution deployment complete")


deploy_realtime_solution()