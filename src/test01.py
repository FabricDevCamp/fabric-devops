"""Deploy solution to new workspace"""
import json


from fabric_devops import  AdoProjectManager

project_name = 'Billy Jean'

AdoProjectManager.create_environment(project_name, 'dev')
AdoProjectManager.create_environment(project_name, 'test')
AdoProjectManager.create_environment(project_name, 'prod')

AdoProjectManager.add_approval_to_environment(project_name, 'prod', 'ted@fabricdevcamp.net')
