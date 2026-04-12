"""Deploy solution to new workspace"""
import json


from fabric_devops import  AdoProjectManager

PROJECT_NAME = 'Bruce'

# repo_name = PROJECT_NAME


# AdoProjectManager.create_project(PROJECT_NAME)
# AdoProjectManager.create_environment(PROJECT_NAME, 'dev')
# AdoProjectManager.create_environment(PROJECT_NAME, 'test')
# AdoProjectManager.create_environment(PROJECT_NAME, 'prod')


response = AdoProjectManager.add_approval_to_environment(PROJECT_NAME, 'prod', 'ted@fabricdevcamp.net')

print( json.dumps(response, indent=4))
