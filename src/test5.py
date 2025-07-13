
import json
from fabric_devops import EntraIdTokenManager, AdoProjectManager


projects = AdoProjectManager.get_projects()

for project in projects:
    AdoProjectManager.delete_project(project['id'])

projects = AdoProjectManager.get_projects()

for project in projects:
    print(f"{project['id']}: {project['name']}")



# project = AdoProjectManager.create_project("TestG")
# print( json.dumps(project, indent=4) )