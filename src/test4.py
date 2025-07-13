
from fabric_devops import AdoProjectManager


project_name = "Angie"

response = AdoProjectManager.get_pull_requests(project_name)

print(response)