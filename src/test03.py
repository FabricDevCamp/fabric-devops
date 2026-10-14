import json
from github import Auth, GithubIntegration
from fabric_devops import GitHubRestApi

response = GitHubRestApi.get_user_id_by_name('TedAtDevCamp')
print(json.dumps(response, indent=4))