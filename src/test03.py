from github import Auth, GithubIntegration
from fabric_devops import EnvironmentSettings

Organization = EnvironmentSettings.ORGANIZATION_GITHUB
PEM_CERTIFICATE = EnvironmentSettings.PRIVATE_KEY_GITHUB
APP_ID = EnvironmentSettings.APP_ID_GITHUB
PRIVATE_KEY = EnvironmentSettings.PRIVATE_KEY_GITHUB
INSTALLATION_ID = EnvironmentSettings.INSTALLATION_ID_GITHUB

auth = Auth.AppAuth(APP_ID, PRIVATE_KEY)
gi = GithubIntegration(auth=auth)
client = gi.get_github_for_installation(int(INSTALLATION_ID))

# access_token_result = gi.get_access_token(INSTALLATION_ID)
# access_token = access_token_result.token
# print("accesstoken:")
# print(access_token)
org = client.get_organization(Organization)
repos = org.get_repos()

rl = list(repos)

print(rl)