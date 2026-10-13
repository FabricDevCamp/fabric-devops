"""Setup project with fabric-cicd and release flow """
import json
import subprocess

from fabric_devops import GitHubRestApi
subprocess.run('cls', shell=True)


repos = GitHubRestApi.get_github_repositories()
print( repos )
