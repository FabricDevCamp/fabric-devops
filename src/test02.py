"""Setup project with fabric-cicd and release flow """
import json
import subprocess

from fabric_devops import FabricRestApi
subprocess.run('cls', shell=True)


project_name = 'Apollo'
FabricRestApi.create_workspace(project_name)