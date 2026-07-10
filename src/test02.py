"""Setup project with fabric-cicd and release flow """
import json
import os

from fabric_devops import AdoProjectManager, DeploymentManager

os.system('cls')


project_name = 'Apollo'
AdoProjectManager.run_pipeline(project_name, 'deploy-to-test-staging-workspace', 'main')
