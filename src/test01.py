"""Setup project with deployment pipeline"""
import os

from fabric_devops import GitHubRestApi

PROJECT_NAME = 'Blutarsky'
SOLUTION_NAME = 'Notebook Solution'
GIT_INTEGRATION_PROVIDER = ''
CREATE_FEATURE_WORKSPACE = False


GitHubRestApi.create_repository("Hanna")
