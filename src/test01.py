"""Setup project with deployment pipeline"""
import os

from fabric_devops import AdoProjectManager

PROJECT_NAME = 'Blutarsky'
SOLUTION_NAME = 'Notebook Solution'
GIT_INTEGRATION_PROVIDER = ''
CREATE_FEATURE_WORKSPACE = False


AdoProjectManager.create_project("Beans")
