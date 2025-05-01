"""yup"""
import os

if os.getenv('GITHUB_ACTIONS') is True:
    print("running in GitHub")
else:
    print("Not running in GitHub")


    # : true
# RUNNER_ENVIRONMENT: github-hosted