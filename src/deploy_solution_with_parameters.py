"""yup"""
import os

if os.getenv('GITHUB_ACTIONS') == 'true':
    print("running in GitHub")
else:
    print("Not running in GitHub")


print(os.getenv('GITHUB_ACTIONS'))
print(os.getenv('RUNNER_ENVIRONMENT'))
print(os.getenv('ENV_714'))
