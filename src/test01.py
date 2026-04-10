"""Deploy solution to new workspace"""
import time


from fabric_devops import  GitHubRestApi

PROJECT_NAME = 'Product Sales'

repo_name = 'Product-Sales'


repo = GitHubRestApi.add_protection_ruleset_for_branch(repo_name, 'main')
