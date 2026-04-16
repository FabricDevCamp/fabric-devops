"""Deploy solution to new workspace"""
import json


from fabric_devops import  AdoProjectManager

project_name = 'Product Sales'

AdoProjectManager.add_branch_policy_to_require_approvals(project_name, 'main')
