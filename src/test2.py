

from fabric_devops import DeploymentManager, ItemDefinitionFactory, AppLogger, StagingEnvironments


import re


lakehouse_id = '11111111-1111-1111-1111-111111111111'
lakehouse_name = 'rufus'
workspace_id = '22222222-2222-2222-2222-222222222222'

definition = ItemDefinitionFactory.get_template_file('Notebooks/HelloWorld.py')

find_and_replace={
    r'("default_lakehouse"\s*:\s*)".*"': rf'\1"{lakehouse_id}"',
    r'("default_lakehouse_name"\s*:\s*)".*"': rf'\1"{lakehouse_name}"',
    r'("default_lakehouse_workspace_id"\s*:\s*)".*"': rf'\1"{workspace_id}"',
    r'("known_lakehouses"\s*:\s*)\[[\s\S]*?\]': rf'\1[{{"id": "{lakehouse_id}"}}]',
}


for find, replace in find_and_replace.items():
    definition, count = re.subn(find, replace, definition)

print(definition)