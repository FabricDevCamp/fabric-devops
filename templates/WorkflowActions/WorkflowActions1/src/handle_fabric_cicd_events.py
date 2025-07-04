"""Handle Fabric CICD Events"""

import os
import json
MESSAGE = 'Hello World fom the Fabric CICD Evet Handler'

print(MESSAGE)

print('')
print('GITHUB_CONTEXT:')

context = json.loads(os.getenv('GITHUB_CONTEXT'))

print(f"event_name: {context['event_name']}")

print(f"ref: {context['ref']}")

print(f"sha: {context['sha']}")

print(f"workflow: {context['workflow']}")

print(f"job: {context['job']}")

if 'event' in context and 'master_branch' in context['event']:
    print(f"event master_branch: {context['event']['master_branch']}")


print( json.dumps(context, indent=4 ) )

