import json
import os
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi, StagingEnvironments



dj = StagingEnvironments.get_prod_environment()

print( dj.parameters[dj.adls_account_key_parameter] )