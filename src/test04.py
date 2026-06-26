"""Setup project with fabric-cicd and release flow """
import json
import os

from fabric_devops import ItemDefinitionFactory, FabricRestApi

os.system("cls")

FabricRestApi.export_item_definitions('source1')
