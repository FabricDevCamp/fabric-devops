"""Setup project with fabric-cicd and release flow """
import json
import os

from fabric_devops import DeploymentManager,  AppLogger, StagingEnvironments,\
                          FabricRestApi, AdoProjectManager, GitHubRestApi 

RUN_CLEANUP_ENVIRONMENT = os.getenv("RUN_CLEANUP_ENVIRONMENT") == 'true'
SETUP_DEMO1 = os.getenv("SETUP_DEMO1") == 'true'

if RUN_CLEANUP_ENVIRONMENT:
    AppLogger.log_job("Running cleanup environment")
    DeploymentManager.cleanup_dev_environment()

if SETUP_DEMO1:
    AppLogger.log_job("Setting up Demo 1")
