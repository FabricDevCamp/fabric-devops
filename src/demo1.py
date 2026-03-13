from fabric_devops import EnvironmentSettings, AppLogger, FabricRestApi, GitHubRestApi

repo_name = 'Biggle'

def get_latest_test_branch(branches):
    """Get latest test branch"""
    branches = GitHubRestApi.list_branches(
        repo_name
    )
    test_branch_names = []

    for branch in branches:
        branch_name = branch['name']
        if branch_name.startswith('test'):
            test_branch_names.append(branch_name)
            
    if len(test_branch_names) == 0:
        return None
    
    test_branch_names.sort(reverse=True)
    
    return test_branch_names[0]

def get_latest_prod_branch(branches):
    """Get latest prod branch"""
    branches = GitHubRestApi.list_branches(
        repo_name
    )
    prod_branch_names = []

    for branch in branches:
        branch_name = branch['name']
        if branch_name.startswith('prod'):
            prod_branch_names.append(branch_name)
            
    if len(prod_branch_names) == 0:
        return None
    
    prod_branch_names.sort(reverse=True)
    
    return prod_branch_names[0]

AppLogger.log_job("Creating and deploying prod build")

REPO_NAME = repo_name

latest_test_branch = get_latest_test_branch(REPO_NAME)
if latest_test_branch is None:
    raise(RuntimeError("No test branches found. Please create a test branch before creating a prod branch."))
    
latest_prod_branch = get_latest_prod_branch(REPO_NAME)
if latest_prod_branch == latest_test_branch.replace('test', 'prod'):
    raise(RuntimeError("The latest test branch has already been promoted to a prod branch."))
else:
    new_prod_branch = latest_test_branch.replace('test', 'prod')
    print(f'Creating prod branch: {new_prod_branch} from test branch: {latest_test_branch}')
    GitHubRestApi.create_branch(REPO_NAME, new_prod_branch, latest_test_branch)
    FabricRestApi.update_workspace_description(
        EnvironmentSettings.WORKSPACE_ID_PROD, 
        f"BUILD: {new_prod_branch}")
