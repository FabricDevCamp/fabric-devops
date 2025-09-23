import requests
import json

from azure.identity import InteractiveBrowserCredential


# Acquire a token
# DO NOT USE IN PRODUCTION.
# Below code to acquire token is to test the User data function endpoint and is for the purpose of development only.
# For production, always register an application in a Microsoft Entra ID tenant and use the appropriate client_id and scopes.


app = InteractiveBrowserCredential()
scp = 'https://analysis.windows.net/powerbi/api/user_impersonation'
result = app.get_token(scp)

if not result.token:
    print('Error:', "Could not get access token")

# Prepare headers
headers = {
    'Authorization': f'Bearer {result.token}',
    'Content-Type': 'application/json'
}

FUNCTION_URL = 'https://ece2dc56-69f0-497d-9066-12c6719d2ab5.zec.userdatafunctions.fabric.microsoft.com/v1/workspaces/ece2dc56-69f0-497d-9066-12c6719d2ab5/userDataFunctions/39d8cd04-1475-4ec8-b328-881c4a9cbdfd/functions/hello_fabric/invoke'

# Prepare the request data
data = '{"name": "John"}' # JSON payload to send to the Azure Function
headers = {
    #  "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
        }

try:   
    # Call the user data function public URL 
    response = requests.post(FUNCTION_URL, json=data, headers=headers)
    response.raise_for_status()
    print(json.dumps(response.json()))
except Exception as e:
    print({"error": str(e)}, 500)

if __name__ == "__main__":
    app.authenticate()