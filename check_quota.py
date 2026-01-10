from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import os

def check_quota():
    json_keyfile = 'service_account.json'
    if not os.path.exists(json_keyfile):
        print("service_account.json not found.")
        return

    creds = Credentials.from_service_account_file(
        json_keyfile, 
        scopes=['https://www.googleapis.com/auth/drive']
    )
    service = build('drive', 'v3', credentials=creds)

    try:
        about = service.about().get(fields="storageQuota, user").execute()
        quota = about.get('storageQuota', {})
        user = about.get('user', {})
        
        print(f"Service Account Email: {user.get('emailAddress')}")
        print("--- Storage Quota ---")
        print(f"Limit: {quota.get('limit', 'Unlimited')}")
        print(f"Usage: {quota.get('usage', '0')}")
        print(f"Usage (In Trash): {quota.get('usageInDriveTrash', '0')}")
        
    except Exception as e:
        print(f"Error checking quota: {e}")

if __name__ == "__main__":
    check_quota()
