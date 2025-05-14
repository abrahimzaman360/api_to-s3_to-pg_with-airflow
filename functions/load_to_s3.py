import requests
import pandas as pd
import boto3
from io import StringIO
from time import sleep
from config.settings import settings

def fetch_and_upload_to_s3(since_id=135, max_users=50):
    # === Step 1: Get list of users ===
    summary_url = f"https://api.github.com/users?since={since_id}&per_page=100"
    summary_resp = requests.get(summary_url)
    summary_resp.raise_for_status()
    users_summary = summary_resp.json()

    # Optional: Limit the number of users
    users_summary = users_summary[:max_users]

    detailed_users = []

    # === Step 2: Get details for each user ===
    for user in users_summary:
        login = user.get("login")
        if not login:
            continue
        
        detail_url = f"https://api.github.com/users/{login}"
        try:
            detail_resp = requests.get(detail_url)
            detail_resp.raise_for_status()
            detailed_users.append(detail_resp.json())
        except Exception as e:
            print(f"⚠️ Failed to fetch user: {login} - {e}")
        
        # Avoid rate-limiting (GitHub API = 60 requests/hour without auth)
        sleep(1)  # or use token + headers to increase limit

    # === Step 3: Convert to DataFrame ===
    df = pd.DataFrame(detailed_users)

    # === Step 4: Upload to S3 ===
    buffer = StringIO()
    df.to_csv(buffer, index=False)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=settings.S3_BUCKET,
        Key=settings.OUTPUT_PATH,
        Body=buffer.getvalue()
    )

    print(f"✅ Uploaded {len(detailed_users)} users to S3: {settings.OUTPUT_PATH}")
