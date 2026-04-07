import requests
import hmac
import hashlib
import json

# Your settings
WEBHOOK_URL = "http://localhost:8000/patreon/callback/webhook/" # Update to your actual URL
WEBHOOK_SECRET = "test_secret_123"
# FIND THIS IN YOUR ADMIN PANEL: Pick a patreon_id from an existing PatronProfile
TARGET_PATREON_ID = "PASTE_YOUR_PATREON_ID_HERE" 

payload = {
    "data": {
        "attributes": {
            "currently_entitled_amount_cents": 1500,
            "patron_status": "active_patron"
        },
        "id": TARGET_PATREON_ID,
        "type": "member"
    }
}

body = json.dumps(payload).encode('utf-8')
signature = hmac.new(WEBHOOK_SECRET.encode('utf-8'), body, hashlib.md5).hexdigest()

headers = {
    "X-Patreon-Signature": signature,
    "X-Patreon-Event": "members:update",
    "Content-Type": "application/json"
}

response = requests.post(WEBHOOK_URL, data=body, headers=headers)

print(f"Status: {response.status_code}")
print(f"Response: {response.text}")