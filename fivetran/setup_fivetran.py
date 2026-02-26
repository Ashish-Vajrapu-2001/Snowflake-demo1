import requests
import yaml
import os
import time

# Configuration
API_KEY    = "{{PLACEHOLDER_FIVETRAN_API_KEY}}"
API_SECRET = "{{PLACEHOLDER_FIVETRAN_API_SECRET}}"
BASE_URL   = "https://api.fivetran.com/v1"

AUTH    = (API_KEY, API_SECRET)
HEADERS = {"Content-Type": "application/json"}


def api_call(method, path, payload=None, retries=3):
    url = BASE_URL + path
    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(method, url, auth=AUTH,
                                    headers=HEADERS, json=payload, timeout=30)
            if resp.status_code == 409 and method == "POST":
                print(f"  [WARN] Object already exists ({path}) — skipping creation")
                return resp.json().get("data", {})
            if resp.status_code == 429:
                wait = 60 * attempt
                print(f"  [WARN] Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            if not resp.ok:
                raise RuntimeError(
                    f"API error {resp.status_code} on {method} {path}: {resp.text}")
            return resp.json().get("data", {})
        except requests.exceptions.RequestException as e:
            if attempt == retries:
                raise RuntimeError(f"Network error after {retries} attempts: {e}")
            time.sleep(5)
    raise RuntimeError(f"Max retries exceeded: {method} {path}")


# STEP 1: Create Fivetran group
print("Step 1: Creating Fivetran group...")
group    = api_call("POST", "/groups", {"name": "Myntra_CLV_Analytics_group"})
group_id = group["id"]
print(f"  group_id = {group_id}")

# STEP 2: Create Snowflake destination
print("Step 2: Creating Snowflake destination...")
api_call("POST", "/destinations", {
    "group_id":         group_id,
    "service":          "snowflake",
    "region":           "US", # Adjust based on actual Snowflake region
    "time_zone_offset": "0",
    "config": {
        "host":          "{{PLACEHOLDER_SNOWFLAKE_ACCOUNT_ID}}.snowflakecomputing.com",
        "port":          443,
        "database":      "BRONZE",
        "schema_prefix": "bronze",
        "warehouse":     "LOADING_WH",
        "auth":          "PASSWORD",
        "user":          "FIVETRAN_USER",
        "password":      "{{PLACEHOLDER_SNOWFLAKE_SVC_PASSWORD}}"
    }
})
print("  Snowflake destination configured")


def create_connector(config_file, group_id):
    with open(config_file, 'r') as f:
        data = yaml.safe_load(f)

    connector_data = data['connector']

    config = connector_data['config'].copy()
    config["schema_prefix"] = connector_data['destination']['schema'].lower()

    payload = {
        "service":            connector_data['service'],
        "group_id":           group_id,
        "config":             config,
        "trust_certificates": True,
        "run_setup_tests":    True
    }

    print(f"Creating connector: {connector_data['name']}...")
    # print(f"Payload: {payload}") # Uncomment for debugging

    response = requests.post(
        f"{BASE_URL}/connectors",
        auth=AUTH,
        headers=HEADERS,
        json=payload
    )

    if response.status_code == 201:
        print(f"Success! Connector ID: {response.json()['data']['id']}")
    else:
        print(f"Failed: {response.text}")


if __name__ == "__main__":
    connectors = [
        "fivetran/Myntra_CLV_ERP_connector.yaml",
        "fivetran/Myntra_CLV_CRM_connector.yaml",
        "fivetran/Myntra_CLV_Marketing_connector.yaml"
    ]

    for c in connectors:
        create_connector(c, group_id)