import requests

# --- Configuration ---
NESSIE_API_URL = "http://localhost:19120/api/v1"
BRANCH = "main"


def aggressive_purge():
    # 1. Get current hash
    resp = requests.get(f"{NESSIE_API_URL}/trees/tree/{BRANCH}")
    if resp.status_code != 200:
        print(f"❌ Error: {resp.text}")
        return
    current_hash = resp.json()["hash"]

    # 2. List ALL entries in the branch
    resp = requests.get(f"{NESSIE_API_URL}/trees/tree/{BRANCH}/entries")
    if resp.status_code != 200:
        print(f"❌ Error listing: {resp.text}")
        return

    entries = resp.json().get("entries", [])
    if not entries:
        print("✅ Nessie is already empty!")
        return

    print(f"🧹 Found {len(entries)} entries to purge.")

    # 3. Prepare mass delete
    operations = []
    for e in entries:
        key = e["name"]["elements"]
        operations.append({"type": "DELETE", "key": {"elements": key}})

    commit_payload = {
        "operations": operations,
        "commitMeta": {
            "author": "Antigravity",
            "message": "AGGRESSIVE PURGE - RESET ALL",
        },
    }

    # 4. Commit
    url = f"{NESSIE_API_URL}/trees/branch/{BRANCH}/commit?expectedHash={current_hash}"
    resp = requests.post(url, json=commit_payload)

    if resp.status_code in [200, 204]:
        print("💥 CRITICAL RESET DONE: Nessie is now 100% empty.")
    else:
        print(f"❌ Purge failed: {resp.text}")


if __name__ == "__main__":
    aggressive_purge()
