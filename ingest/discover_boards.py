# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Discover Monday.com Board IDs
# MAGIC
# MAGIC Task 1 of the dynamic ingestion pipeline.
# MAGIC
# MAGIC - Queries Monday.com API for all accessible boards
# MAGIC - Publishes `board_ids` as a Task Value for downstream tasks

# COMMAND ----------

import sys
repo_root = "/Repos/junior.esteca@databricks.com/lakeflow-monday-connector"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

import requests

api_token = dbutils.secrets.get(scope="monday", key="api_token")

def fetch_all_board_ids(api_token: str) -> list[str]:
    """Fetch all accessible board IDs from Monday.com using pagination."""
    url = "https://api.monday.com/v2"
    headers = {
        "Authorization": api_token,
        "Content-Type": "application/json",
        "API-Version": "2024-01",
    }
    board_ids = []
    page = 1
    page_size = 50

    while True:
        query = """
        query($page: Int!, $limit: Int!) {
            boards(page: $page, limit: $limit, state: active, order_by: created_at) {
                id
                name
                state
            }
        }
        """
        resp = requests.post(
            url,
            headers=headers,
            json={"query": query, "variables": {"page": page, "limit": page_size}},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        boards = data.get("data", {}).get("boards", [])
        if not boards:
            break

        for b in boards:
            print(f"  Found board: {b['id']} — {b['name']} [{b['state']}]")
            board_ids.append(str(b["id"]))

        if len(boards) < page_size:
            break
        page += 1

    return board_ids

# COMMAND ----------

import json

print("Discovering Monday.com boards...")
board_ids = fetch_all_board_ids(api_token)
board_ids_str = ",".join(board_ids)

print(f"\nTotal boards found: {len(board_ids)}")
print(f"Board IDs: {board_ids_str}")

# Publish as comma-separated string — consumed by dynamic pipeline (managed/external)
dbutils.jobs.taskValues.set(key="board_ids", value=board_ids_str)
dbutils.jobs.taskValues.set(key="board_count", value=str(len(board_ids)))

# Publish as JSON array — consumed by ForEach pipeline (managed_foreach/external_foreach)
# ForEach task requires the input to be a JSON array: ["id1","id2",...]
dbutils.jobs.taskValues.set(key="board_ids_json", value=json.dumps(board_ids))
