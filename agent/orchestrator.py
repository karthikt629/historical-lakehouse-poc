import os
import sys
import json
import yaml
import subprocess
import time
from datetime import datetime
from github import Github
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
GITHUB_REPO  = os.environ["GITHUB_REPO"]
CONFIG_PATH  = os.path.join(os.path.dirname(__file__), "../sync-config.yaml")
SCRIPT_PATH  = os.path.join(os.path.dirname(__file__), "../scripts/convert_to_parquet.py")

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ── Pre-flight check ──────────────────────────────────────────────────────────
log("PRE-FLIGHT: Checking HDFS connectivity...")
result = subprocess.run(
    ["hdfs", "dfs", "-ls", "/user/hive/warehouse"],
    capture_output=True, text=True
)
if result.returncode != 0:
    log("PRE-FLIGHT FAILED: Cannot reach HDFS. Exiting.")
    sys.exit(1)
log("PRE-FLIGHT: HDFS reachable.")

log("PRE-FLIGHT: Validating source paths from config...")
with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

for db in config["databases"]:
    for table in db["tables"]:
        path = f"{db['hdfs_path']}/{table['name']}"
        check = subprocess.run(
            ["hdfs", "dfs", "-ls", path],
            capture_output=True, text=True
        )
        if check.returncode != 0:
            log(f"PRE-FLIGHT FAILED: Path not found: {path}")
            sys.exit(1)
        log(f"PRE-FLIGHT: Found {path}")

log("PRE-FLIGHT: All checks passed. Proceeding.\n")

# ── Step 1: Run PySpark conversion ───────────────────────────────────────────
log("STEP 1: Starting PySpark conversion HDFS DBs → Parquet sync DB...")
spark_result = subprocess.run(
    ["spark-submit", "--master", "local[*]", SCRIPT_PATH, CONFIG_PATH],
    text=True
)
if spark_result.returncode != 0:
    log("STEP 1 FAILED: PySpark job failed. Exiting.")
    sys.exit(1)
log("STEP 1: PySpark conversion complete.")

# ── Read row count metadata ───────────────────────────────────────────────────
with open("/tmp/row_counts.json") as f:
    row_counts = json.load(f)

all_ok = all(v["status"] == "OK" for v in row_counts.values())
if not all_ok:
    log("STEP 1 WARNING: Row count mismatch detected. Check /tmp/row_counts.json.")
else:
    log("STEP 1: All row counts validated OK.\n")

# ── Step 2: Generate DMS task definition ─────────────────────────────────────
log("STEP 2: Generating DMS task definition...")
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
task_filename = f"dms-tasks/sync-task-{timestamp}.yaml"

task_definition = {
    "task_id": f"sync-task-{timestamp}",
    "created_at": timestamp,
    "source": {
        "type": "hdfs_parquet",
        "path": config["sync_db_path"]
    },
    "target": {
        "type": "s3_iceberg",
        "s3_path": "s3://your-lakehouse-bucket/sync-db/",
        "database": "lakehouse_db",
        "format": "iceberg"
    },
    "migration_mode": "full-load",
    "tables": []
}

for db in config["databases"]:
    for table in db["tables"]:
        key = f"{db['name']}.{table['name']}"
        task_definition["tables"].append({
            "source_db": db["name"],
            "table_name": table["name"],
            "parquet_path": f"{config['sync_db_path']}/{table['name']}",
            "row_count": row_counts.get(key, {}).get("source_count", 0)
        })

task_yaml = yaml.dump(task_definition, default_flow_style=False)

# Save locally too
os.makedirs("dms-tasks", exist_ok=True)
with open(task_filename, "w") as f:
    f.write(task_yaml)

log(f"STEP 2: Task definition generated → {task_filename}\n")

# ── Step 2: Open GitHub PR ────────────────────────────────────────────────────
log("STEP 2: Opening GitHub PR...")
g    = Github(GITHUB_TOKEN)
repo = g.get_repo(GITHUB_REPO)

branch_name = f"auto/sync-task-{timestamp}"
main_sha    = repo.get_branch("main").commit.sha

# Create branch
repo.create_git_ref(f"refs/heads/{branch_name}", main_sha)
log(f"STEP 2: Created branch {branch_name}")

# Commit task definition file to branch
repo.create_file(
    path=task_filename,
    message=f"[auto] Add DMS sync task definition {timestamp}",
    content=task_yaml,
    branch=branch_name
)

# Create PR
total_rows = sum(v["source_count"] for v in row_counts.values())
pr_body = f"""## Automated Data Sync Task

**Triggered by:** git push  
**Timestamp:** {timestamp}  
**Total tables:** {len(task_definition['tables'])}  
**Total rows synced:** {total_rows:,}

### Tables included:
"""
for t in task_definition["tables"]:
    pr_body += f"- `{t['source_db']}.{t['table_name']}` — {t['row_count']:,} rows\n"

pr_body += """
### Next step:
Merge this PR to trigger Step 3 — DMS task creation and run in AWS.
"""

pr = repo.create_pull(
    title=f"[Auto] Data sync task: Hadoop → Lakehouse ({timestamp})",
    body=pr_body,
    head=branch_name,
    base="main"
)

log(f"STEP 2: PR created successfully!")
log(f"STEP 2: PR URL → {pr.html_url}")
log("\n========== Agent completed Steps 1 and 2 ==========")
log("Waiting for PR review and merge to proceed to Step 3 (AWS DMS).")
