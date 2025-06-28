from __future__ import annotations

"""Backup MoodTrackerBot data from remote Render worker.

1. Downloads the SQLite DB via SSH.
2. Exports the `logs` table to timestamped CSV.
3. Saves both files into ./backups/<YYYYMMDD_HHMMSS>/.

Uses the same SSH credentials as load_data.py:
    SSH_HOST = "ssh.oregon.render.com"
    SSH_USER = "srv-cssvk3ogph6c7399j0gg"
    REMOTE_DB_PATH = "/MoodTrackerBot_data/mood_tracker.db"

Example:
    $ python scripts/backup_data.py
"""

import os
import sqlite3
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Final

import paramiko

# ---------------------------------------------------------------------------
# Config helpers (using credentials from load_data.py)
# ---------------------------------------------------------------------------

SSH_HOST: Final[str] = "ssh.oregon.render.com"
SSH_USER: Final[str] = "srv-cssvk3ogph6c7399j0gg"
SSH_KEY_PATH: Final[str] = str(Path.home() / ".ssh/id_rsa")
REMOTE_DB_PATH: Final[str] = "/MoodTrackerBot_data/mood_tracker.db"

# Output directory ./backups/YYYYMMDD_HHMMSS
BACKUP_ROOT: Final[Path] = Path.cwd() / "backups"
BACKUP_ROOT.mkdir(exist_ok=True)
BACKUP_DIR: Final[Path] = BACKUP_ROOT / datetime.utcnow().strftime("%Y%m%d_%H%M%S")
BACKUP_DIR.mkdir()

LOCAL_DB_PATH: Final[Path] = BACKUP_DIR / "mood_tracker.db"
CSV_PATH: Final[Path] = BACKUP_DIR / "logs.csv"
ARCHIVE_PATH: Final[Path] = BACKUP_ROOT / f"backup_{BACKUP_DIR.name}.tar.gz"


# ---------------------------------------------------------------------------
# SSH download
# ---------------------------------------------------------------------------
print("[1/3] Connecting via SSH …")
ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=SSH_HOST, username=SSH_USER, key_filename=SSH_KEY_PATH)

print(f"     Downloading {REMOTE_DB_PATH} → {LOCAL_DB_PATH}")
sftp = ssh_client.open_sftp()
sftp.get(REMOTE_DB_PATH, str(LOCAL_DB_PATH))
sftp.close()
ssh_client.close()

# ---------------------------------------------------------------------------
# Export logs table
# ---------------------------------------------------------------------------
print("[2/3] Exporting logs table to CSV …")
conn = sqlite3.connect(LOCAL_DB_PATH)
cursor = conn.cursor()

query = "SELECT * FROM logs"
cursor.execute(query)
rows = cursor.fetchall()

import csv
with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    headers = [description[0] for description in cursor.description]
    writer.writerow(headers)
    writer.writerows(rows)

conn.close()

# ---------------------------------------------------------------------------
# Archive
# ---------------------------------------------------------------------------
print("[3/3] Compressing backup…")
with tarfile.open(ARCHIVE_PATH, "w:gz") as tar:
    tar.add(BACKUP_DIR, arcname=BACKUP_DIR.name)

print(f"Backup completed: {ARCHIVE_PATH}")

# Optionally remove uncompressed directory
# shutil.rmtree(BACKUP_DIR) 