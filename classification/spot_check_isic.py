"""
spot_check_isic.py

Purpose:
    Quick sanity-check tool. Prints a handful of FILE-level classification
    results (project title + file name + what it was classified as) so you
    can eyeball whether they look reasonable before finalizing everything.

How to use it:
    python spot_check_isic.py
    python spot_check_isic.py --project-id 8      (see all files for one project)
    python spot_check_isic.py --sample 15          (see a bigger random sample)
"""

import sqlite3
import sys
import random
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "23158572-sq26-classification.db"


def get_arg(flag, default=None, cast=str):
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return cast(sys.argv[idx + 1])
    return default


def run():
    project_id_filter = get_arg("--project-id", None, int)
    sample_size = get_arg("--sample", 10, int)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    query = """
        SELECT
            c.target_id AS file_id,
            c.project_id,
            p.title AS project_title,
            f.file_name,
            c.primary_class,
            c.secondary_class,
            c.confidence,
            c.evidence
        FROM classifications c
        JOIN projects p ON p.id = c.project_id
        JOIN files f ON f.id = c.target_id
        WHERE c.target_type = 'FILE'
    """
    params = ()

    if project_id_filter is not None:
        query += " AND c.project_id = ?"
        params = (project_id_filter,)

    rows = conn.execute(query, params).fetchall()
    conn.close()

    if not rows:
        print("No FILE-level classifications found. Did you run classify_isic.py yet?")
        return

    if project_id_filter is None:
        rows = random.sample(rows, min(sample_size, len(rows)))

    print(f"Showing {len(rows)} file-level classification(s):\n")

    for row in rows:
        flag = " [LOW CONFIDENCE]" if "[LOW CONFIDENCE" in (row["evidence"] or "") else ""
        print(f"Project {row['project_id']}: {row['project_title'][:50]}")
        print(f"  File: {row['file_name']}")
        print(f"  -> {row['primary_class']}{flag}")
        if row["secondary_class"]:
            print(f"     (secondary: {row['secondary_class']})")
        print(f"     confidence: {row['confidence']}")
        print()


if __name__ == "__main__":
    run()