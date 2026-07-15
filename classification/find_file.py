"""
find_file.py

Purpose:
    Your files are stored in different folder structures depending on the
    repository (AUSSDA = flat, UKDS = deeply nested). Instead of guessing
    the exact path from database columns, this script searches the whole
    data/ folder for a file by its name.

How to use it:
    from find_file import find_file_on_disk
    path = find_file_on_disk("interview_04.pdf")

Run this file directly to test it against your own data/ folder:
    python find_file.py
"""

from pathlib import Path

# Adjust this if your folder layout is different.
# This assumes: classification/find_file.py  and  data/  are siblings
# inside Seeding-QDArchive_23158572/
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"


def find_file_on_disk(file_name: str) -> Path | None:
    """
    Search recursively inside DATA_DIR for a file with this exact name.
    Returns the first match found, or None if nothing matches.
    """
    if not DATA_DIR.exists():
        print(f"WARNING: data folder not found at {DATA_DIR}")
        return None

    matches = list(DATA_DIR.rglob(file_name))

    if not matches:
        return None

    if len(matches) > 1:
        # Not an error, just good to know - some file names might repeat
        # across different projects/repositories.
        print(f"NOTE: '{file_name}' found in {len(matches)} places, using the first one.")

    return matches[0]


def test_against_database():
    """
    Quick self-test: pulls every file_name from your database and checks
    how many can actually be found on disk. Run this once to make sure
    the recursive search works before building extract_text.py on top of it.
    """
    import sqlite3

    db_path = BASE_DIR / "23158572-sq26-classification.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT f.id, f.file_name, f.status, p.id as project_id
        FROM files f
        JOIN projects p ON f.project_id = p.id
        WHERE f.status = 'SUCCEEDED'
    """)
    rows = cursor.fetchall()
    conn.close()

    found = 0
    missing = []

    for file_id, file_name, status, project_id in rows:
        path = find_file_on_disk(file_name)
        if path:
            found += 1
        else:
            missing.append((project_id, file_id, file_name))

    print(f"\n--- Results ---")
    print(f"Total SUCCEEDED files in DB: {len(rows)}")
    print(f"Found on disk: {found}")
    print(f"Missing: {len(missing)}")

    if missing:
        print("\nFirst 10 missing files (project_id, file_id, file_name):")
        for m in missing[:10]:
            print(" ", m)


if __name__ == "__main__":
    test_against_database()