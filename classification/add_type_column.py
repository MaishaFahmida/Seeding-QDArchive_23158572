import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "23158572-sq26-classification.db"

connection = sqlite3.connect(DB_PATH)
cursor = connection.cursor()

cursor.execute("PRAGMA table_info(projects)")
columns = [col[1] for col in cursor.fetchall()]

if "type" not in columns:
    cursor.execute("ALTER TABLE projects ADD COLUMN type TEXT")
    connection.commit()
    print("Column 'type' added to projects table.")
else:
    print("Column 'type' already exists.")

connection.close()