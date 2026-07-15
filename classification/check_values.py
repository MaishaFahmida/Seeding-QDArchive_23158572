import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "23158572-sq26-classification.db"

connection = sqlite3.connect(DB_PATH)
cursor = connection.cursor()

print("DISTINCT file_type values:")
cursor.execute("SELECT file_type, COUNT(*) FROM files GROUP BY file_type ORDER BY COUNT(*) DESC")
for row in cursor.fetchall():
    print(f"  {row[0]!r}: {row[1]}")

print("\nDISTINCT status values:")
cursor.execute("SELECT status, COUNT(*) FROM files GROUP BY status")
for row in cursor.fetchall():
    print(f"  {row[0]!r}: {row[1]}")

connection.close()