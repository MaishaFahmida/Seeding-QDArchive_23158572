import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "23158572-sq26-classification.db"

def inspect_database():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return

    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    print("\nTABLES")
    print("=" * 50)

    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type = 'table'
        ORDER BY name
    """)
    tables = [row[0] for row in cursor.fetchall()]

    for table in tables:
        print(f"\nTable: {table}")
        cursor.execute(f"PRAGMA table_info('{table}')")
        columns = cursor.fetchall()
        for column in columns:
            print(f"  {column[0]}: {column[1]} ({column[2]})")

        cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
        count = cursor.fetchone()[0]
        print(f"  Rows: {count}")

    connection.close()

if __name__ == "__main__":
    inspect_database()