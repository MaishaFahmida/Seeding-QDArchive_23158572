import sqlite3
from pathlib import Path

DB_PATH = Path("23158572-seeding.db")


def main():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 1. Fix invalid file status
    cur.execute("""
        UPDATE files
        SET status = 'FAILED_SERVER_UNRESPONSIVE'
        WHERE status = 'FAILED_SERVER'
    """)

    # 2. Fix invalid UNKNOWN licenses
    # Empty string is better because validator ignores empty trimmed license values
    cur.execute("""
        UPDATE licenses
        SET license = ''
        WHERE TRIM(license) = 'UNKNOWN'
    """)

    conn.commit()

    print("Database fixes applied.\n")

    print("Current FILES.status values:")
    rows = cur.execute("""
        SELECT DISTINCT status
        FROM files
        ORDER BY status
    """).fetchall()
    for (value,) in rows:
        print(f"- {value}")

    print("\nCurrent LICENSES.license values:")
    rows = cur.execute("""
        SELECT DISTINCT license
        FROM licenses
        ORDER BY license
    """).fetchall()
    for (value,) in rows:
        print(f"- {repr(value)}")

    conn.close()


if __name__ == "__main__":
    main()