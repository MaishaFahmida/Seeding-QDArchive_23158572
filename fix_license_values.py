import sqlite3
from pathlib import Path

DB_PATH = Path("23158572-seeding.db")


def main():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Fix invalid file status
    cur.execute("""
        UPDATE files
        SET status = 'FAILED_SERVER_UNRESPONSIVE'
        WHERE status = 'FAILED_SERVER'
    """)

    # Fix invalid license values
    # UNKNOWN should become empty string so validator skips it
    cur.execute("""
        UPDATE licenses
        SET license = ''
        WHERE TRIM(license) = 'UNKNOWN'
    """)

    conn.commit()

    print("Updated invalid FILES.status and LICENSES.license values.\n")

    print("Distinct FILES.status values:")
    for (value,) in cur.execute("SELECT DISTINCT status FROM files ORDER BY status"):
        print(f" - {value}")

    print("\nDistinct LICENSES.license values:")
    for (value,) in cur.execute("SELECT DISTINCT license FROM licenses ORDER BY license"):
        print(f" - {repr(value)}")

    conn.close()


if __name__ == "__main__":
    main()