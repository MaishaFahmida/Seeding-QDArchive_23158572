import sqlite3
from pathlib import Path
from collections import Counter

DB_PATH = Path("23158572-seeding.db")

ALLOWED_FILE_STATUS = {
    "SUCCEEDED",
    "FAILED_SERVER_UNRESPONSIVE",
    "FAILED_LOGIN_REQUIRED",
    "FAILED_TOO_LARGE",
}

ALLOWED_PERSON_ROLES = {
    "UPLOADER",
    "AUTHOR",
    "OWNER",
    "OTHER",
    "UNKNOWN",
}


def connect():
    return sqlite3.connect(DB_PATH)


def print_section(title: str):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def show_duplicates_by_doi(conn):
    print_section("Duplicate DOI URLs in projects")
    rows = conn.execute("""
        SELECT doi, COUNT(*) AS cnt
        FROM projects
        WHERE doi IS NOT NULL AND TRIM(doi) <> ''
        GROUP BY doi
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC, doi
    """).fetchall()

    if not rows:
        print("No duplicate DOI URLs found.")
        return

    for doi, cnt in rows:
        print(f"{cnt}x  {doi}")


def show_missing_required_fields(conn):
    print_section("Projects with missing required fields")
    rows = conn.execute("""
        SELECT id, title, repository_url, project_url, description, download_date,
               download_repository_folder, download_project_folder, download_method
        FROM projects
        WHERE repository_url IS NULL OR TRIM(repository_url) = ''
           OR project_url IS NULL OR TRIM(project_url) = ''
           OR title IS NULL OR TRIM(title) = ''
           OR description IS NULL
           OR download_date IS NULL OR TRIM(download_date) = ''
           OR download_repository_folder IS NULL OR TRIM(download_repository_folder) = ''
           OR download_project_folder IS NULL OR TRIM(download_project_folder) = ''
           OR download_method IS NULL OR TRIM(download_method) = ''
        ORDER BY id
    """).fetchall()

    if not rows:
        print("No missing required project fields found.")
        return

    for row in rows[:50]:
        print(row)
    if len(rows) > 50:
        print(f"... and {len(rows) - 50} more")


def show_invalid_file_status(conn):
    print_section("Invalid FILES.status values")
    rows = conn.execute("""
        SELECT DISTINCT status
        FROM files
        WHERE status IS NOT NULL AND TRIM(status) <> ''
        ORDER BY status
    """).fetchall()

    values = [r[0] for r in rows]
    invalid = [v for v in values if v not in ALLOWED_FILE_STATUS]

    if not invalid:
        print("All file status values are valid.")
        return

    for value in invalid:
        print(value)


def show_invalid_roles(conn):
    print_section("Invalid PERSON_ROLE.role values")
    rows = conn.execute("""
        SELECT DISTINCT role
        FROM person_role
        WHERE role IS NOT NULL AND TRIM(role) <> ''
        ORDER BY role
    """).fetchall()

    values = [r[0] for r in rows]
    invalid = [v for v in values if v not in ALLOWED_PERSON_ROLES]

    if not invalid:
        print("All person roles are valid.")
        return

    for value in invalid:
        print(value)


def show_orphans(conn):
    print_section("Orphan child rows")
    checks = {
        "files": "SELECT COUNT(*) FROM files f LEFT JOIN projects p ON f.project_id = p.id WHERE p.id IS NULL",
        "keywords": "SELECT COUNT(*) FROM keywords k LEFT JOIN projects p ON k.project_id = p.id WHERE p.id IS NULL",
        "person_role": "SELECT COUNT(*) FROM person_role pr LEFT JOIN projects p ON pr.project_id = p.id WHERE p.id IS NULL",
        "licenses": "SELECT COUNT(*) FROM licenses l LEFT JOIN projects p ON l.project_id = p.id WHERE p.id IS NULL",
    }

    for table, sql in checks.items():
        count = conn.execute(sql).fetchone()[0]
        print(f"{table}: {count}")


def show_duplicate_child_rows(conn):
    print_section("Duplicate child rows")

    queries = {
        "keywords": """
            SELECT project_id, keyword, COUNT(*) AS cnt
            FROM keywords
            GROUP BY project_id, keyword
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC, project_id
            LIMIT 20
        """,
        "person_role": """
            SELECT project_id, name, role, COUNT(*) AS cnt
            FROM person_role
            GROUP BY project_id, name, role
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC, project_id
            LIMIT 20
        """,
        "licenses": """
            SELECT project_id, license, COUNT(*) AS cnt
            FROM licenses
            GROUP BY project_id, license
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC, project_id
            LIMIT 20
        """,
        "files": """
            SELECT project_id, file_name, file_type, status, COUNT(*) AS cnt
            FROM files
            GROUP BY project_id, file_name, file_type, status
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC, project_id
            LIMIT 20
        """,
    }

    for table, sql in queries.items():
        print(f"\n{table}:")
        rows = conn.execute(sql).fetchall()
        if not rows:
            print("  none")
            continue
        for row in rows:
            print(" ", row)


def show_summary(conn):
    print_section("Summary counts")

    for table in ["projects", "files", "keywords", "person_role", "licenses"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"{table}: {count}")

    print("\nFILES.status distribution:")
    rows = conn.execute("""
        SELECT status, COUNT(*)
        FROM files
        GROUP BY status
        ORDER BY COUNT(*) DESC, status
    """).fetchall()
    for status, count in rows:
        print(f"{status}: {count}")

    print("\nPERSON_ROLE.role distribution:")
    rows = conn.execute("""
        SELECT role, COUNT(*)
        FROM person_role
        GROUP BY role
        ORDER BY COUNT(*) DESC, role
    """).fetchall()
    for role, count in rows:
        print(f"{role}: {count}")


def main():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return

    conn = connect()
    try:
        show_summary(conn)
        show_duplicates_by_doi(conn)
        show_missing_required_fields(conn)
        show_invalid_file_status(conn)
        show_invalid_roles(conn)
        show_orphans(conn)
        show_duplicate_child_rows(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()