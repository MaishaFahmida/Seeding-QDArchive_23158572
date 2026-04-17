import sqlite3
from pathlib import Path

DB_PATH = Path("23158572-seeding.db")


def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def get_duplicate_doi_groups(conn):
    return conn.execute("""
        SELECT doi
        FROM projects
        WHERE doi IS NOT NULL AND TRIM(doi) <> ''
        GROUP BY doi
        HAVING COUNT(*) > 1
        ORDER BY doi
    """).fetchall()


def get_project_ids_for_doi(conn, doi):
    rows = conn.execute("""
        SELECT id
        FROM projects
        WHERE doi = ?
        ORDER BY id
    """, (doi,)).fetchall()
    return [row[0] for row in rows]


def move_child_rows(conn, from_project_id, to_project_id):
    for table in ["files", "keywords", "person_role", "licenses"]:
        conn.execute(
            f"UPDATE {table} SET project_id = ? WHERE project_id = ?",
            (to_project_id, from_project_id)
        )


def delete_project(conn, project_id):
    conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))


def remove_duplicate_child_rows(conn):
    conn.execute("""
        DELETE FROM keywords
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM keywords
            GROUP BY project_id, keyword
        )
    """)

    conn.execute("""
        DELETE FROM person_role
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM person_role
            GROUP BY project_id, name, role
        )
    """)

    conn.execute("""
        DELETE FROM licenses
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM licenses
            GROUP BY project_id, license
        )
    """)

    conn.execute("""
        DELETE FROM files
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM files
            GROUP BY project_id, file_name, file_type, status
        )
    """)


def main():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return

    conn = connect()
    try:
        groups = get_duplicate_doi_groups(conn)
        if not groups:
            print("No duplicate DOI URLs found.")
            return

        total_deleted_projects = 0

        for (doi,) in groups:
            project_ids = get_project_ids_for_doi(conn, doi)
            keep_id = project_ids[0]
            delete_ids = project_ids[1:]

            print(f"\nDOI: {doi}")
            print(f"Keep project id: {keep_id}")
            print(f"Delete project ids: {delete_ids}")

            for duplicate_id in delete_ids:
                move_child_rows(conn, duplicate_id, keep_id)
                delete_project(conn, duplicate_id)
                total_deleted_projects += 1

        remove_duplicate_child_rows(conn)
        conn.commit()

        print(f"\nDone. Deleted duplicate projects: {total_deleted_projects}")
        print("Merged child rows and removed duplicate child entries.")

    except Exception as exc:
        conn.rollback()
        print(f"Error: {exc}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()