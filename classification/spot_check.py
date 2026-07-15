import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "23158572-sq26-classification.db"

connection = sqlite3.connect(DB_PATH)
cursor = connection.cursor()


def show_projects_of_type(project_type, limit=5):
    print(f"\n=== {project_type} (showing up to {limit}) ===")
    cursor.execute(
        "SELECT id, title FROM projects WHERE type = ? LIMIT ?",
        (project_type, limit),
    )
    projects = cursor.fetchall()

    for project_id, title in projects:
        print(f"\nProject {project_id}: {title}")
        cursor.execute(
            """
            SELECT file_name, file_type, status FROM files
            WHERE project_id = ?
            """,
            (project_id,),
        )
        for file_name, file_type, status in cursor.fetchall():
            print(f"   {file_name} ({file_type}) [{status}]")


show_projects_of_type("NOT_A_PROJECT", limit=10)
show_projects_of_type("QD_PROJECT", limit=5)

connection.close()