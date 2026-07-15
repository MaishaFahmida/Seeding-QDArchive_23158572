import sqlite3
from pathlib import Path
from classify_project_type import classify_project_type

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "23158572-sq26-classification.db"


def run_classification():
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    cursor.execute("SELECT id FROM projects")
    project_ids = [row[0] for row in cursor.fetchall()]

    counts = {
        "QDA_PROJECT": 0,
        "QD_PROJECT": 0,
        "OTHER_PROJECT": 0,
        "NOT_A_PROJECT": 0,
    }

    for project_id in project_ids:
        cursor.execute(
            """
            SELECT file_type FROM files
            WHERE project_id = ? AND status = 'SUCCEEDED'
            """,
            (project_id,),
        )
        file_types = [row[0] for row in cursor.fetchall()]

        project_type = classify_project_type(file_types)
        counts[project_type] += 1

        cursor.execute(
            "UPDATE projects SET type = ? WHERE id = ?",
            (project_type, project_id),
        )

    connection.commit()
    connection.close()

    print("Classification complete.")
    print("=" * 40)
    for project_type, count in counts.items():
        print(f"{project_type}: {count}")


if __name__ == "__main__":
    run_classification()