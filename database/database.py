import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path("23158572-seeding.db")
def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query_string TEXT,
        repository_id INTEGER NOT NULL,
        repository_url TEXT NOT NULL,
        project_url TEXT NOT NULL,
        version TEXT,
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        language TEXT,
        doi TEXT,
        upload_date TEXT,
        download_date TEXT NOT NULL,
        download_repository_folder TEXT NOT NULL,
        download_project_folder TEXT NOT NULL,
        download_version_folder TEXT,
        download_method TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        file_name TEXT NOT NULL,
        file_type TEXT NOT NULL,
        status TEXT NOT NULL,
        FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS keywords (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        keyword TEXT NOT NULL,
        FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS person_role (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS licenses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        license TEXT NOT NULL,
        FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
    )
    """)

    conn.commit()
    conn.close()


def project_exists_by_doi(doi_url: str) -> bool:
    if not doi_url:
        return False

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM projects WHERE doi = ? LIMIT 1", (doi_url,))
    row = cur.fetchone()
    conn.close()
    return row is not None


def insert_project(project_data: dict) -> int:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO projects (
        query_string,
        repository_id,
        repository_url,
        project_url,
        version,
        title,
        description,
        language,
        doi,
        upload_date,
        download_date,
        download_repository_folder,
        download_project_folder,
        download_version_folder,
        download_method
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        project_data.get("query_string"),
        project_data["repository_id"],
        project_data["repository_url"],
        project_data["project_url"],
        project_data.get("version"),
        project_data["title"],
        project_data["description"],
        project_data.get("language"),
        project_data.get("doi"),
        project_data.get("upload_date"),
        project_data["download_date"],
        project_data["download_repository_folder"],
        project_data["download_project_folder"],
        project_data.get("download_version_folder"),
        project_data["download_method"],
    ))

    project_id = cur.lastrowid
    conn.commit()
    conn.close()
    return project_id


def insert_file(project_id: int, file_name: str, file_type: str, status: str) -> int:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO files (project_id, file_name, file_type, status)
    VALUES (?, ?, ?, ?)
    """, (project_id, file_name, file_type, status))

    file_id = cur.lastrowid
    conn.commit()
    conn.close()
    return file_id


def insert_keyword(project_id: int, keyword: str) -> int:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO keywords (project_id, keyword)
    VALUES (?, ?)
    """, (project_id, keyword))

    keyword_id = cur.lastrowid
    conn.commit()
    conn.close()
    return keyword_id


def insert_person_role(project_id: int, name: str, role: str) -> int:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO person_role (project_id, name, role)
    VALUES (?, ?, ?)
    """, (project_id, name, role))

    person_role_id = cur.lastrowid
    conn.commit()
    conn.close()
    return person_role_id


def insert_license(project_id: int, license_value: str) -> int:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO licenses (project_id, license)
    VALUES (?, ?)
    """, (project_id, license_value))

    license_id = cur.lastrowid
    conn.commit()
    conn.close()
    return license_id