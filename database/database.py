import sqlite3
from datetime import datetime, UTC
from config import DB_PATH


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_string TEXT,
            repository_id INTEGER NOT NULL,
            repository_url TEXT NOT NULL,
            project_url TEXT NOT NULL,
            version TEXT,
            title TEXT NOT NULL,
            description TEXT,
            language TEXT,
            doi TEXT,
            upload_date DATE,
            download_date TIMESTAMP NOT NULL,
            download_repository_folder TEXT NOT NULL,
            download_project_folder TEXT NOT NULL,
            download_version_folder TEXT,
            download_method TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            file_type TEXT NOT NULL,
            status TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            keyword TEXT NOT NULL,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS person_role (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            role TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS licenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            license TEXT NOT NULL,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        )
    """)

    conn.commit()
    conn.close()


def insert_project(project):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
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
        project.get("query_string", ""),
        project.get("repository_id", 1),
        project.get("repository_url", "https://data.aussda.at"),
        project.get("project_url", ""),
        project.get("version", ""),
        project.get("title", ""),
        project.get("description", ""),
        project.get("language", ""),
        project.get("doi", ""),
        project.get("upload_date", ""),
        datetime.now(UTC).isoformat(),
        project.get("download_repository_folder", "aussda"),
        project.get("download_project_folder", ""),
        project.get("download_version_folder", ""),
        project.get("download_method", "API"),
    ))

    project_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return project_id


def insert_license(project_id, license_value):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO licenses (project_id, license) VALUES (?, ?)",
        (project_id, license_value)
    )
    conn.commit()
    conn.close()


def insert_keyword(project_id, keyword):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO keywords (project_id, keyword) VALUES (?, ?)",
        (project_id, keyword)
    )
    conn.commit()
    conn.close()


def insert_person_role(project_id, name, role="AUTHOR"):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO person_role (project_id, name, role) VALUES (?, ?, ?)",
        (project_id, name, role)
    )
    conn.commit()
    conn.close()


def insert_file(project_id, file_name, file_type, status):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO files (project_id, file_name, file_type, status) VALUES (?, ?, ?, ?)",
        (project_id, file_name, file_type, status)
    )
    conn.commit()
    conn.close()