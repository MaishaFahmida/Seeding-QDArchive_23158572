from pathlib import Path

# Project root = repo root (assuming src/ is directly under root)
ROOT_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = ROOT_DIR / "data"
DOWNLOADS_DIR = DATA_DIR / "downloads" / "zenodo"
EXPORTS_DIR = DATA_DIR / "exports"

DB_PATH = DATA_DIR / "database" / "qdarchive.db"

# Stop after N datasets (records)
DEFAULT_LIMIT_RECORDS = 5

# A small set of file extensions that are common in QDA / qualitative workflows
QDA_EXTENSIONS = {
    ".qdpx", ".qpdx", ".nvpx", ".nvp", ".atlasti", ".atlproj", ".atlpx",
    ".mx20", ".maxqda", ".mqd", ".project", ".rqda", ".qda",
}

# Also allow typical qualitative primary-data formats
QUAL_EXTENSIONS = {
    ".txt", ".md", ".doc", ".docx", ".odt", ".pdf", ".rtf",
    ".csv", ".xlsx", ".xls", ".json", ".xml",
}