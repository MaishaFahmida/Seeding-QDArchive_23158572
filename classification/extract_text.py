"""
extract_text.py

Purpose:
    Opens a file (pdf, rtf, txt, docx) and pulls out the plain text inside it.
    If something goes wrong, it tells you WHY instead of just failing silently.

How to use it:
    from extract_text import extract_text_from_file
    text, status = extract_text_from_file(Path("some_file.pdf"))

    - text   -> the extracted text (empty string if extraction failed)
    - status -> "OK" if it worked, otherwise one of:
                UNSUPPORTED_FORMAT, EMPTY_FILE, CORRUPT_FILE,
                NO_EXTRACTABLE_TEXT, ENCRYPTED_FILE

Run this file directly to test it against every file findable via find_file.py:
    python extract_text.py
"""

from pathlib import Path

from find_file import find_file_on_disk, DATA_DIR, BASE_DIR

import sqlite3

# --- Supported extensions -----------------------------------------------

SUPPORTED_EXTENSIONS = {".pdf", ".rtf", ".txt", ".docx"}


def extract_text_from_file(path: Path):
    """
    Given a Path to a file, return (text, status).
    status is "OK" on success, or a failure code string on failure.
    """
    if path is None or not path.exists():
        return "", "CORRUPT_FILE"  # file vanished / bad path

    ext = path.suffix.lower()

    if ext not in SUPPORTED_EXTENSIONS:
        return "", "UNSUPPORTED_FORMAT"

    # Check for empty file up front
    if path.stat().st_size == 0:
        return "", "EMPTY_FILE"

    try:
        if ext == ".pdf":
            return _extract_pdf(path)
        elif ext == ".rtf":
            return _extract_rtf(path)
        elif ext == ".txt":
            return _extract_txt(path)
        elif ext == ".docx":
            return _extract_docx(path)
    except Exception as e:
        # Catch-all: anything unexpected counts as corrupt for now
        print(f"  Unexpected error on {path.name}: {e}")
        return "", "CORRUPT_FILE"

    return "", "UNSUPPORTED_FORMAT"


def _extract_pdf(path: Path):
    import pdfplumber

    try:
        with pdfplumber.open(path) as pdf:
            if pdf.is_encrypted:
                # Many "encrypted" PDFs only have an owner password (restricts
                # printing/editing) but NO real password needed to read the
                # text. Try to unlock with an empty password before giving up.
                recovered = _try_extract_with_empty_password(path)
                if recovered is not None:
                    return recovered, "OK"
                return "", "ENCRYPTED_FILE"

            pages_text = []
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    pages_text.append(page_text)

            full_text = "\n".join(pages_text).strip()

            if not full_text:
                return "", "NO_EXTRACTABLE_TEXT"  # e.g. scanned image PDF

            return full_text, "OK"

    except Exception as e:
        msg = str(e).lower()
        if "encrypt" in msg or "password" in msg:
            recovered = _try_extract_with_empty_password(path)
            if recovered is not None:
                return recovered, "OK"
            return "", "ENCRYPTED_FILE"
        return "", "CORRUPT_FILE"


def _try_extract_with_empty_password(path: Path):
    """
    Attempts to open a PDF flagged as encrypted using pypdf with an empty
    password. This works for the very common case where a PDF has an
    "owner password" (restricting printing/editing) but no real password
    needed to read the content.

    Returns the extracted text (string) if successful, or None if it
    truly needs a real password / can't be read.
    """
    try:
        from pypdf import PdfReader

        reader = PdfReader(path)

        if reader.is_encrypted:
            result = reader.decrypt("")  # try empty password
            if result == 0:  # 0 = decrypt failed, needs a real password
                return None

        pages_text = []
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                pages_text.append(page_text)

        full_text = "\n".join(pages_text).strip()
        return full_text if full_text else None

    except Exception:
        return None


def _extract_rtf(path: Path):
    from striprtf.striprtf import rtf_to_text

    try:
        raw = path.read_text(encoding="utf-8", errors="ignore")
        text = rtf_to_text(raw).strip()

        if not text:
            return "", "NO_EXTRACTABLE_TEXT"

        return text, "OK"

    except Exception:
        return "", "CORRUPT_FILE"


def _extract_txt(path: Path):
    try:
        text = path.read_text(encoding="utf-8", errors="ignore").strip()
        if not text:
            return "", "NO_EXTRACTABLE_TEXT"
        return text, "OK"
    except Exception:
        return "", "CORRUPT_FILE"


def _extract_docx(path: Path):
    import docx

    try:
        doc = docx.Document(path)
        text = "\n".join(p.text for p in doc.paragraphs).strip()
        if not text:
            return "", "NO_EXTRACTABLE_TEXT"
        return text, "OK"
    except Exception:
        return "", "CORRUPT_FILE"


# --- Self-test against your real database --------------------------------

def test_against_database():
    """
    Runs extraction on every SUCCEEDED file in the database that has a
    supported extension, and prints a summary of how many succeeded vs
    failed, broken down by failure reason.
    """
    db_path = BASE_DIR / "23158572-sq26-classification.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT f.id, f.file_name, f.file_type, p.id as project_id
        FROM files f
        JOIN projects p ON f.project_id = p.id
        WHERE f.status = 'SUCCEEDED'
    """)
    rows = cursor.fetchall()
    conn.close()

    status_counts = {}
    examples_of_failures = {}

    checked = 0
    for file_id, file_name, file_type, project_id in rows:
        ext = (file_type or "").strip().lower()
        if ext not in SUPPORTED_EXTENSIONS:
            continue  # skip .xml, .zip, etc. - not text-extractable anyway

        checked += 1
        path = find_file_on_disk(file_name)
        text, status = extract_text_from_file(path)

        status_counts[status] = status_counts.get(status, 0) + 1

        if status != "OK" and status not in examples_of_failures:
            examples_of_failures[status] = (project_id, file_id, file_name)

    print(f"\n--- Extraction Results ---")
    print(f"Files checked (pdf/rtf/txt/docx only): {checked}")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")

    if examples_of_failures:
        print("\nExample of each failure type (project_id, file_id, file_name):")
        for status, example in examples_of_failures.items():
            print(f"  {status}: {example}")


if __name__ == "__main__":
    test_against_database()