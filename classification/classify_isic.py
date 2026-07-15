"""
classify_isic.py

Purpose:
    The "orchestrator" script. Ties together everything built so far:
        - find_file.py       (locate files on disk)
        - extract_text.py    (read text out of files)
        - isic_classifier.py (match text to an ISIC division)

    For every QD_PROJECT in your database, this script:
        1. Classifies the WHOLE PROJECT (title + description + keywords +
           text from ALL its successfully-downloaded primary data files).
        2. Classifies EACH PRIMARY DATA FILE individually.
    Results are saved into a new "classifications" table.

    NOT_A_PROJECT and OTHER_PROJECT are skipped (per the assignment,
    and also because OTHER_PROJECT count is 0 in your dataset anyway).

How to use it:
    python classify_isic.py

    Safe to re-run: it clears old rows in "classifications" first, so
    re-running won't create duplicates.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from find_file import find_file_on_disk
from extract_text import extract_text_from_file, SUPPORTED_EXTENSIONS
from isic_classifier import IsicClassifier, CLASSIFIER_VERSION

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "23158572-sq26-classification.db"

# Extensions we treat as "primary data files" for QD_PROJECT purposes -
# matches the PRIMARY_DATA_EXTENSIONS used in Step 1's classify_project_type.py
PRIMARY_DATA_EXTENSIONS = {".pdf", ".rtf", ".txt", ".doc", ".docx"}

# Filename patterns that indicate administrative/documentation files
# rather than real qualitative content (interview transcripts, articles,
# etc). These files often talk ABOUT data/formats/methods, which makes
# them confidently (and wrongly) match unrelated ISIC divisions like
# "Computing infrastructure, data processing". They still count as
# primary files for Part 1 (PROJECT_TYPE), but are excluded here since
# they're not the real qualitative content we want to classify.
ADMIN_FILENAME_PATTERNS = [
    "file_information", "fileinformation", "technical_report",
    "readme", "read_me", "codebook", "code_book", "documentation",
    "user_guide", "userguide", "data_dictionary", "datadictionary",
    "study_information", "studyinformation",
]


def is_administrative_file(file_name: str) -> bool:
    """True if the filename looks like documentation/metadata rather
    than real qualitative content (interview, article, transcript etc)."""
    name_lower = (file_name or "").lower()
    return any(pattern in name_lower for pattern in ADMIN_FILENAME_PATTERNS)


def ensure_classifications_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS classifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_type TEXT NOT NULL,   -- 'PROJECT' or 'FILE'
            target_id TEXT NOT NULL,     -- project.id or file.id
            project_id INTEGER,          -- always filled, for easy joins/exports
            primary_class TEXT,
            secondary_class TEXT,
            confidence REAL,
            evidence TEXT,
            classifier_version TEXT,
            classified_at TEXT
        )
    """)
    conn.commit()


def clear_previous_results(conn):
    """Safe to re-run the script: wipe old results first."""
    conn.execute("DELETE FROM classifications")
    conn.commit()


def get_qd_projects(conn):
    cursor = conn.execute("""
        SELECT id, title, description
        FROM projects
        WHERE type = 'QD_PROJECT'
    """)
    return cursor.fetchall()


def get_keywords_for_project(conn, project_id):
    try:
        cursor = conn.execute(
            "SELECT keyword FROM keywords WHERE project_id = ?", (project_id,)
        )
        return [row[0] for row in cursor.fetchall() if row[0]]
    except sqlite3.OperationalError:
        # In case the keywords table has a different column name -
        # don't let this break the whole run.
        return []


def get_succeeded_files(conn, project_id):
    cursor = conn.execute("""
        SELECT id, file_name, file_type
        FROM files
        WHERE project_id = ? AND status = 'SUCCEEDED'
    """, (project_id,))
    return cursor.fetchall()


def extract_text_cached(file_name, cache):
    """Avoid re-extracting the same file twice (some file names repeat)."""
    if file_name in cache:
        return cache[file_name]

    path = find_file_on_disk(file_name)
    text, status = extract_text_from_file(path)
    cache[file_name] = (text, status)
    return text, status


def run():
    conn = sqlite3.connect(DB_PATH)
    ensure_classifications_table(conn)
    clear_previous_results(conn)

    classifier = IsicClassifier()
    text_cache = {}  # file_name -> (text, status), avoids re-reading files

    projects = get_qd_projects(conn)
    print(f"Found {len(projects)} QD_PROJECT projects to classify.\n")

    now = datetime.now(timezone.utc).isoformat()

    for project_id, title, description in projects:
        print(f"--- Project {project_id}: {title[:60]!r} ---")

        keywords = get_keywords_for_project(conn, project_id)
        files = get_succeeded_files(conn, project_id)

        # Only primary data files count toward text-based classification -
        # and we exclude administrative/documentation files (readmes,
        # codebooks, file_information sheets), since those describe the
        # dataset rather than being real qualitative content themselves.
        primary_files = [
            (fid, fname, ftype) for fid, fname, ftype in files
            if (ftype or "").strip().lower() in PRIMARY_DATA_EXTENSIONS
            and not is_administrative_file(fname)
        ]

        # --- 1. Classify individual files FIRST, then build the PROJECT
        # classification from a confidence-weighted vote across them ---
        # (This fixes a real dilution bug: naively concatenating all file
        # text equally let ~150 short, noisy essay files drown out one
        # genuinely strong, confident file match. Voting lets strong
        # signals count more than weak ones, instead of everything being
        # treated as equally important regardless of quality.)

        combined_metadata = " ".join([
            title or "",
            description or "",
            " ".join(keywords),
        ])
        metadata_result = classifier.classify(combined_metadata)

        file_results = {}  # file_id -> (file_name, result dict)
        for file_id, file_name, file_type in primary_files:
            text, status = extract_text_cached(file_name, text_cache)
            if status != "OK":
                continue
            file_result = classifier.classify_weighted(title or "", text)
            file_results[file_id] = (file_name, file_result)

        # Weighted vote: each file's own confidence is its vote weight.
        # The project's own title/description/keywords gets a strong
        # fixed weight (0.5) since it's short but highly reliable -
        # roughly as influential as several confident files combined.
        #
        # IMPORTANT: the vote WEIGHT decides which class wins, but the
        # reported confidence is the AVERAGE of the actual underlying
        # cosine-similarity confidences that voted for the winner - not
        # the vote's share of the total. Vote-share is misleading with
        # few voters (a single voter always "wins" 100% of an empty
        # contest, even if its own match was weak) - averaging the real
        # confidences keeps the number meaningful and comparable to the
        # file-level confidences you already reviewed.
        METADATA_VOTE_WEIGHT = 0.5
        votes = {}  # primary_class string -> accumulated vote weight
        contributing_confidences = {}  # primary_class string -> [confidences]

        if metadata_result["primary_class"]:
            pc = metadata_result["primary_class"]
            votes[pc] = votes.get(pc, 0) + METADATA_VOTE_WEIGHT
            contributing_confidences.setdefault(pc, []).append(
                metadata_result["confidence"]
            )
        for file_name, file_result in file_results.values():
            pc = file_result["primary_class"]
            if pc:
                votes[pc] = votes.get(pc, 0) + file_result["confidence"]
                contributing_confidences.setdefault(pc, []).append(
                    file_result["confidence"]
                )

        if votes:
            ranked_votes = sorted(votes.items(), key=lambda x: x[1], reverse=True)
            top_class, top_weight = ranked_votes[0]
            second_class, second_weight = (
                ranked_votes[1] if len(ranked_votes) > 1 else (None, 0)
            )
            real_confidences = contributing_confidences[top_class]
            avg_confidence = round(
                sum(real_confidences) / len(real_confidences), 3
            )
            project_result = {
                "primary_class": top_class,
                "secondary_class": (
                    second_class if second_weight >= top_weight * 0.6 else None
                ),
                "confidence": avg_confidence,
                "evidence": (
                    f"Weighted vote across metadata + {len(file_results)} "
                    f"classified files ({len(real_confidences)} voted for "
                    f"the winner, avg confidence {avg_confidence})"
                ),
                "classifier_version": CLASSIFIER_VERSION,
            }
        else:
            project_result = metadata_result

        print(f"  PROJECT -> {project_result['primary_class']} "
              f"(confidence {project_result['confidence']})")

        conn.execute("""
            INSERT INTO classifications
            (target_type, target_id, project_id, primary_class,
             secondary_class, confidence, evidence, classifier_version,
             classified_at)
            VALUES ('PROJECT', ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(project_id), project_id,
            project_result["primary_class"], project_result["secondary_class"],
            project_result["confidence"], project_result["evidence"],
            project_result["classifier_version"], now,
        ))

        # --- 2. Classify EACH primary data file individually ---
        # (includes admin files too, but they get a clearly-marked
        # "skipped" row instead of being silently absent from the table)
        admin_files = [
            (fid, fname, ftype) for fid, fname, ftype in
            [(fid, fname, ftype) for fid, fname, ftype in files
             if (ftype or "").strip().lower() in PRIMARY_DATA_EXTENSIONS]
            if is_administrative_file(fname)
        ]
        for file_id, file_name, file_type in admin_files:
            conn.execute("""
                INSERT INTO classifications
                (target_type, target_id, project_id, primary_class,
                 secondary_class, confidence, evidence,
                 classifier_version, classified_at)
                VALUES ('FILE', ?, ?, NULL, NULL, 0, ?, ?, ?)
            """, (
                str(file_id), project_id,
                "Skipped: administrative/documentation file, not primary "
                "qualitative content",
                "keyword-tfidf-v2", now,
            ))

        # Files where text extraction failed get their own row too
        extraction_failed_files = [
            (fid, fname) for fid, fname, ftype in primary_files
            if fid not in file_results
        ]
        for file_id, file_name in extraction_failed_files:
            _, status = extract_text_cached(file_name, text_cache)
            conn.execute("""
                INSERT INTO classifications
                (target_type, target_id, project_id, primary_class,
                 secondary_class, confidence, evidence,
                 classifier_version, classified_at)
                VALUES ('FILE', ?, ?, NULL, NULL, 0, ?, ?, ?)
            """, (
                str(file_id), project_id,
                f"Text extraction failed: {status}",
                "keyword-tfidf-v2", now,
            ))

        # Insert the FILE-level results already computed above (Step 1) -
        # no need to re-extract or re-classify, keeps this consistent
        # with the vote that decided the project-level result.
        for file_id, (file_name, file_result) in file_results.items():
            conn.execute("""
                INSERT INTO classifications
                (target_type, target_id, project_id, primary_class,
                 secondary_class, confidence, evidence, classifier_version,
                 classified_at)
                VALUES ('FILE', ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(file_id), project_id,
                file_result["primary_class"], file_result["secondary_class"],
                file_result["confidence"], file_result["evidence"],
                file_result["classifier_version"], now,
            ))

        conn.commit()
        print()

    print("Done. Results saved to the 'classifications' table.")

    # Quick summary
    cursor = conn.execute("""
        SELECT target_type, COUNT(*) FROM classifications GROUP BY target_type
    """)
    print("\n--- Summary ---")
    for target_type, count in cursor.fetchall():
        print(f"  {target_type}: {count} rows")

    conn.close()


if __name__ == "__main__":
    run()