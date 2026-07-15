"""
isic_taxonomy.py

Purpose:
    Reads the ISIC Rev. 5 reference file your professor shared
    (isic_rev5_structure.csv) and builds a simple lookup table of just
    the two levels your assignment needs:
        - Section  (1 letter,        e.g. "A")
        - Division (letter + 2 digits, e.g. "A01")

    We ignore Group (letter + 3 digits, e.g. "A011") and
    Class (letter + 4 digits, e.g. "A0111") since those are more detail
    than the assignment asks for.

How the code column works:
    "A"     -> Section        (1 character)
    "A01"   -> Division       (3 characters: 1 letter + 2 digits)
    "A011"  -> Group          (4 characters) -> SKIPPED
    "A0111" -> Class          (5 characters) -> SKIPPED

How to use it:
    from isic_taxonomy import load_taxonomy, get_all_divisions

    taxonomy = load_taxonomy()
    # taxonomy["sections"]  -> {"A": "Agriculture, forestry and fishing", ...}
    # taxonomy["divisions"] -> {"A01": "Crop and animal production...", ...}

Run this file directly to test it against your CSV:
    python isic_taxonomy.py
"""

import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = Path(__file__).resolve().parent / "isic_rev5_structure.csv"


def load_taxonomy(csv_path: Path = CSV_PATH):
    """
    Reads the CSV and returns a dict:
        {
            "sections":  {code: title, ...},   # e.g. {"A": "Agriculture..."}
            "divisions": {code: title, ...},   # e.g. {"A01": "Crop and animal..."}
        }
    """
    sections = {}
    divisions = {}

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Could not find {csv_path}. Make sure you saved the CSV "
            f"as 'isic_rev5_structure.csv' inside your classification folder."
        )

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            code = (row.get("ISIC Rev 5 Code (with Section)") or "").strip()
            title = (row.get("ISIC Rev 5 Title") or "").strip()

            if not code or not title:
                continue  # skip blank / continuation rows

            if len(code) == 1 and code.isalpha():
                # Section, e.g. "A"
                sections[code] = title

            elif len(code) == 3 and code[0].isalpha() and code[1:].isdigit():
                # Division, e.g. "A01"
                divisions[code] = title

            # len == 4 -> Group, len == 5 -> Class: skipped on purpose

    return {"sections": sections, "divisions": divisions}


def get_division_section(division_code: str) -> str:
    """Given a division code like 'A01', return its section letter 'A'."""
    return division_code[0] if division_code else ""


def get_all_divisions(taxonomy=None):
    """
    Returns a flat list of (division_code, division_title, section_code,
    section_title) tuples - handy for building a classifier prompt or a
    dropdown of valid answers.
    """
    if taxonomy is None:
        taxonomy = load_taxonomy()

    results = []
    for div_code, div_title in taxonomy["divisions"].items():
        sec_code = get_division_section(div_code)
        sec_title = taxonomy["sections"].get(sec_code, "UNKNOWN SECTION")
        results.append((div_code, div_title, sec_code, sec_title))

    results.sort(key=lambda x: x[0])
    return results


if __name__ == "__main__":
    taxonomy = load_taxonomy()

    print(f"Sections found: {len(taxonomy['sections'])}")
    print(f"Divisions found: {len(taxonomy['divisions'])}")

    print("\n--- First 5 sections ---")
    for code, title in list(taxonomy["sections"].items())[:5]:
        print(f"  {code}: {title}")

    print("\n--- First 10 divisions (with parent section) ---")
    for div_code, div_title, sec_code, sec_title in get_all_divisions(taxonomy)[:10]:
        print(f"  {div_code} ({sec_code}: {sec_title}) -> {div_title}")