import argparse
from pathlib import Path

from config import DOWNLOADS_DIR, DEFAULT_LIMIT_RECORDS, QDA_EXTENSIONS, QUAL_EXTENSIONS
from db import init_db, insert_file_row, now_iso_utc
from utils import safe_slug, sha256_file
from zenodo_client import ZenodoClient

def parse_license(meta_license):
    """
    Zenodo license can be dict or string or None.
    """
    if meta_license is None:
        return None
    if isinstance(meta_license, dict):
        return meta_license.get("id") or meta_license.get("title")
    if isinstance(meta_license, str):
        return meta_license
    return str(meta_license)

def file_is_interesting(filename: str) -> bool:
    """
    Keep QDA formats and common qualitative formats.
    (We still download all files in the record, but this can be used later if you want filtering.)
    """
    suffix = Path(filename).suffix.lower()
    return suffix in QDA_EXTENSIONS or suffix in QUAL_EXTENSIONS

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--query",
        default='(qdpx OR qpdx OR nvivo OR "atlas.ti" OR maxqda OR "qualitative interview")',
        help="Zenodo query"
    )
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT_RECORDS, help="Number of Zenodo records (datasets) to download")
    ap.add_argument("--downloads_dir", default=str(DOWNLOADS_DIR), help="Where to store dataset folders")
    args = ap.parse_args()

    init_db()
    client = ZenodoClient()

    downloads_root = Path(args.downloads_dir)
    downloads_root.mkdir(parents=True, exist_ok=True)

    records = client.iter_records(query=args.query, max_records=args.limit)

    downloaded_records = 0
    for rec in records:
        rec_id = str(rec.get("id") or "")
        links = rec.get("links") or {}
        dataset_url = links.get("html")

        meta = rec.get("metadata") or {}
        title = meta.get("title") or "untitled"
        description = meta.get("description")
        doi = meta.get("doi") or rec.get("doi")
        license_str = parse_license(meta.get("license"))

        # One folder per dataset/record
        dataset_folder = downloads_root / safe_slug(f"{rec_id}-{title}")
        dataset_folder.mkdir(parents=True, exist_ok=True)

        files = rec.get("files") or []
        if not files:
            continue

        any_success = False

        for fmeta in files:
            key = fmeta.get("key") or "file.bin"
            file_links = fmeta.get("links") or {}
            file_url = file_links.get("self")
            if not file_url:
                continue

            out_path = dataset_folder / safe_slug(key, max_len=180)

            try:
                size_bytes = client.download_file(file_url, out_path)
                file_hash = sha256_file(out_path)
                ts = now_iso_utc()

                row = {
                    # REQUIRED fields
                    "file_url": file_url,
                    "download_timestamp": ts,
                    "local_directory": str(dataset_folder),
                    "local_filename": out_path.name,

                    # OPTIONAL / useful fields
                    "source_name": "Zenodo",
                    "dataset_url": dataset_url,
                    "record_id": rec_id,
                    "title": title,
                    "description": description,
                    "doi": doi,
                    "license": license_str,

                    "file_key": key,
                    "file_type": out_path.suffix.lower().lstrip("."),
                    "size_bytes": size_bytes,
                    "sha256": file_hash,
                }
                insert_file_row(row)
                any_success = True

            except Exception as e:
                # Keep going; record partial downloads are still useful
                print(f"[WARN] Failed file: record={rec_id} key={key} error={e}")

        if any_success:
            downloaded_records += 1
            print(f"[OK] Dataset folder created: {dataset_folder}")
        else:
            print(f"[WARN] No files downloaded for record={rec_id}")

    print(f"\nDone. Downloaded {downloaded_records} dataset folders into: {downloads_root}")
    print(f"SQLite DB: data/qdarchive.db")

if __name__ == "__main__":
    main()