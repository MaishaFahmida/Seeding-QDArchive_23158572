import sys
import os
import json
from pathlib import Path

from repositories.ukds_repository import (
    get_ukds_record_by_doi,
    parse_ukds_record,
    save_ukds_project,
    save_folder_files_to_db,
    insert_file,
    enrich_ukds_license,
)

sys.path.append(os.path.dirname(os.path.dirname(__file__)))


from downloader.downloader import (
    download_ukds_file,
    extract_zip,
    delete_zip,
)


def doi_to_folder_name(doi: str) -> str:
    return doi.replace("/", "_").replace(":", "_")


def find_metadata_by_doi(target_doi: str):
    try:
        record = get_ukds_record_by_doi(target_doi)
        if not record:
            return None

        return parse_ukds_record(record)

    except Exception as e:
        print("Direct DOI lookup failed:", e)
        return None


def process_batch(json_path: str):
    with open(json_path, "r", encoding="utf-8") as f:
        items = json.load(f)

    for item in items:
        doi = item["doi"]
        signed_url = item["download_url"]

        print(f"\nProcessing: {doi}")

        metadata = find_metadata_by_doi(doi)
        if not metadata:
            print("Metadata not found")
            continue

        metadata = enrich_ukds_license(metadata)   

        project_id = save_ukds_project(metadata, query_string=doi)

        base_folder = Path("data/downloads/ukds") / doi_to_folder_name(doi)
        zip_path = base_folder / "original.zip"
        extract_folder = base_folder / "files"

        success = download_ukds_file(signed_url, str(zip_path))

        if not success:
            insert_file(
                project_id=project_id,
                file_name="original.zip",
                file_type=".zip",
                status="FAILED_SERVER",
            )
            continue

        extracted = extract_zip(str(zip_path), str(extract_folder))
        if not extracted:
            insert_file(
                project_id=project_id,
                file_name="original.zip",
                file_type=".zip",
                status="FAILED_SERVER",
            )
            continue

        delete_zip(str(zip_path))

        save_folder_files_to_db(project_id, str(extract_folder))

        print(f"Finished: {doi}")


if __name__ == "__main__":
    process_batch("data/ukds_download_list.json")