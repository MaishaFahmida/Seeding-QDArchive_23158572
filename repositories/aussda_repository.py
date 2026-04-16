import mimetypes
import time
import requests
from pathlib import Path
from urllib.parse import quote

from database.database import (
    insert_project,
    insert_license,
    insert_keyword,
    insert_person_role,
    insert_file,
)

BASE_URL = "https://data.aussda.at"
SEARCH_URL = f"{BASE_URL}/api/search"

QDA_EXTENSIONS = {
    ".qdpx", ".mx24", ".nvp", ".nvpx", ".qda", ".qdp"
}


def sanitize_filename(name: str) -> str:
    if not name:
        return "downloaded_file"

    bad = '<>:"/\\|?*'
    for ch in bad:
        name = name.replace(ch, "_")

    return name.strip() or "downloaded_file"


def extension_from_content_type(content_type: str) -> str:
    content_type = (content_type or "").split(";")[0].strip().lower()
    guessed = mimetypes.guess_extension(content_type)
    return guessed or ""


def extract_license_value(value):
    if not value:
        return ""

    if isinstance(value, dict):
        return (
            value.get("name")
            or value.get("rightsIdentifier")
            or value.get("uri")
            or str(value)
        )

    return str(value).strip()


def extract_license(latest, citation_fields):
    direct_candidates = [
        latest.get("license"),
        latest.get("termsOfUse"),
        latest.get("termsOfAccess"),
    ]

    for value in direct_candidates:
        extracted = extract_license_value(value)
        if extracted:
            return extracted

    for field in citation_fields:
        if field.get("typeName") in {"license", "termsOfUse", "termsOfAccess"}:
            extracted = extract_license_value(field.get("value"))
            if extracted:
                return extracted

    return ""


def should_download_by_license(raw_license: str) -> bool:
    return bool(raw_license and raw_license.strip())


def search_aussda(query="qualitative", start=0, per_page=25):
    params = {
        "q": query,
        "type": "dataset",
        "start": start,
        "per_page": per_page,
    }

    response = requests.get(SEARCH_URL, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()
    return data.get("data", {}).get("items", [])


def get_dataset_metadata(doi: str):
    url = f"{BASE_URL}/api/datasets/:persistentId/?persistentId={quote(doi, safe='')}"
    response = requests.get(url, timeout=60)

    if response.status_code != 200:
        print(f"Metadata fetch failed for {doi}")
        return None

    payload = response.json()
    data = payload["data"]
    latest = data["latestVersion"]
    citation_fields = latest["metadataBlocks"]["citation"]["fields"]

    title = ""
    description = ""
    keywords = []
    authors = []
    language = ""
    upload_date = latest.get("releaseTime", "")[:10] if latest.get("releaseTime") else ""
    project_url = f"{BASE_URL}/dataset.xhtml?persistentId={doi}"
    license_value = extract_license(latest, citation_fields)

    for field in citation_fields:
        type_name = field.get("typeName")

        if type_name == "title":
            title = field.get("value", "")

        elif type_name == "dsDescription":
            values = field.get("value", [])
            for item in values:
                if isinstance(item, dict):
                    desc_info = item.get("dsDescriptionValue")
                    if isinstance(desc_info, dict):
                        description = desc_info.get("value", "")
                        if description:
                            break

        elif type_name == "author":
            values = field.get("value", [])
            for item in values:
                if isinstance(item, dict):
                    author_info = item.get("authorName")
                    if isinstance(author_info, dict):
                        name = author_info.get("value", "")
                        if name:
                            authors.append(name)

        elif type_name == "keyword":
            values = field.get("value", [])
            for item in values:
                if isinstance(item, dict):
                    keyword_info = item.get("keywordValue")
                    if isinstance(keyword_info, dict):
                        keyword = keyword_info.get("value", "")
                        if keyword:
                            keywords.append(keyword)

        elif type_name == "language":
            value = field.get("value")
            if isinstance(value, str):
                language = value

    files = []
    for f in latest.get("files", []):
        datafile = f.get("dataFile", {})
        file_id = datafile.get("id")
        filename = datafile.get("filename") or f.get("label") or ""
        content_type = datafile.get("contentType", "")
        extension = Path(filename).suffix.lower() or extension_from_content_type(content_type)

        if file_id and filename:
            files.append({
                "file_id": file_id,
                "file_name": filename,
                "file_type": extension,
                "download_url": f"{BASE_URL}/api/access/datafile/{file_id}",
                "content_type": content_type,
                "is_qda_file": extension in QDA_EXTENSIONS,
            })

    return {
        "doi": doi,
        "title": title,
        "description": description,
        "keywords": keywords,
        "authors": authors,
        "license": license_value,
        "language": language,
        "upload_date": upload_date,
        "project_url": project_url,
        "files": files,
    }


def download_file(url: str, destination: Path) -> bool:
    destination.parent.mkdir(parents=True, exist_ok=True)

    try:
        with requests.get(url, stream=True, timeout=180) as response:
            response.raise_for_status()
            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return True
    except Exception:
        return False


def save_aussda_project(metadata: dict, query_string="qualitative") -> int:
    doi = metadata["doi"]
    project_folder_name = doi.replace(":", "_").replace("/", "_")

    project_id = insert_project({
        "query_string": query_string,
        "repository_id": 1,
        "repository_url": BASE_URL,
        "project_url": metadata.get("project_url", ""),
        "version": "",
        "title": metadata.get("title", ""),
        "description": metadata.get("description", ""),
        "language": metadata.get("language", ""),
        "doi": metadata.get("doi", ""),
        "upload_date": metadata.get("upload_date", ""),
        "download_repository_folder": "aussda",
        "download_project_folder": project_folder_name,
        "download_version_folder": "",
        "download_method": "API",
    })

    insert_license(project_id, metadata.get("license", ""))

    for author in metadata.get("authors", []):
        insert_person_role(project_id, author, "AUTHOR")

    for keyword in metadata.get("keywords", []):
        insert_keyword(project_id, keyword)

    return project_id


def process_all_aussda_projects(query="qualitative", per_page=25):
    total_saved = 0
    page = 0

    while True:
        start = page * per_page
        print(f"\n[AUSSDA] Fetching page {page + 1} (start={start})...")

        items = search_aussda(query=query, start=start, per_page=per_page)

        if not items:
            print("AUSSDA: no more data. Stopping.")
            break

        for item in items:
            doi = item.get("global_id", "")
            title = item.get("name", "")

            if not doi:
                continue

            print(f"\n[AUSSDA] Checking: {title}")

            metadata = get_dataset_metadata(doi)
            if not metadata:
                print("Skipped: metadata failed")
                continue

            raw_license = metadata.get("license", "")
            print("License:", raw_license if raw_license else "MISSING")

            if not should_download_by_license(raw_license):
                print("Skipped: no license string found")
                continue

            project_id = save_aussda_project(metadata, query_string=query)

            project_folder = Path("data/downloads/aussda") / doi.replace(":", "_").replace("/", "_")
            project_folder.mkdir(parents=True, exist_ok=True)

            for file_info in metadata.get("files", []):
                file_name = sanitize_filename(file_info["file_name"])
                destination = project_folder / file_name
                success = download_file(file_info["download_url"], destination)

                insert_file(
                    project_id=project_id,
                    file_name=file_name,
                    file_type=file_info.get("file_type", ""),
                    status="SUCCEEDED" if success else "FAILED_SERVER",
                )

                if success:
                    print(f"Downloaded file: {file_name}")
                else:
                    print(f"Failed file: {file_name}")

            total_saved += 1

        page += 1
        time.sleep(1)

    print(f"\nAUSSDA finished. Projects saved: {total_saved}")