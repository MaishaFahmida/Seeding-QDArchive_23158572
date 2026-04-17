import mimetypes
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests

from database.database import (
    insert_project,
    insert_license,
    insert_keyword,
    insert_person_role,
    insert_file,
    project_exists_by_doi,
)

BASE_URL = "https://data.aussda.at"
SEARCH_URL = f"{BASE_URL}/api/search"

QDA_EXTENSIONS = {
    ".qdpx",
    ".mx24",
    ".nvp",
    ".nvpx",
    ".qda",
    ".qdp",
}

ALLOWED_FILE_STATUS = {
    "SUCCEEDED",
    "FAILED_SERVER_UNRESPONSIVE",
    "FAILED_LOGIN_REQUIRED",
    "FAILED_TOO_LARGE",
}

ALLOWED_PERSON_ROLES = {
    "UPLOADER",
    "AUTHOR",
    "OWNER",
    "OTHER",
    "UNKNOWN",
}


def sanitize_filename(name: str) -> str:
    if not name:
        return "downloaded_file"

    bad = '<>:"/\\|?*'
    for ch in bad:
        name = name.replace(ch, "_")

    return name.strip() or "downloaded_file"


def clean_text(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()


def unique_clean_list(values):
    seen = set()
    result = []

    for value in values or []:
        cleaned = clean_text(value)
        if not cleaned:
            continue

        key = cleaned.lower()
        if key not in seen:
            seen.add(key)
            result.append(cleaned)

    return result


def extension_from_content_type(content_type: str) -> str:
    content_type = (content_type or "").split(";")[0].strip().lower()
    guessed = mimetypes.guess_extension(content_type)
    if guessed:
        return guessed.lstrip(".").lower()
    return ""


def normalize_license(raw_license: str) -> str:
    text = clean_text(raw_license)
    lower = text.lower()

    if not text:
        return ""

    # messy HTML-like extracted content
    if "<li>" in lower or "</li>" in lower or "<ul>" in lower or "</ul>" in lower:
        if "sharealike" in lower or "cc by-sa" in lower:
            return "CC BY-SA 4.0"
        if "creative commons attribution" in lower or "cc by" in lower:
            return "CC BY 4.0"
        return ""

    # AUSSDA mixed text
    if "aussda scientific use licence" in lower and "cc by" in lower:
        return "CC BY 4.0"

    # common CC variants
    if "creative commons attribution-sharealike" in lower or "cc by-sa" in lower:
        return "CC BY-SA 4.0"

    if "creative commons attribution" in lower or "cc by 4.0" in lower or lower == "cc by":
        return "CC BY 4.0"

    if "cc by-nc-nd" in lower:
        return "CC BY-NC-ND"

    if "cc by-nc" in lower:
        return "CC BY-NC"

    if "cc by-nd" in lower:
        return "CC BY-ND"

    if "cc0" in lower:
        return "CC0"

    if "odbl-1.0" in lower:
        return "ODbL-1.0"

    if "odbl" in lower:
        return "ODbL"

    if "odc-by-1.0" in lower:
        return "ODC-By-1.0"

    if "odc-by" in lower:
        return "ODC-By"

    if "pddl" in lower:
        return "PDDL"

    # unrecognized -> empty string, NOT UNKNOWN
    return ""


def extract_license_value(value):
    if not value:
        return ""

    if isinstance(value, dict):
        return clean_text(
            value.get("name")
            or value.get("rightsIdentifier")
            or value.get("uri")
            or str(value)
        )

    return clean_text(value)


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


def to_doi_url(doi: str) -> str:
    doi = clean_text(doi)
    if not doi:
        return ""
    if doi.startswith("http://") or doi.startswith("https://"):
        return doi
    return f"https://doi.org/{doi}"


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
    raw_license = extract_license(latest, citation_fields)

    for field in citation_fields:
        type_name = field.get("typeName")

        if type_name == "title":
            title = clean_text(field.get("value", ""))

        elif type_name == "dsDescription":
            values = field.get("value", [])
            for item in values:
                if isinstance(item, dict):
                    desc_info = item.get("dsDescriptionValue")
                    if isinstance(desc_info, dict):
                        description = clean_text(desc_info.get("value", ""))
                        if description:
                            break

        elif type_name == "author":
            values = field.get("value", [])
            for item in values:
                if isinstance(item, dict):
                    author_info = item.get("authorName")
                    if isinstance(author_info, dict):
                        name = clean_text(author_info.get("value", ""))
                        if name:
                            authors.append(name)

        elif type_name == "keyword":
            values = field.get("value", [])
            for item in values:
                if isinstance(item, dict):
                    keyword_info = item.get("keywordValue")
                    if isinstance(keyword_info, dict):
                        keyword = clean_text(keyword_info.get("value", ""))
                        if keyword:
                            keywords.append(keyword)

        elif type_name == "language":
            value = field.get("value")
            if isinstance(value, str):
                language = clean_text(value)

    files = []
    for f in latest.get("files", []):
        datafile = f.get("dataFile", {})
        file_id = datafile.get("id")
        filename = clean_text(datafile.get("filename") or f.get("label") or "")
        content_type = clean_text(datafile.get("contentType", ""))

        suffix = Path(filename).suffix.lower()
        if suffix:
            file_type = suffix.lstrip(".")
        else:
            file_type = extension_from_content_type(content_type)

        if file_id and filename:
            files.append(
                {
                    "file_id": file_id,
                    "file_name": filename,
                    "file_type": file_type,
                    "download_url": f"{BASE_URL}/api/access/datafile/{file_id}",
                    "content_type": content_type,
                    "is_qda_file": Path(filename).suffix.lower() in QDA_EXTENSIONS,
                }
            )

    return {
        "doi": doi,
        "doi_url": to_doi_url(doi),
        "title": clean_text(title),
        "description": clean_text(description),
        "keywords": unique_clean_list(keywords),
        "authors": unique_clean_list(authors),
        "license": normalize_license(raw_license),
        "language": clean_text(language),
        "upload_date": clean_text(upload_date),
        "project_url": clean_text(project_url),
        "files": files,
    }


def download_file(url: str, destination: Path) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)

    try:
        with requests.get(url, stream=True, timeout=180) as response:
            response.raise_for_status()

            content_length = response.headers.get("Content-Length")
            if content_length:
                try:
                    size_bytes = int(content_length)
                    if size_bytes > 1024 * 1024 * 1024 * 5:
                        return "FAILED_TOO_LARGE"
                except ValueError:
                    pass

            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        return "SUCCEEDED"

    except requests.HTTPError:
        return "FAILED_SERVER_UNRESPONSIVE"
    except requests.RequestException:
        return "FAILED_SERVER_UNRESPONSIVE"
    except Exception:
        return "FAILED_SERVER_UNRESPONSIVE"


def save_aussda_project(metadata: dict, query_string="qualitative") -> int:
    doi = metadata.get("doi", "")
    project_folder_name = doi.replace(":", "_").replace("/", "_") if doi else "unknown_project"

    project_id = insert_project(
        {
            "query_string": query_string,
            "repository_id": 1,
            "repository_url": BASE_URL,
            "project_url": metadata.get("project_url", ""),
            "version": "",
            "title": metadata.get("title", "") or "Untitled Project",
            "description": metadata.get("description", "") or "",
            "language": metadata.get("language", ""),
            "doi": metadata.get("doi_url", ""),
            "upload_date": metadata.get("upload_date", ""),
            "download_date": datetime.now().isoformat(timespec="seconds"),
            "download_repository_folder": "aussda",
            "download_project_folder": project_folder_name,
            "download_version_folder": "",
            "download_method": "API-CALL",
        }
    )

    # Empty string is okay; validator only checks non-empty values
    insert_license(project_id, metadata.get("license", ""))

    for author in metadata.get("authors", []):
        insert_person_role(project_id, author, "AUTHOR")

    for keyword in metadata.get("keywords", []):
        insert_keyword(project_id, keyword)

    return project_id


def process_all_aussda_projects(query="qualitative", per_page=25):
    total_saved = 0
    total_skipped_duplicates = 0
    page = 0

    while True:
        start = page * per_page
        print(f"\n[AUSSDA] Fetching page {page + 1} (start={start})...")

        items = search_aussda(query=query, start=start, per_page=per_page)

        if not items:
            print("AUSSDA: no more data. Stopping.")
            break

        for item in items:
            doi = clean_text(item.get("global_id", ""))
            title = clean_text(item.get("name", ""))

            if not doi:
                continue

            doi_url = to_doi_url(doi)

            if project_exists_by_doi(doi_url):
                print(f"Skipped duplicate DOI: {doi_url}")
                total_skipped_duplicates += 1
                continue

            print(f"\n[AUSSDA] Checking: {title}")

            metadata = get_dataset_metadata(doi)
            if not metadata:
                print("Skipped: metadata fetch failed")
                continue

            print("License:", metadata.get("license", "") or "EMPTY")

            project_id = save_aussda_project(metadata, query_string=query)

            project_folder = Path("data/downloads/aussda") / doi.replace(":", "_").replace("/", "_")
            project_folder.mkdir(parents=True, exist_ok=True)

            for file_info in metadata.get("files", []):
                file_name = sanitize_filename(file_info["file_name"])
                destination = project_folder / file_name

                status = download_file(file_info["download_url"], destination)
                if status not in ALLOWED_FILE_STATUS:
                    status = "FAILED_SERVER_UNRESPONSIVE"

                insert_file(
                    project_id=project_id,
                    file_name=file_name,
                    file_type=clean_text(file_info.get("file_type", "")),
                    status=status,
                )

                if status == "SUCCEEDED":
                    print(f"Downloaded file: {file_name}")
                else:
                    print(f"Failed file: {file_name} ({status})")

            total_saved += 1

        page += 1
        time.sleep(1)

    print(f"\nAUSSDA finished. Projects saved: {total_saved}")
    print(f"AUSSDA duplicate DOIs skipped: {total_skipped_duplicates}")