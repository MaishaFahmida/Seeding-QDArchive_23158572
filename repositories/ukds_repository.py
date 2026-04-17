import mimetypes
import os
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from database.database import (
    insert_project,
    insert_license,
    insert_keyword,
    insert_person_role,
    insert_file,
    project_exists_by_doi,
)

DATACITE_URL = "https://api.datacite.org/dois"

QDA_EXTENSIONS = {
    ".qdpx", ".mx24", ".nvp", ".nvpx", ".qda", ".qdp"
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


def normalize_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


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
    if guessed:
        return guessed.lstrip(".").lower()
    return ""


def is_probable_file_url(url: str) -> bool:
    lower = url.lower()
    file_exts = {
        ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".zip", ".rar", ".7z",
        ".txt", ".rtf", ".xml", ".json", ".jpg", ".jpeg", ".png", ".mp3", ".mp4",
        ".wav", ".avi", ".mov", ".qda", ".qdpx", ".qdp", ".nvp", ".nvpx",
        ".sav", ".dta", ".tab", ".por"
    }
    return any(lower.endswith(ext) for ext in file_exts)


def extract_rights_string(rights_item):
    if not isinstance(rights_item, dict):
        return clean_text(rights_item) if rights_item else ""

    return clean_text(
        rights_item.get("rights")
        or rights_item.get("rightsIdentifier")
        or rights_item.get("rightsUri")
        or ""
    )


def normalize_license(raw_license: str) -> str:
    text = clean_text(raw_license)
    lower = text.lower()

    if not text:
        return "UNKNOWN"

    if "cc by-sa" in lower or "creative commons attribution-sharealike" in lower:
        return "CC BY-SA 4.0"
    if "cc by-nc-nd" in lower:
        return "CC BY-NC-ND"
    if "cc by-nc" in lower:
        return "CC BY-NC"
    if "cc by-nd" in lower:
        return "CC BY-ND"
    if "cc by" in lower or "creative commons attribution" in lower:
        return "CC BY 4.0"
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

    return text


def search_ukds(query="qualitative", page_size=25, page_number=1):
    params = {
        "client-id": "bl.ukda",
        "query": query,
        "page[size]": page_size,
        "page[number]": page_number,
    }

    response = requests.get(DATACITE_URL, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()
    return data.get("data", [])


def get_ukds_record_by_doi(doi: str):
    url = f"{DATACITE_URL}/{doi}"

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    data = response.json()
    return data.get("data")


def to_doi_url(doi: str) -> str:
    doi = clean_text(doi)
    if not doi:
        return ""
    if doi.startswith("http://") or doi.startswith("https://"):
        return doi
    return f"https://doi.org/{doi}"


def parse_ukds_record(record):
    attrs = record.get("attributes", {})

    doi = clean_text(attrs.get("doi", ""))

    title = ""
    titles = normalize_list(attrs.get("titles"))
    if titles and isinstance(titles[0], dict):
        title = clean_text(titles[0].get("title", ""))

    description = ""
    descriptions = normalize_list(attrs.get("descriptions"))
    if descriptions and isinstance(descriptions[0], dict):
        description = clean_text(descriptions[0].get("description", ""))

    authors = []
    for creator in normalize_list(attrs.get("creators")):
        if isinstance(creator, dict):
            name = clean_text(creator.get("name", ""))
            if name:
                authors.append(name)

    keywords = []
    for subject in normalize_list(attrs.get("subjects")):
        if isinstance(subject, dict):
            subject_text = clean_text(subject.get("subject", ""))
            if subject_text:
                keywords.append(subject_text)

    license_value = ""
    for rights_item in normalize_list(attrs.get("rightsList")):
        rights_text = extract_rights_string(rights_item)
        if rights_text:
            license_value = rights_text
            break

    language = clean_text(attrs.get("language", ""))
    upload_date = clean_text(attrs.get("published", "") or attrs.get("created", ""))
    project_url = to_doi_url(doi)

    return {
        "doi": doi,
        "doi_url": project_url,
        "title": title,
        "description": description,
        "keywords": unique_clean_list(keywords),
        "authors": unique_clean_list(authors),
        "license": normalize_license(license_value),
        "language": language,
        "upload_date": upload_date[:10] if upload_date else "",
        "project_url": project_url,
    }


def should_keep_ukds_record(metadata):
    return bool(clean_text(metadata.get("doi_url", "")))


def resolve_doi_to_landing_page(doi_url: str) -> str:
    try:
        r = requests.get(doi_url, timeout=30, allow_redirects=True)
        r.raise_for_status()
        return r.url
    except Exception:
        return doi_url


def fetch_html(url: str):
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        r.raise_for_status()
        if "text/html" not in (r.headers.get("Content-Type", "").lower()):
            return None, url
        return r.text, r.url
    except Exception:
        return None, url


def page_indicates_open_access(page_text: str) -> bool:
    text = (page_text or "").lower()

    positive_markers = [
        "these data are open",
        "available to any user without the requirement for registration",
        "without the requirement for registration for download/access",
        "creative commons attribution 4.0 international licence",
        "open ukda download",
    ]

    negative_markers = [
        "available to registered users",
        "end user licence",
        "special licence",
        "safeguarded",
        "download these data by adding them to your account",
        "register / login",
        "login",
    ]

    if any(marker in text for marker in negative_markers):
        return False

    return any(marker in text for marker in positive_markers)


def collect_links_from_container(container, final_url, results, seen):
    for link in container.find_all("a", href=True):
        href = clean_text(link.get("href", ""))
        text = " ".join(link.get_text(" ", strip=True).split())

        if not href:
            continue

        full_url = urljoin(final_url, href)
        file_name = sanitize_filename(text or Path(full_url).name)

        key = (file_name, full_url)
        if key not in seen:
            seen.add(key)
            results.append({
                "file_name": file_name,
                "download_url": full_url,
            })


def extract_open_file_links_from_landing_page(landing_url: str):
    html, final_url = fetch_html(landing_url)
    if not html:
        return []

    if not page_indicates_open_access(html):
        return []

    soup = BeautifulSoup(html, "html.parser")
    results = []
    seen = set()

    for header in soup.find_all(string=lambda s: s and "Access study data" in s):
        parent = header.find_parent()
        if parent:
            collect_links_from_container(parent, final_url, results, seen)

    format_names = ["SPSS", "STATA", "TAB", "CSV", "TXT", "R", "SAS"]
    for link in soup.select("a[href]"):
        href = clean_text(link.get("href", ""))
        text = " ".join(link.get_text(" ", strip=True).split())

        if not href:
            continue

        if any(fmt.lower() in text.lower() for fmt in format_names):
            full_url = urljoin(final_url, href)
            file_name = sanitize_filename(text or Path(full_url).name)

            key = (file_name, full_url)
            if key not in seen:
                seen.add(key)
                results.append({
                    "file_name": file_name,
                    "download_url": full_url,
                })

    for link in soup.select("a[href]"):
        href = clean_text(link.get("href", ""))
        text = " ".join(link.get_text(" ", strip=True).split()).lower()

        if not href:
            continue

        full_url = urljoin(final_url, href)
        file_name = sanitize_filename(Path(full_url).name or text)

        looks_downloadable = (
            is_probable_file_url(full_url)
            or "download" in text
            or "open ukda download" in text
        )

        if looks_downloadable:
            key = (file_name, full_url)
            if key not in seen:
                seen.add(key)
                results.append({
                    "file_name": file_name,
                    "download_url": full_url,
                })

    return results


def download_file(url: str, destination: Path) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        with requests.get(url, headers=headers, stream=True, timeout=180, allow_redirects=True) as response:
            response.raise_for_status()

            content_type = (response.headers.get("Content-Type") or "").lower()
            if "text/html" in content_type:
                return "FAILED_LOGIN_REQUIRED"

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


def save_ukds_project(metadata, query_string="qualitative"):
    doi = metadata.get("doi", "")
    project_folder_name = doi.replace("/", "_").replace(":", "_") if doi else "ukds_unknown"

    project_id = insert_project({
        "query_string": query_string,
        "repository_id": 2,
        "repository_url": "https://api.datacite.org/dois?client-id=bl.ukda",
        "project_url": metadata.get("project_url", ""),
        "version": "",
        "title": metadata.get("title", "") or "Untitled Project",
        "description": metadata.get("description", "") or "",
        "language": metadata.get("language", ""),
        "doi": metadata.get("doi_url", ""),
        "upload_date": metadata.get("upload_date", ""),
        "download_date": datetime.now().isoformat(timespec="seconds"),
        "download_repository_folder": "ukds",
        "download_project_folder": project_folder_name,
        "download_version_folder": "",
        "download_method": "API-CALL",
    })

    insert_license(project_id, metadata.get("license", "") or "UNKNOWN")

    for author in metadata.get("authors", []):
        insert_person_role(project_id, author, "AUTHOR")

    for keyword in metadata.get("keywords", []):
        insert_keyword(project_id, keyword)

    return project_id


def save_folder_files_to_db(project_id: int, folder_path: str):
    folder = Path(folder_path)

    for root, _, files in os.walk(folder):
        for file_name in files:
            file_path = Path(root) / file_name
            file_type = file_path.suffix.lower().lstrip(".")

            insert_file(
                project_id=project_id,
                file_name=file_name,
                file_type=file_type,
                status="SUCCEEDED",
            )


def process_all_ukds_projects(query="qualitative", page_size=25):
    total_saved = 0
    total_downloaded_projects = 0
    total_skipped_duplicates = 0
    page_number = 1

    while True:
        print(f"\n[UKDS] Fetching page {page_number}...")

        records = search_ukds(query=query, page_size=page_size, page_number=page_number)

        if not records:
            print("UKDS: no more data. Stopping.")
            break

        for record in records:
            metadata = parse_ukds_record(record)

            if not should_keep_ukds_record(metadata):
                continue

            doi_url = metadata.get("doi_url", "")
            if project_exists_by_doi(doi_url):
                print(f"Skipped duplicate DOI: {doi_url}")
                total_skipped_duplicates += 1
                continue

            print(f"\n[UKDS] Checking: {metadata.get('title', '')}")
            print("License:", metadata.get("license", "") or "MISSING")

            project_id = save_ukds_project(metadata, query_string=query)
            total_saved += 1

            landing_url = resolve_doi_to_landing_page(metadata.get("project_url", ""))
            print("Resolved landing URL:", landing_url)

            file_links = extract_open_file_links_from_landing_page(landing_url)
            print("Found file links:", len(file_links))

            project_folder = Path("data/downloads/ukds") / metadata["doi"].replace("/", "_").replace(":", "_")
            project_folder.mkdir(parents=True, exist_ok=True)

            successful_file_download = False

            if file_links:
                for file_info in file_links:
                    file_name = sanitize_filename(file_info["file_name"])
                    destination = project_folder / file_name
                    status = download_file(file_info["download_url"], destination)

                    insert_file(
                        project_id=project_id,
                        file_name=file_name,
                        file_type=Path(file_name).suffix.lower().lstrip("."),
                        status=status,
                    )

                    if status == "SUCCEEDED":
                        successful_file_download = True
                        print(f"Downloaded file: {file_name}")
                    else:
                        print(f"Failed file: {file_name} ({status})")

            if not successful_file_download:
                insert_file(
                    project_id=project_id,
                    file_name="UKDS_METADATA_ONLY",
                    file_type="txt",
                    status="FAILED_LOGIN_REQUIRED",
                )
                print("Saved metadata only (no public direct file download found)")

            if successful_file_download:
                total_downloaded_projects += 1

        page_number += 1
        time.sleep(1)

    print(f"\nUKDS finished. Projects saved: {total_saved}")
    print(f"UKDS projects with public file downloads: {total_downloaded_projects}")
    print(f"UKDS duplicate DOIs skipped: {total_skipped_duplicates}")