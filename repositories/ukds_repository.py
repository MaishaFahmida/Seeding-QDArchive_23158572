import mimetypes
import time
import requests
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from database.database import (
    insert_project,
    insert_license,
    insert_keyword,
    insert_person_role,
    insert_file
)
DATACITE_URL = "https://api.datacite.org/dois"

QDA_EXTENSIONS = {
    ".qdpx", ".mx24", ".nvp", ".nvpx", ".qda", ".qdp"
}


def normalize_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


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


def extract_rights_string(rights_item):
    if not isinstance(rights_item, dict):
        return str(rights_item) if rights_item else ""

    return (
        rights_item.get("rights")
        or rights_item.get("rightsIdentifier")
        or rights_item.get("rightsUri")
        or ""
    )


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


def parse_ukds_record(record):
    attrs = record.get("attributes", {})

    doi = attrs.get("doi", "")

    title = ""
    titles = normalize_list(attrs.get("titles"))
    if titles and isinstance(titles[0], dict):
        title = titles[0].get("title", "")

    description = ""
    descriptions = normalize_list(attrs.get("descriptions"))
    if descriptions and isinstance(descriptions[0], dict):
        description = descriptions[0].get("description", "")

    authors = []
    for creator in normalize_list(attrs.get("creators")):
        if isinstance(creator, dict):
            name = creator.get("name", "")
            if name:
                authors.append(name)

    keywords = []
    for subject in normalize_list(attrs.get("subjects")):
        if isinstance(subject, dict):
            subject_text = subject.get("subject", "")
            if subject_text:
                keywords.append(subject_text)

    license_value = ""
    for rights_item in normalize_list(attrs.get("rightsList")):
        rights_text = extract_rights_string(rights_item)
        if rights_text:
            license_value = rights_text
            break

    language = attrs.get("language", "")
    upload_date = attrs.get("published", "") or attrs.get("created", "")
    project_url = f"https://doi.org/{doi}" if doi else ""

    return {
        "doi": doi,
        "title": title,
        "description": description,
        "keywords": keywords,
        "authors": authors,
        "license": license_value,
        "language": language,
        "upload_date": upload_date[:10] if upload_date else "",
        "project_url": project_url,
    }


def should_keep_ukds_record(metadata):
    raw_license = metadata.get("license", "")
    return bool(raw_license and raw_license.strip())


def resolve_doi_to_landing_page(doi_url: str) -> str:
    try:
        r = requests.get(doi_url, timeout=30, allow_redirects=True)
        r.raise_for_status()
        return r.url
    except Exception:
        return doi_url


def is_probable_file_url(url: str) -> bool:
    lower = url.lower()
    file_exts = {
        ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".zip", ".rar", ".7z",
        ".txt", ".rtf", ".xml", ".json", ".jpg", ".jpeg", ".png", ".mp3", ".mp4",
        ".wav", ".avi", ".mov", ".qda", ".qdpx", ".qdp", ".nvp", ".nvpx"
    }
    return any(lower.endswith(ext) for ext in file_exts)


def extract_open_file_links_from_landing_page(landing_url: str):
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(landing_url, headers=headers, timeout=30, allow_redirects=True)
        r.raise_for_status()
    except Exception:
        return []

    page_text = r.text.lower()

    access_blockers = [
        "registered users",
        "end user licence",
        "download these data by adding them to your account",
        "register / login",
        "login",
    ]
    if any(token in page_text for token in access_blockers):
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results = []
    seen = set()

    for link in soup.select("a[href]"):
        href = (link.get("href") or "").strip()
        if not href:
            continue

        full_url = urljoin(r.url, href)
        text = " ".join(link.get_text(" ", strip=True).split())
        filename = Path(full_url).name or sanitize_filename(text)

        downloadable_words = [
            "download", "file", "pdf", "doc", "docx", "txt", "csv", "zip", "data"
        ]

        looks_downloadable = (
            is_probable_file_url(full_url)
            or any(word in text.lower() for word in downloadable_words)
            or "download" in full_url.lower()
        )

        if looks_downloadable:
            key = (filename, full_url)
            if key not in seen:
                seen.add(key)
                results.append({
                    "file_name": sanitize_filename(filename),
                    "download_url": full_url,
                })

    return results


def download_file(url: str, destination: Path) -> bool:
    destination.parent.mkdir(parents=True, exist_ok=True)

    try:
        with requests.get(url, stream=True, timeout=180) as response:
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "")
            if "text/html" in content_type.lower():
                return False

            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return True
    except Exception:
        return False


def save_ukds_project(metadata, query_string="qualitative"):
    doi = metadata.get("doi", "")
    project_folder_name = doi.replace("/", "_") if doi else "ukds_unknown"

    project_id = insert_project({
        "query_string": query_string,
        "repository_id": 2,
        "repository_url": "https://api.datacite.org/dois?client-id=bl.ukda",
        "project_url": metadata.get("project_url", ""),
        "version": "",
        "title": metadata.get("title", ""),
        "description": metadata.get("description", ""),
        "language": metadata.get("language", ""),
        "doi": metadata.get("doi", ""),
        "upload_date": metadata.get("upload_date", ""),
        "download_repository_folder": "ukds",
        "download_project_folder": project_folder_name,
        "download_version_folder": "",
        "download_method": "API_METADATA_PLUS_PUBLIC_PAGE",
    })

    insert_license(project_id, metadata.get("license", ""))

    for author in metadata.get("authors", []):
        insert_person_role(project_id, author, "AUTHOR")

    for keyword in metadata.get("keywords", []):
        insert_keyword(project_id, keyword)

    return project_id


def process_all_ukds_projects(query="qualitative", page_size=25):
    total_saved = 0
    total_downloaded_projects = 0
    page_number = 1

    while True:
        print(f"\n[UKDS] Fetching page {page_number}...")

        records = search_ukds(query=query, page_size=page_size, page_number=page_number)

        if not records:
            print("UKDS: no more data. Stopping.")
            break

        for record in records:
            metadata = parse_ukds_record(record)

            if not metadata.get("doi"):
                continue

            print(f"\n[UKDS] Checking: {metadata.get('title', '')}")
            print("License:", metadata.get("license", "") or "MISSING")

            if not should_keep_ukds_record(metadata):
                print("Skipped: no license string found")
                continue

            project_id = save_ukds_project(metadata, query_string=query)
            total_saved += 1

            doi_url = metadata.get("project_url", "")
            landing_url = resolve_doi_to_landing_page(doi_url)
            file_links = extract_open_file_links_from_landing_page(landing_url)

            project_folder = Path("data/downloads/ukds") / metadata["doi"].replace("/", "_")
            project_folder.mkdir(parents=True, exist_ok=True)

            successful_file_download = False

            if file_links:
                for file_info in file_links:
                    file_name = sanitize_filename(file_info["file_name"])
                    destination = project_folder / file_name
                    success = download_file(file_info["download_url"], destination)

                    file_type = Path(file_name).suffix.lower()
                    insert_file(
                        project_id=project_id,
                        file_name=file_name,
                        file_type=file_type,
                        status="SUCCEEDED" if success else "FAILED_SERVER",
                    )

                    if success:
                        successful_file_download = True
                        print(f"Downloaded file: {file_name}")
                    else:
                        print(f"Failed file: {file_name}")

            if not successful_file_download:
                insert_file(
                    project_id=project_id,
                    file_name="UKDS_METADATA_ONLY",
                    file_type="",
                    status="FAILED_LOGIN",
                )
                print("Saved metadata only (no public direct file download found)")

            if successful_file_download:
                total_downloaded_projects += 1

        page_number += 1
        time.sleep(1)

    print(f"\nUKDS finished. Projects saved: {total_saved}")
    print(f"UKDS projects with public file downloads: {total_downloaded_projects}")