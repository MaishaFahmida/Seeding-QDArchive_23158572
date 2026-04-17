import mimetypes
import time
import requests
import os
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from bs4 import BeautifulSoup

from database.database import (
    insert_project,
    insert_license,
    insert_keyword,
    insert_person_role,
    insert_file,
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

def get_ukds_record_by_doi(doi: str):
    url = f"{DATACITE_URL}/{doi}"

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    data = response.json()
    return data.get("data")

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
        href = (link.get("href") or "").strip()
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
        href = (link.get("href") or "").strip()
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
        href = (link.get("href") or "").strip()
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


def download_file(url: str, destination: Path) -> bool:
    destination.parent.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        with requests.get(url, headers=headers, stream=True, timeout=180, allow_redirects=True) as response:
            response.raise_for_status()

            content_type = (response.headers.get("Content-Type") or "").lower()
            if "text/html" in content_type:
                return False

            if not destination.suffix:
                ext = extension_from_content_type(content_type)
                if ext:
                    destination = destination.with_suffix(ext)

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


def save_folder_files_to_db(project_id: int, folder_path: str):
    folder = Path(folder_path)

    for root, _, files in os.walk(folder):
        for file_name in files:
            file_path = Path(root) / file_name
            file_type = file_path.suffix.lower()

            insert_file(
                project_id=project_id,
                file_name=file_name,
                file_type=file_type,
                status="SUCCEEDED",
            )


def extract_license_from_landing_page(landing_url: str) -> str:
    html, final_url = fetch_html(landing_url)

    print("\n--- DEBUG extract_license_from_landing_page ---")
    print("Input landing URL =", repr(landing_url))
    print("Final URL =", repr(final_url))
    print("HTML fetched =", html is not None)

    if not html:
        return ""

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    print("Page text sample =", repr(text[:500]))

    candidates = [
        "Creative Commons Attribution 4.0 International Licence",
        "Creative Commons Attribution 4.0 International License",
        "CC BY 4.0",
        "Open Government Licence",
        "End User Licence",
        "Special Licence",
        "Safeguarded",
        "These data are open",
    ]

    lower_text = text.lower()
    for item in candidates:
        if item.lower() in lower_text:
            print("Matched candidate =", repr(item))
            return item

    print("No candidate matched.")
    return ""

def normalize_ukds_license(raw_text: str) -> str:
    text = (raw_text or "").lower()

    if "attribution-sharealike" in text:
        return "Creative Commons Attribution-ShareAlike 4.0 International Licence"

    if "creative commons attribution 4.0" in text:
        return "Creative Commons Attribution 4.0 International Licence"

    if "cc by-sa 4.0" in text:
        return "Creative Commons Attribution-ShareAlike 4.0 International Licence"

    if "cc by 4.0" in text:
        return "Creative Commons Attribution 4.0 International Licence"

    if "open government licence" in text:
        return "Open Government Licence"

    if "end user licence" in text:
        return "End User Licence"

    if "special licence" in text:
        return "Special Licence"

    if "safeguarded" in text:
        return "Safeguarded"

    return ""

def enrich_ukds_license(metadata: dict) -> dict:
    print("\n--- DEBUG enrich_ukds_license ---")
    print("Before enrich, license =", repr(metadata.get("license")))
    print("Project URL =", repr(metadata.get("project_url")))

    # 1. If license already exists, normalize it
    existing_license = metadata.get("license", "")
    normalized_existing = normalize_ukds_license(existing_license)
    if normalized_existing:
        metadata["license"] = normalized_existing
        print("Normalized existing license =", repr(metadata["license"]))
        return metadata

    # 2. Try landing page
    landing_url = resolve_doi_to_landing_page(metadata.get("project_url", ""))
    print("Resolved landing URL =", repr(landing_url))

    landing_license = extract_license_from_landing_page(landing_url)
    print("Landing page license found =", repr(landing_license))

    normalized_landing = normalize_ukds_license(landing_license)
    if normalized_landing:
        metadata["license"] = normalized_landing
        print("Updated metadata license from landing page =", repr(metadata["license"]))
        return metadata

    # 3. DOI fallback for manually verified studies
    doi = (metadata.get("doi") or "").strip().lower()

    doi_fallbacks = {
        "10.5255/ukda-sn-8049-1": "Creative Commons Attribution 4.0 International Licence",
        "10.5255/ukda-sn-7465-1": "Open Government Licence",
        "10.5255/ukda-sn-2713-1": "Creative Commons Attribution-ShareAlike 4.0 International Licence",
        "10.5255/ukda-sn-2000-1": "Creative Commons Attribution 4.0 International Licence",
        "10.5255/ukda-sn-4867-1": "Creative Commons Attribution 4.0 International Licence",
         "10.5255/ukda-sn-6226-1": "Creative Commons Attribution 4.0 International Licence",
        "10.5255/ukda-sn-9227-1": "Creative Commons Attribution 4.0 International Licence",
 }

    if doi in doi_fallbacks:
        metadata["license"] = doi_fallbacks[doi]
        print("Applied DOI fallback license =", repr(metadata["license"]))
        return metadata

    print("No license found.")
    return metadata

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

            landing_url = resolve_doi_to_landing_page(metadata.get("project_url", ""))
            print("Resolved landing URL:", landing_url)

            file_links = extract_open_file_links_from_landing_page(landing_url)
            print("Found file links:", len(file_links))

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