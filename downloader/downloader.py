from pathlib import Path
import requests
import zipfile


def download_ukds_file(download_url: str, save_path: str) -> bool:
    try:
        save_file = Path(save_path)
        save_file.parent.mkdir(parents=True, exist_ok=True)

        with requests.get(download_url, stream=True, timeout=180) as response:
            response.raise_for_status()

            with open(save_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        print(f"Downloaded: {save_file}")
        return True

    except Exception as e:
        print("Download failed:", e)
        return False


def extract_zip(zip_path: str, extract_to: str) -> bool:
    try:
        zip_file = Path(zip_path)
        extract_dir = Path(extract_to)
        extract_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_file, "r") as zf:
            zf.extractall(extract_dir)

        print(f"Extracted: {zip_file} -> {extract_dir}")
        return True

    except Exception as e:
        print("Extraction failed:", e)
        return False


def delete_zip(zip_path: str) -> bool:
    try:
        zip_file = Path(zip_path)
        if zip_file.exists():
            zip_file.unlink()
            print(f"Deleted zip: {zip_file}")
        return True

    except Exception as e:
        print("Delete failed:", e)
        return False