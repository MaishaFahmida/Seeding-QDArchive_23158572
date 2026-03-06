import time
from typing import Dict, Any, List, Optional
import requests

ZENODO_API = "https://zenodo.org/api/records"

class ZenodoClient:
    def __init__(self, timeout: int = 60, polite_sleep: float = 0.3):
        self.timeout = timeout
        self.polite_sleep = polite_sleep
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Seeding-QDArchive/1.0"})

    def search(self, query: str, page: int = 1, size: int = 25, sort: str = "mostrecent") -> Dict[str, Any]:
        params = {"q": query, "page": page, "size": size, "sort": sort}
        r = self.session.get(ZENODO_API, params=params, timeout=self.timeout)
        r.raise_for_status()
        time.sleep(self.polite_sleep)
        return r.json()

    def iter_records(self, query: str, max_records: int) -> List[Dict[str, Any]]:
        records = []
        page = 1
        while len(records) < max_records:
            data = self.search(query=query, page=page, size=25)
            hits = (data.get("hits") or {}).get("hits") or []
            if not hits:
                break
            for rec in hits:
                records.append(rec)
                if len(records) >= max_records:
                    break
            page += 1
        return records

    def download_file(self, file_url: str, out_path, chunk_size: int = 1024 * 256) -> int:
        """
        Download file_url -> out_path. Returns size_bytes downloaded.
        """
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with self.session.get(file_url, stream=True, timeout=120) as r:
            r.raise_for_status()
            total = 0
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        total += len(chunk)
        time.sleep(self.polite_sleep)
        return total