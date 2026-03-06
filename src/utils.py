import hashlib
import re
from pathlib import Path

def safe_slug(text: str, max_len: int = 120) -> str:
    """
    Make a filesystem-safe folder/file slug.
    """
    text = (text or "").strip()
    if not text:
        return "untitled"
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^a-zA-Z0-9._ -]+", "_", text)
    text = text.replace(" ", "_")
    return text[:max_len].strip("._-") or "untitled"

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()