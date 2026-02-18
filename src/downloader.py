"""
NTSB website scraper and file fetcher.

Handles:
  - Listing available zip files on the NTSB avdata page
  - Downloading individual zip files with progress feedback
  - Extracting the MDB file from a zip archive
"""

import re
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.config import BASE_URL, TEMP_DIR

# Pattern for incremental update files, e.g. up08FEB.zip or up08FEB25.zip
_UPDATE_PATTERN = re.compile(r"up\d{2}[A-Za-z]{3}(\d{2})?\.zip", re.IGNORECASE)


def fetch_available_files() -> list[str]:
    """
    Scrape the NTSB avdata index page and return a list of zip filenames.

    Both absolute hrefs and relative hrefs are handled.
    """
    resp = requests.get(BASE_URL, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    found: list[str] = []

    for tag in soup.find_all("a", href=True):
        href: str = tag["href"]
        # Normalise to just the filename component
        filename = Path(href.split("?")[0]).name
        if filename.lower().endswith(".zip"):
            found.append(filename)

    return found


def get_update_files(available_files: list[str]) -> list[str]:
    """Filter a file list to incremental update archives only (up*.zip)."""
    return [f for f in available_files if _UPDATE_PATTERN.match(f)]


def download_file(filename: str, dest_dir: str = TEMP_DIR) -> Path:
    """
    Download *filename* from the NTSB server into *dest_dir*.

    Returns the local Path of the downloaded file.
    Raises requests.HTTPError on a non-2xx response.
    """
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)

    url = urljoin(BASE_URL + "/", filename)
    dest_path = dest / filename

    print(f"  Downloading {filename} ...")
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        with open(dest_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=65_536):
                fh.write(chunk)
                downloaded += len(chunk)
        if total:
            print(f"    {downloaded / 1_048_576:.1f} MB downloaded")

    return dest_path


def extract_mdb(zip_path: Path, dest_dir: str = TEMP_DIR) -> Path:
    """
    Extract the first .mdb (or .accdb) file found inside *zip_path*.

    Returns the local Path of the extracted database file.
    Raises ValueError if no MDB file is present in the archive.
    """
    dest = Path(dest_dir)

    with zipfile.ZipFile(zip_path, "r") as zf:
        mdb_entries = [
            n for n in zf.namelist()
            if n.lower().endswith(".mdb") or n.lower().endswith(".accdb")
        ]
        if not mdb_entries:
            raise ValueError(f"No .mdb / .accdb file found inside {zip_path}")

        entry = mdb_entries[0]
        zf.extract(entry, dest)
        extracted_path = dest / entry

    print(f"  Extracted: {extracted_path.name}")
    return extracted_path
