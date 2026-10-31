"""
Unit Tests for PharmaSafe DownloadManager
Tests streaming downloads, SHA-256 calculation, zip integrity validation,
mocked HTTP error scenarios, and manifest tracking.
"""

import sys
import io
import zipfile
import hashlib
from pathlib import Path
import pytest
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.download_manager import DownloadManager


class MockResponse:
    """Mock requests.Response for streaming chunks."""
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def iter_content(self, chunk_size=1024):
        stream = io.BytesIO(self.content)
        while chunk := stream.read(chunk_size):
            yield chunk

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP Error {self.status_code}")


class MockSession:
    """Mock requests.Session returning predetermined MockResponse."""
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def get(self, url, **kwargs):
        return MockResponse(self.content, self.status_code)


def create_in_memory_zip() -> bytes:
    """Creates a valid zip archive in memory with a test ASCII file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("ASCII/DEMO24Q1.txt", "primaryid$caseid$caseversion\n101$101$1\n")
    return buf.getvalue()


def test_download_valid_archive(tmp_path):
    """Test successful streaming download, SHA-256 verification, and manifest registration."""
    zip_bytes = create_in_memory_zip()
    expected_sha256 = hashlib.sha256(zip_bytes).hexdigest()

    cfg = {
        "storage": {
            "raw_dir": str(tmp_path / "raw"),
            "manifest_path": str(tmp_path / "raw/manifest.json")
        },
        "quarters": {
            "pilot": [{
                "quarter_id": "2024Q1",
                "archive_name": "faers_ascii_2024q1.zip",
                "url": "https://fis.fda.gov/content/Exports/faers_ascii_2024q1.zip"
            }]
        }
    }

    dm = DownloadManager(config=cfg)
    mock_session = MockSession(zip_bytes, status_code=200)

    record = dm.download_quarter("2024Q1", session=mock_session)
    assert record["verification_status"] == "VERIFIED"
    assert record["sha256_checksum"] == expected_sha256
    assert record["file_size_bytes"] == len(zip_bytes)
    assert (tmp_path / "raw" / "faers_ascii_2024q1.zip").exists()

    # Verify manifest was written
    assert dm.manifest_path.exists()
    assert dm.is_archive_valid("2024Q1") is True


def test_download_corrupted_archive_rejected(tmp_path):
    """Test that a non-zip or corrupted file raises ValueError and cleans up temporary file."""
    corrupted_bytes = b"NOT_A_REAL_ZIP_FILE_DATA_12345"

    cfg = {
        "storage": {
            "raw_dir": str(tmp_path / "raw"),
            "manifest_path": str(tmp_path / "raw/manifest.json")
        },
        "quarters": {
            "pilot": [{
                "quarter_id": "2024Q1",
                "archive_name": "faers_ascii_2024q1.zip",
                "url": "https://fis.fda.gov/content/Exports/faers_ascii_2024q1.zip"
            }]
        }
    }

    dm = DownloadManager(config=cfg)
    mock_session = MockSession(corrupted_bytes, status_code=200)

    with pytest.raises(ValueError, match="not a valid zip file or is corrupted"):
        dm.download_quarter("2024Q1", session=mock_session)

    # Temporary file must be removed
    assert not (tmp_path / "raw" / "faers_ascii_2024q1.zip.download.tmp").exists()
    assert not (tmp_path / "raw" / "faers_ascii_2024q1.zip").exists()


def test_download_http_error_handling(tmp_path):
    """Test that HTTP 404 or 500 raises requests.HTTPError and cleans up."""
    cfg = {
        "storage": {
            "raw_dir": str(tmp_path / "raw"),
            "manifest_path": str(tmp_path / "raw/manifest.json")
        },
        "quarters": {
            "pilot": [{
                "quarter_id": "2024Q1",
                "archive_name": "faers_ascii_2024q1.zip",
                "url": "https://fis.fda.gov/content/Exports/faers_ascii_2024q1.zip"
            }]
        }
    }

    dm = DownloadManager(config=cfg)
    mock_session = MockSession(b"", status_code=404)

    with pytest.raises(requests.HTTPError):
        dm.download_quarter("2024Q1", session=mock_session)


def test_download_synthetic_archive_rejected(tmp_path):
    """Test that an archive containing synthetic markers is rejected from official registration."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("ASCII/DRUG24Q1.txt", "primaryid$drugname\n101$SYNTH_DRUG_ALPHA\n")
    synth_bytes = buf.getvalue()

    cfg = {
        "storage": {
            "raw_dir": str(tmp_path / "raw"),
            "manifest_path": str(tmp_path / "raw/manifest.json")
        },
        "quarters": {
            "pilot": [{
                "quarter_id": "2024Q1",
                "archive_name": "faers_ascii_2024q1.zip",
                "url": "https://fis.fda.gov/content/Exports/faers_ascii_2024q1.zip"
            }]
        }
    }

    dm = DownloadManager(config=cfg)
    mock_session = MockSession(synth_bytes, status_code=200)

    with pytest.raises(ValueError, match="Contains synthetic test markers"):
        dm.download_quarter("2024Q1", session=mock_session)

    # Temporary file must be removed and not registered
    assert not (tmp_path / "raw" / "faers_ascii_2024q1.zip").exists()
    assert not dm.is_archive_valid("2024Q1")

