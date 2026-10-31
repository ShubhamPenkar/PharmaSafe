"""
PharmaSafe Data Acquisition & Download Manager
Downloads official FDA FAERS quarterly data archives with streaming,
SHA-256 checksum tracking, atomic writes, archive validation, synthetic marker guards,
and trustworthy manifest logging.
"""

import os
import sys
import hashlib
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
import requests

# Ensure PharmaSafe root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.config_loader import load_data_sources_config, get_project_root

logger = get_logger("PharmaSafe.Downloader")

SYNTHETIC_MARKERS = [
    "SYNTH_DRUG_ALPHA",
    "SYNTH_DRUG_BETA",
    "SYNTH_EVENT_TACHY",
    "BENCH_STATIN",
    "BENCH_RHABDOMYOLYSIS",
    "ORPHAN_DRUG",
    "SYNTH_EVENT_ORPHAN",
    "SYNTH_CORP"
]


class DownloadManager:
    """
    Manages downloading, verifying, and logging official FDA FAERS quarterly archives.
    Guards against synthetic fixtures posing as official FDA data.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or load_data_sources_config()
        self.root = get_project_root()
        self.raw_dir = self.root / self.config.get("storage", {}).get("raw_dir", "data/raw")
        self.manifest_path = self.root / self.config.get("storage", {}).get("manifest_path", "data/raw/manifest.json")
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.manifest = self._load_manifest()

    def _load_manifest(self) -> Dict[str, Any]:
        """Loads or initializes data/raw/manifest.json."""
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning("Could not read existing manifest (%s), initializing new.", e)
        return {"version": "2.0", "archives": {}}

    def _save_manifest(self) -> None:
        """Persists the manifest file atomically."""
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.manifest_path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(self.manifest, f, indent=2, sort_keys=True)
        temp_path.replace(self.manifest_path)

    @staticmethod
    def calculate_sha256(file_path: Path, chunk_size: int = 65536) -> str:
        """Computes the SHA-256 hash of a file."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
        return hasher.hexdigest()

    @staticmethod
    def verify_zip_integrity(file_path: Path) -> Tuple[bool, List[str]]:
        """
        Tests the zip archive for corruption and lists contained filenames.
        Returns (is_valid, list_of_filenames).
        """
        if not zipfile.is_zipfile(file_path):
            return False, []
        try:
            with zipfile.ZipFile(file_path, "r") as zf:
                corrupted = zf.testzip()
                if corrupted is not None:
                    logger.error("Corrupted file in zip archive %s: %s", file_path.name, corrupted)
                    return False, []
                return True, zf.namelist()
        except Exception as e:
            logger.error("Error inspecting zip archive %s: %s", file_path.name, e)
            return False, []

    @staticmethod
    def scan_for_synthetic_markers(file_path: Path) -> List[str]:
        """Scans archive members for known synthetic identifiers."""
        markers_detected = []
        if not zipfile.is_zipfile(file_path):
            return markers_detected
        try:
            with zipfile.ZipFile(file_path, "r") as zf:
                for name in zf.namelist():
                    if name.upper().endswith((".TXT", ".DAT")):
                        try:
                            # Read first 100KB of each text member to check markers
                            with zf.open(name, "r") as member_file:
                                chunk = member_file.read(102400).decode("utf-8", errors="replace")
                                for sm in SYNTHETIC_MARKERS:
                                    if sm in chunk and sm not in markers_detected:
                                        markers_detected.append(sm)
                        except Exception:
                            pass
        except Exception as ex:
            logger.warning("Error scanning for synthetic markers in %s: %s", file_path.name, ex)
        return markers_detected

    def get_quarter_config(self, quarter_id: str) -> Dict[str, Any]:
        """Finds configuration metadata for a given quarter (e.g. '2024Q3')."""
        quarter_id_upper = quarter_id.upper()
        # Search study quarters first
        study_quarters = self.config.get("quarters", {}).get("study_quarters", [])
        for q in study_quarters:
            if q.get("quarter_id", "").upper() == quarter_id_upper:
                return q

        # Search pilot quarters
        pilot_quarters = self.config.get("quarters", {}).get("pilot", [])
        for q in pilot_quarters:
            if q.get("quarter_id", "").upper() == quarter_id_upper:
                return q

        base_url = self.config.get("faers_source", {}).get("base_url", "https://fis.fda.gov/content/Exports")
        archive_name = f"faers_ascii_{quarter_id_upper.lower()}.zip"
        return {
            "quarter_id": quarter_id_upper,
            "archive_name": archive_name,
            "url": f"{base_url.rstrip('/')}/{archive_name}",
            "status": "dynamic"
        }

    def resolve_archive_file(self, quarter_id: str) -> Optional[Path]:
        """Locates the archive file on disk from manifest or default paths."""
        quarter_key = quarter_id.upper()
        q_meta = self.manifest.get("archives", {}).get(quarter_key)
        if q_meta:
            rel_path = q_meta.get("relative_path")
            if rel_path:
                cand = self.root / rel_path
                if cand.exists():
                    return cand

            fn = q_meta.get("filename", "")
            cand1 = self.raw_dir / "official" / quarter_key / fn
            if cand1.exists():
                return cand1

            cand2 = self.raw_dir / fn
            if cand2.exists():
                return cand2

        # Fallback check
        q_cfg = self.get_quarter_config(quarter_key)
        fname = q_cfg.get("archive_name", f"faers_ascii_{quarter_key.lower()}.zip")
        cand3 = self.raw_dir / "official" / quarter_key / fname
        if cand3.exists():
            return cand3
        cand4 = self.raw_dir / fname
        if cand4.exists():
            return cand4

        return None

    def is_archive_valid(self, quarter_id: str) -> bool:
        """Checks if a quarter's archive is already downloaded, verified, and recorded in the manifest."""
        quarter_key = quarter_id.upper()
        q_meta = self.manifest.get("archives", {}).get(quarter_key)
        if not q_meta:
            return False

        status = q_meta.get("verification_status")
        if status not in ("VERIFIED", "VERIFIED_OFFICIAL"):
            return False

        if q_meta.get("synthetic_markers_detected"):
            return False

        file_path = self.resolve_archive_file(quarter_key)
        if not file_path or not file_path.exists():
            return False

        if file_path.stat().st_size != q_meta.get("file_size_bytes", 0):
            return False

        return True

    def download_quarter(
        self,
        quarter_id: str,
        timeout: int = 120,
        force: bool = False,
        session: Optional[requests.Session] = None
    ) -> Dict[str, Any]:
        """
        Downloads a specific quarter's FAERS archive using streaming and atomic file placement.
        Validates archive integrity, guards against synthetic markers, and records provenance in manifest.
        """
        q_cfg = self.get_quarter_config(quarter_id)
        quarter_key = q_cfg["quarter_id"]
        archive_name = q_cfg["archive_name"]
        url = q_cfg["url"]

        target_file = self.raw_dir / archive_name
        temp_file = self.raw_dir / f"{archive_name}.download.tmp"

        if not force and self.is_archive_valid(quarter_key):
            logger.info("Quarter %s already present and verified. Skipping download.", quarter_key)
            return self.manifest["archives"][quarter_key]

        logger.info("Initiating download for quarter %s from %s...", quarter_key, url)
        http = session or requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "*/*"
        }

        try:
            response = http.get(url, stream=True, headers=headers, timeout=timeout)
            response.raise_for_status()

            hasher = hashlib.sha256()
            total_bytes = 0
            with open(temp_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=131072):
                    if chunk:
                        f.write(chunk)
                        hasher.update(chunk)
                        total_bytes += len(chunk)

            calculated_sha256 = hasher.hexdigest()
            logger.info("Downloaded %d bytes. Verifying archive integrity...", total_bytes)

            is_valid_zip, contained_files = self.verify_zip_integrity(temp_file)
            if not is_valid_zip:
                temp_file.unlink(missing_ok=True)
                raise ValueError(f"Downloaded archive for {quarter_key} is not a valid zip file or is corrupted.")

            # Synthetic fixture guard
            synthetic_detected = self.scan_for_synthetic_markers(temp_file)
            if synthetic_detected:
                temp_file.unlink(missing_ok=True)
                raise ValueError(
                    f"Rejecting archive for {quarter_key}: Contains synthetic test markers {synthetic_detected}. "
                    f"Synthetic fixtures cannot be ingested or recorded as official FDA data."
                )

            if target_file.exists():
                target_file.unlink()
            temp_file.replace(target_file)

            record = {
                "quarter": quarter_key,
                "filename": archive_name,
                "source_url": url,
                "file_size_bytes": total_bytes,
                "sha256_checksum": calculated_sha256,
                "download_timestamp": datetime.now(timezone.utc).isoformat(),
                "verification_status": "VERIFIED",
                "provenance_type": "OFFICIAL_FDA",
                "zip_crc_integrity": "PASSED",
                "synthetic_markers_detected": [],
                "tables_found": [f for f in contained_files if any(t in f.upper() for t in ["DEMO", "DRUG", "REAC", "OUTC", "INDI", "THER", "RPSR"])],
                "total_archive_files": len(contained_files)
            }

            self.manifest["archives"][quarter_key] = record
            self._save_manifest()
            logger.info("Successfully verified and registered %s in manifest (SHA-256: %s)", quarter_key, calculated_sha256)
            return record

        except Exception as e:
            logger.error("Download failed for quarter %s: %s", quarter_key, e)
            temp_file.unlink(missing_ok=True)
            raise


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="PharmaSafe FDA FAERS Archive Download Manager")
    parser.add_argument("--quarter", type=str, default="2024Q3", help="Quarter ID to download (e.g. 2024Q3)")
    parser.add_argument("--force", action="store_true", help="Force re-download even if present")
    parser.add_argument("--timeout", type=int, default=180, help="Download timeout in seconds")
    args = parser.parse_args()

    dm = DownloadManager()
    logger.info("Starting download for quarter %s...", args.quarter)
    try:
        rec = dm.download_quarter(args.quarter, timeout=args.timeout, force=args.force)
        print("\n=== DOWNLOAD SUCCESSFUL ===")
        print(json.dumps(rec, indent=2))
    except Exception as e:
        logger.error("Download failed: %s", e)
        sys.exit(1)
