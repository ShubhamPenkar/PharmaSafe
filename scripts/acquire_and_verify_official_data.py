"""
PharmaSafe Official FDA Dataset Acquisition & Verification
Downloads, validates, and profiles the 8 target consecutive quarters from the official FDA source:
- 2024Q3, 2024Q4, 2025Q1, 2025Q2, 2025Q3, 2025Q4, 2026Q1, 2026Q2
Preserves complete provenance, verifies zip integrity and member CRC,
inspects ASCII tables (DEMO, DRUG, REAC, OUTC, INDI, THER, RPSR),
checks for synthetic contamination, and produces trustworthy manifest.json records.
"""

import os
import sys
import time
import json
import zipfile
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger

logger = get_logger("PharmaSafe.OfficialAcquisition")

OFFICIAL_SOURCE_PAGE = (
    "https://www.fda.gov/drugs/fda-adverse-event-monitoring-system-aems/"
    "fda-adverse-event-monitoring-system-aems-latest-quarterly-data-files"
)

TARGET_QUARTERS = [
    {
        "quarter_id": "2024Q3",
        "coverage": "July–September 2024",
        "url": "https://fis.fda.gov/content/Exports/faers_ascii_2024q3.zip",
        "filename": "faers_ascii_2024q3.zip",
        "year": 2024,
        "quarter": 3,
        "yy": "24"
    },
    {
        "quarter_id": "2024Q4",
        "coverage": "October–December 2024",
        "url": "https://fis.fda.gov/content/Exports/faers_ascii_2024Q4.zip",
        "filename": "faers_ascii_2024Q4.zip",
        "year": 2024,
        "quarter": 4,
        "yy": "24"
    },
    {
        "quarter_id": "2025Q1",
        "coverage": "January–March 2025",
        "url": "https://fis.fda.gov/content/Exports/faers_ascii_2025q1.zip",
        "filename": "faers_ascii_2025q1.zip",
        "year": 2025,
        "quarter": 1,
        "yy": "25"
    },
    {
        "quarter_id": "2025Q2",
        "coverage": "April–June 2025",
        "url": "https://fis.fda.gov/content/Exports/faers_ascii_2025q2.zip",
        "filename": "faers_ascii_2025q2.zip",
        "year": 2025,
        "quarter": 2,
        "yy": "25"
    },
    {
        "quarter_id": "2025Q3",
        "coverage": "July–September 2025",
        "url": "https://fis.fda.gov/content/Exports/faers_ascii_2025q3.zip",
        "filename": "faers_ascii_2025q3.zip",
        "year": 2025,
        "quarter": 3,
        "yy": "25"
    },
    {
        "quarter_id": "2025Q4",
        "coverage": "October–December 2025",
        "url": "https://fis.fda.gov/content/Exports/faers_ascii_2025Q4.zip",
        "filename": "faers_ascii_2025Q4.zip",
        "year": 2025,
        "quarter": 4,
        "yy": "25"
    },
    {
        "quarter_id": "2026Q1",
        "coverage": "January–March 2026",
        "url": "https://fis.fda.gov/content/Exports/faers_ascii_2026q1.zip",
        "filename": "faers_ascii_2026q1.zip",
        "year": 2026,
        "quarter": 1,
        "yy": "26"
    },
    {
        "quarter_id": "2026Q2",
        "coverage": "April–June 2026",
        "url": "https://fis.fda.gov/content/Exports/faers_ascii_2026q2.zip",
        "filename": "faers_ascii_2026q2.zip",
        "year": 2026,
        "quarter": 2,
        "yy": "26"
    }
]

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

CORE_TABLE_PREFIXES = ["DEMO", "DRUG", "REAC", "OUTC", "INDI", "THER", "RPSR"]


def calculate_file_sha256(file_path: Path) -> str:
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(131072):
            hasher.update(chunk)
    return hasher.hexdigest()


def download_official_quarter(
    target: Dict[str, Any],
    raw_official_dir: Path,
    max_retries: int = 3,
    timeout: int = 180
) -> Dict[str, Any]:
    q_id = target["quarter_id"]
    url = target["url"]
    filename = target["filename"]
    q_dir = raw_official_dir / q_id
    q_dir.mkdir(parents=True, exist_ok=True)
    target_zip = q_dir / filename
    temp_zip = q_dir / f"{filename}.download.tmp"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "*/*"
    }

    # If target exists and is a valid zip, check if we need to re-download
    if target_zip.exists():
        logger.info("[%s] Existing file found at %s. Validating...", q_id, target_zip)
        try:
            with zipfile.ZipFile(target_zip, "r") as zf:
                if zf.testzip() is None:
                    logger.info("[%s] Existing file passed zip CRC test! Reusing existing verified archive.", q_id)
                    sha256_hash = calculate_file_sha256(target_zip)
                    return {
                        "status": "EXISTING_VERIFIED",
                        "path": target_zip,
                        "sha256": sha256_hash,
                        "size_bytes": target_zip.stat().st_size,
                        "url": url,
                        "final_url": url,
                        "download_timestamp": datetime.fromtimestamp(target_zip.stat().st_mtime, tz=timezone.utc).isoformat(),
                        "http_status": 200
                    }
        except Exception as e:
            logger.warning("[%s] Existing archive invalid (%s). Re-downloading...", q_id, e)
            target_zip.unlink(missing_ok=True)

    for attempt in range(1, max_retries + 1):
        logger.info("[%s] Download attempt %d/%d from %s...", q_id, attempt, max_retries, url)
        try:
            start_t = time.time()
            with requests.get(url, headers=headers, stream=True, timeout=timeout) as response:
                response.raise_for_status()
                final_url = response.url
                status_code = response.status_code

                hasher = hashlib.sha256()
                total_bytes = 0

                with open(temp_zip, "wb") as f:
                    for chunk in response.iter_content(chunk_size=262144):
                        if chunk:
                            f.write(chunk)
                            hasher.update(chunk)
                            total_bytes += len(chunk)

                elapsed = time.time() - start_t
                speed_mb_s = (total_bytes / (1024 * 1024)) / max(elapsed, 0.001)
                logger.info("[%s] Downloaded %.2f MB in %.1fs (%.2f MB/s).", q_id, total_bytes / (1024 * 1024), elapsed, speed_mb_s)

                # Verify zip integrity
                logger.info("[%s] Verifying zip CRC integrity...", q_id)
                with zipfile.ZipFile(temp_zip, "r") as zf:
                    corrupted = zf.testzip()
                    if corrupted is not None:
                        raise ValueError(f"Corrupted file member detected in zip: {corrupted}")

                # Atomically replace target
                if target_zip.exists():
                    target_zip.unlink()
                temp_zip.replace(target_zip)

                calculated_sha = hasher.hexdigest()
                logger.info("[%s] Successfully verified and saved archive (SHA-256: %s)", q_id, calculated_sha)

                return {
                    "status": "DOWNLOADED_AND_VERIFIED",
                    "path": target_zip,
                    "sha256": calculated_sha,
                    "size_bytes": total_bytes,
                    "url": url,
                    "final_url": final_url,
                    "download_timestamp": datetime.now(timezone.utc).isoformat(),
                    "http_status": status_code
                }
        except Exception as e:
            logger.warning("[%s] Attempt %d failed: %s", q_id, attempt, e)
            temp_zip.unlink(missing_ok=True)
            if attempt == max_retries:
                logger.error("[%s] All download attempts failed.", q_id)
                raise
            time.sleep(3)


def inspect_quarterly_archive(
    target: Dict[str, Any],
    zip_path: Path
) -> Dict[str, Any]:
    """
    Performs comprehensive structural and content validation on a quarterly FAERS archive:
    - Lists all members
    - Discovers README / source documentation
    - Validates all 7 tables (headers, rows, delimiter, column count)
    - Checks for synthetic markers
    - Profiles primaryid uniqueness and relationships
    """
    q_id = target["quarter_id"]
    yy = target["yy"]
    q_num = target["quarter"]

    results = {
        "quarter": q_id,
        "archive_filename": zip_path.name,
        "members_count": 0,
        "members": [],
        "readme_file": None,
        "readme_snippet": None,
        "tables": {},
        "synthetic_markers_found": [],
        "validation_passed": True,
        "errors": []
    }

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        results["members_count"] = len(members)
        results["members"] = members

        # Look for README / documentation
        for m in members:
            if any(term in m.upper() for term in ["README", "READ_ME", "DOCUMENTATION"]):
                results["readme_file"] = m
                try:
                    raw = zf.read(m)
                    text = raw.decode("utf-8", errors="replace")[:1000]
                    results["readme_snippet"] = text.strip()
                except Exception as ex:
                    results["readme_snippet"] = f"Binary or unreadable doc: {ex}"
                break

        # Validate core tables
        primaryids_demo = set()
        primaryids_drug = set()
        caseids_demo = set()

        for prefix in CORE_TABLE_PREFIXES:
            # Look for member starting with prefix
            matching = [m for m in members if Path(m).name.upper().startswith(prefix) and Path(m).name.upper().endswith((".TXT", ".DAT"))]
            if not matching:
                results["errors"].append(f"Missing core table: {prefix}")
                results["validation_passed"] = False
                continue

            member_name = matching[0]
            with zf.open(member_name, "r") as f:
                # Read line by line with streaming to avoid loading 200MB into RAM all at once
                header_line = f.readline().decode("utf-8", errors="replace").strip()
                delimiter = "$" if "$" in header_line else "\t"
                columns = [c.strip().lower() for c in header_line.split(delimiter)]
                num_cols = len(columns)

                row_count = 0
                malformed_count = 0
                sample_rows = []
                markers_in_table = set()

                for line_b in f:
                    line = line_b.decode("utf-8", errors="replace").strip()
                    if not line:
                        continue
                    row_count += 1
                    parts = line.split(delimiter)
                    if len(parts) != num_cols:
                        malformed_count += 1

                    # Check synthetic markers
                    for sm in SYNTHETIC_MARKERS:
                        if sm in line:
                            markers_in_table.add(sm)

                    # Extract primaryids for relationship checks
                    if row_count <= 5:
                        sample_rows.append(parts)

                    if prefix == "DEMO" and len(parts) >= 2:
                        primaryids_demo.add(parts[0])
                        caseids_demo.add(parts[1])
                    elif prefix == "DRUG" and len(parts) >= 1:
                        if row_count <= 50000:  # sample first 50k
                            primaryids_drug.add(parts[0])

            if markers_in_table:
                results["synthetic_markers_found"].extend(list(markers_in_table))
                results["validation_passed"] = False
                results["errors"].append(f"Synthetic markers found in {prefix}: {list(markers_in_table)}")

            results["tables"][prefix] = {
                "member_name": member_name,
                "delimiter": delimiter,
                "column_count": num_cols,
                "columns": columns,
                "row_count": row_count,
                "malformed_count": malformed_count,
                "sample_header": header_line
            }

        # Check relational integrity between DEMO and DRUG (sampled)
        if primaryids_demo and primaryids_drug:
            overlap = primaryids_drug.intersection(primaryids_demo)
            results["demo_unique_primaryids"] = len(primaryids_demo)
            results["demo_unique_caseids"] = len(caseids_demo)
            results["demo_duplicate_cases_in_quarter"] = len(primaryids_demo) - len(caseids_demo)
            results["drug_sampled_demo_link_rate"] = len(overlap) / max(len(primaryids_drug), 1)

    return results


def run_acquisition_and_verification():
    root = PROJECT_ROOT
    raw_dir = root / "data/raw"
    official_dir = raw_dir / "official"
    official_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = raw_dir / "manifest.json"

    logger.info("=================================================================")
    logger.info("PHARMASAFE: ACQUIRING & VERIFYING 8 TARGET CONSECUTIVE QUARTERS")
    logger.info("Official Source Page: %s", OFFICIAL_SOURCE_PAGE)
    logger.info("Target Quarters: %s", [t["quarter_id"] for t in TARGET_QUARTERS])
    logger.info("=================================================================")

    manifest_data = {
        "version": "2.0",
        "provenance_standard": "FDA_FAERS_OFFICIAL_ARCHIVES",
        "official_source_page": OFFICIAL_SOURCE_PAGE,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total_target_quarters": len(TARGET_QUARTERS),
        "verified_quarters_count": 0,
        "archives": {}
    }

    all_quarter_profiles = []

    for target in TARGET_QUARTERS:
        q_id = target["quarter_id"]
        logger.info("\n--- Processing Quarter %s (%s) ---", q_id, target["coverage"])
        
        # 1. Download & verify archive CRC
        dl_res = download_official_quarter(target, official_dir)
        zip_path = Path(dl_res["path"])

        # 2. Inspect structure, tables, schemas, and check synthetic contamination
        inspection = inspect_quarterly_archive(target, zip_path)
        all_quarter_profiles.append(inspection)

        # 3. Build manifest record
        archive_record = {
            "quarter": q_id,
            "coverage_period": target["coverage"],
            "year": target["year"],
            "quarter_number": target["quarter"],
            "filename": target["filename"],
            "relative_path": str(zip_path.relative_to(root)),
            "official_source_page": OFFICIAL_SOURCE_PAGE,
            "download_url": target["url"],
            "final_response_url": dl_res.get("final_url", target["url"]),
            "download_timestamp": dl_res["download_timestamp"],
            "http_status": dl_res.get("http_status", 200),
            "file_size_bytes": dl_res["size_bytes"],
            "file_size_mb": round(dl_res["size_bytes"] / (1024 * 1024), 2),
            "sha256_checksum": dl_res["sha256"],
            "zip_crc_integrity": "PASSED",
            "readme_discovered": inspection["readme_file"],
            "tables_found": list(inspection["tables"].keys()),
            "total_archive_members": inspection["members_count"],
            "tables_summary": {
                k: {
                    "member": v["member_name"],
                    "rows": v["row_count"],
                    "columns": v["column_count"],
                    "delimiter": v["delimiter"]
                }
                for k, v in inspection["tables"].items()
            },
            "synthetic_markers_detected": inspection["synthetic_markers_found"],
            "classification": "VERIFIED_OFFICIAL" if inspection["validation_passed"] and not inspection["synthetic_markers_found"] else "FAILED_VERIFICATION",
            "verification_status": "VERIFIED_OFFICIAL" if inspection["validation_passed"] else "FAILED"
        }

        if archive_record["verification_status"] == "VERIFIED_OFFICIAL":
            manifest_data["verified_quarters_count"] += 1

        manifest_data["archives"][q_id] = archive_record

    # Save manifest
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)
    logger.info("Manifest saved to %s (Verified %d/%d quarters)", manifest_path, manifest_data["verified_quarters_count"], len(TARGET_QUARTERS))

    # Also save comprehensive audit profile JSON
    profile_out = root / "reports/official_dataset_profile.json"
    profile_out.parent.mkdir(parents=True, exist_ok=True)
    with open(profile_out, "w", encoding="utf-8") as f:
        json.dump(all_quarter_profiles, f, indent=2)
    logger.info("Saved detailed official dataset profile to %s", profile_out)

    return manifest_data, all_quarter_profiles


if __name__ == "__main__":
    run_acquisition_and_verification()

