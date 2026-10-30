import requests
import re
# no bs4

portal_url = "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"
headers = {"User-Agent": "PharmaSafe/1.0 (Pharmacovigilance Research)"}

r = requests.get(portal_url, headers=headers, timeout=15)
links = [l for l in re.findall(r'href=["\']([^"\']+)["\']', r.text) if "faers_ascii" in l.lower()]

target_quarters = [
    "2024Q3", "2024Q4",
    "2025Q1", "2025Q2", "2025Q3", "2025Q4",
    "2026Q1", "2026Q2"
]

quarter_links = {}
for q in target_quarters:
    pattern = re.compile(rf"faers_ascii_{q}\.zip", re.IGNORECASE)
    matching = [l for l in links if pattern.search(l)]
    quarter_links[q] = matching[0] if matching else None

print("Resolved Official Links:")
for q, url in quarter_links.items():
    print(f"{q}: {url}")

print("\nTesting HTTP HEAD / Stream on each URL:")
for q, url in quarter_links.items():
    if not url:
        print(f"{q}: NOT FOUND IN OFFICIAL LISTING")
        continue
    try:
        head_resp = requests.head(url, headers=headers, timeout=10)
        # Check stream
        stream_resp = requests.get(url, stream=True, headers=headers, timeout=10)
        chunk = next(stream_resp.iter_content(1024))
        is_zip = chunk[:4] == b"PK\x03\x04"
        print(f"{q}: HTTP {stream_resp.status_code}, Chunked: {stream_resp.headers.get('transfer-encoding')}, Zip magic: {is_zip}")
    except Exception as e:
        print(f"{q}: ERROR {e}")

