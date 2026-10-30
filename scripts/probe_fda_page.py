import requests
import re

urls_to_test = [
    "https://www.fda.gov/drugs/questions-and-answers-fdas-adverse-event-reporting-system-faers/fda-adverse-event-reporting-system-faers-latest-quarterly-data-files",
    "https://www.fda.gov/drugs/surveillance/fda-adverse-event-reporting-system-faers-latest-quarterly-data-files",
    "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"
]

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

for u in urls_to_test:
    try:
        r = requests.get(u, headers=headers, timeout=15, allow_redirects=True)
        print(f"URL: {u}\nStatus: {r.status_code}\nFinal: {r.url}")
        zip_links = [l for l in re.findall(r'href=["\']([^"\']+)["\']', r.text) if ".zip" in l.lower()]
        print(f"Zip links ({len(zip_links)}):")
        for zl in zip_links[:20]:
            print("  ", zl)
    except Exception as e:
        print(f"Error {u}: {e}")
