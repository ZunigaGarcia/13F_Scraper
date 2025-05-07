import string
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from config import BASE_URL, HEADERS

# Configuración de sesión con reintentos y backoff
session = requests.Session()
session.headers.update(HEADERS)
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)


def fetch_managers_by_letter(letter):
    
    # Scrapea la página de managers que empiezan por una letra o dígito.
    
    url = f"{BASE_URL}/managers/{letter}"
    try:
        r = session.get(url, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        managers = []
        for row in soup.select("table tr")[1:]:
            link = row.select_one("td a[href^='/manager/']")
            if not link:
                continue
            href = link["href"]
            name = link.text.strip()
            cik = href.split("/manager/")[1].split("-", 1)[0]
            managers.append({"name": name, "cik": cik})
        return managers

    except Exception as e:
        print(f"ERROR al screapear letra {letter}: {e}")
        return []


def get_all_managers():
    
    # Obtiene todos los managers A-Z y 0 y los guarda en una lista
    
    letters = list(string.ascii_uppercase) + ['0']
    all_managers = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_managers_by_letter, lt): lt for lt in letters}
        for future in tqdm(as_completed(futures), total=len(futures), desc="🔍 Scrapeando managers por letra"):
            result = future.result()
            if result:
                all_managers.extend(result)

    # Eliminamos duplicados según CIK
    seen = set()
    unique = []
    for m in all_managers:
        if m['cik'] not in seen:
            seen.add(m['cik'])
            unique.append(m)

    return unique