import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from config import BASE_URL, HEADERS

def fetch_filings(manager):
    
    try:
        if isinstance(manager, dict):
            cik = manager.get("cik")
            manager_name = manager.get("name", "Unknown Fund")
        else:
            # Fallback si solo es un CIK y no trae el nombre
            cik = manager
            manager_name = "Unknown Fund"

        if not cik:
            print(f"Manager sin CIK válido: {manager}")
            return []

        url = f"{BASE_URL}/manager/{cik}"

        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("table tr")[1:]  # Toma todas las filas de la tabla, omitiendo el encabezado

        filings = []
        for row in rows:
            cols = row.select("td") # Extrae cada celda de la fila
            if len(cols) < 4:
                continue    # Saltar filas con menos de 4 celdas

            quarter = cols[0].text.strip()  # Trimestre
            date = cols[5].text.strip()     # Fecha
            link_tag = cols[0].select_one("a")
            filing_url = BASE_URL + link_tag["href"] if link_tag else None  # URL

            if filing_url:
                filings.append({
                    "cik": cik,
                    "fund_name": manager_name,
                    "quarter": quarter,
                    "date": date,
                    "url": filing_url,
                })
        return filings

    except Exception as e:
        print(f"Error procesando manager {manager}: {e}")
        return []

def get_all_filings(managers):
    all_filings = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(fetch_filings, m) for m in managers]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Procesando filings"):
            result = future.result()
            all_filings.extend(result)

    return all_filings
