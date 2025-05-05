import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from config import BASE_URL, HEADERS

FINAL_OUTPUT_CSV = "all_holdings.csv"

# Configuración de sesión con reintentos y backoff
session = requests.Session()
session.headers.update(HEADERS)
retry_strategy = Retry(
    total=5,                # Número total de reintentos
    backoff_factor=1,       # Tiempo de espera exponencial entre intentos       
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
# Monta el adaptador de reintentos
adapter = HTTPAdapter(max_retries=retry_strategy)
# Agrega el adaptador a la sesión
session.mount("https://", adapter)
session.mount("http://", adapter)


def get_holdings_for_filings(filings):
    
    all_data = []

    # Procesamiento en paralelo de todos los filings
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {executor.submit(process_filing, filing): filing for filing in filings}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Procesando filings"):
            result = future.result()
            if result:
                all_data.extend(result)

    # Guardar CSV final
    df = pd.DataFrame(all_data)
    if not df.empty:
        df.to_csv(FINAL_OUTPUT_CSV, index=False)
    return all_data


def process_filing(filing):

    url = filing['url']
    try:
        # Descarga el HTML y localiza la tabla con data-url
        r = session.get(url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'html.parser')
        # Localiza la tabla con atributo data-url
        table = soup.find("table", {"id": "filingAggregated"})
        if not table or "data-url" not in table.attrs:
            print(f"ATENCIÓN: En [{url}] tabla 'filingAggregated' o atributo data-url no encontrado")
            return []

        # Extrae nombres de columnas del encabezado
        headers = [th.text.strip() for th in table.find_all("th")]

        # Descarga JSON de holdings usando data-url
        data_url = BASE_URL + table["data-url"]
        resp = session.get(data_url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        rows = payload.get("data", [])

        # Itera sobre las filas y filtra solo la clase COM
        data = []
        for rec in rows:
            record = dict(zip(headers, rec))
            cl = record.get("Cl", "").strip().upper()
            if cl not in ("COM", "COMMON STOCK"):
                continue
            # Construye el registro del holding
            data.append({
                "fund_name":   filing["fund_name"],
                "filing_date": filing["date"],
                "quarter":     filing["quarter"],
                "stocksymbol": record.get("Sym"),
                "cl":          record.get("Cl"),
                "value($000)": record.get("Value ($000)"),
                "shares":      record.get("Shares"),
                "url":         url
            })

        if not data:
            print(f"ATENCIÓN: EN [{url}] No se encontraron holdings COM") 
        return data

    except Exception as e:
        print(f"ERROR en [{url}] EXCEPCIÓN: {e}")
        return []
