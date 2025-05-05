import requests
from bs4 import BeautifulSoup
from config import BASE_URL, HEADERS

def get_all_managers():
    
    # Obtiene todos los managers
     
    url = f"{BASE_URL}/managers"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    
    # Extrae los nombres y CIK de los managers
    
    managers = []
    for link in soup.select("a"):
        href = link.get("href", "")
        if href.startswith("/manager/") and "-q" not in href:
            full_name = link.text.strip()
            cik = href.split("/manager/")[1].split("-")[0]  # Extrae solo el CIK
            managers.append({
                "name": full_name,
                "cik": cik
            })

    return managers
