from scraper.manager_scraper import get_all_managers
from scraper.filing_scraper import get_all_filings
from scraper.holdings_scraper import get_holdings_for_filings, FINAL_OUTPUT_CSV
from processor.transaction_inferer import infer_transactions
import pandas as pd
import os

def main():
    print("Scrapeando managers")
    managers = get_all_managers()

    print( "Scrapeando filings")
    all_filings = get_all_filings(managers)

    print("Scrapeando holdings...")
    all_holdings = get_holdings_for_filings(all_filings)

    print("Infiriendo transacciones...")

    if isinstance(all_holdings, list):
        df = pd.DataFrame(all_holdings)
    else:
        df = all_holdings

    if df.empty:
        print("FATAL ERROR: No se encontraron holdings para procesar.")
        return

    result_df = infer_transactions(df)

    # Guardar resultados
    os.makedirs("data", exist_ok=True)
    result_df.to_csv(FINAL_OUTPUT_CSV, index=False)
    print(f"SUCCESS: Transacciones inferidas guardadas en {FINAL_OUTPUT_CSV}")

if __name__ == "__main__":
    main()
