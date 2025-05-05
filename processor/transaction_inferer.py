import pandas as pd
from tqdm import tqdm

def infer_transactions(df):
    tqdm.pandas(desc="Inferiendo transacciones")

    # Extraer año y número de trimestre para ordenar cronológicamente en formato QX YYYY
    
    quarters = df["quarter"].str.extract(r"Q([1-4])\s+(\d{4})")
    df["qnum"] = quarters[0].astype(int)
    df["year"] = quarters[1].astype(int)

    # Ordenar primero por fondo, símbolo, año y trimestre
    df = df.sort_values(
        by=["fund_name", "stocksymbol", "year", "qnum"],
        ignore_index=True
    )

    # Eliminar columnas auxiliares
    df = df.drop(columns=["qnum", "year", "url"], errors="ignore")

    # Calcular cambios absolutos y relativos
    df["shares"] =      pd.to_numeric(df["shares"], errors="coerce")
    df["change"] =      df.groupby(["fund_name", "stocksymbol"])["shares"].diff()
    df["pct_change"] =  df.groupby(["fund_name", "stocksymbol"])["shares"].pct_change()

    # Clasificar
    def classify(difference):
        if pd.isna(difference):
            return "new"
        elif difference > 0:
            return "buy"
        elif difference < 0:
            return "sell"
        else:
            return "hold"

    df["inferred_transaction_type"] = df["change"].progress_apply(classify)

    return df
