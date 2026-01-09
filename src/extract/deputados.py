import os
import requests
import pandas as pd
from datetime import datetime

API_URL = "https://dadosabertos.camara.leg.br/api/v2/deputados"
RAW_PATH = "data/raw"


def extract_all_deputados():
    all_data = []
    url = API_URL
    params = {"itens": 100}  # máximo permitido

    while url:
        response = requests.get(url, params=params)
        response.raise_for_status()

        payload = response.json()
        all_data.extend(payload["dados"])

        # procura o link da próxima página
        next_url = None
        for link in payload.get("links", []):
            if link.get("rel") == "next":
                next_url = link.get("href")

        url = next_url
        params = None  # parâmetros só na primeira chamada

    df = pd.DataFrame(all_data)
    return df


def save_parquet(df: pd.DataFrame):
    os.makedirs(RAW_PATH, exist_ok=True)

    ref_date = datetime.now().strftime("%Y%m%d")
    file_path = f"{RAW_PATH}/deputados_{ref_date}.parquet"

    df.to_parquet(file_path, index=False)

    print(f"💾 Arquivo salvo em: {file_path}")


if __name__ == "__main__":
    print("🔹 Iniciando extração COMPLETA de deputados...")

    df = extract_all_deputados()

    print("✅ Extração concluída")
    print(f"🔢 Total de registros: {len(df)}")
    print(df.head())

    save_parquet(df)
