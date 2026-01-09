import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ===============================
# ENV
# ===============================
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# ===============================
# CONFIG
# ===============================
API_BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
ITENS_POR_PAGINA = 100
SCHEMA = "bronze"
TABLE = "deputados"
INDEX_NAME = "idx_bronze_deputados_id"


# ===============================
# FUNÇÕES API
# ===============================
def get_all_deputados():
    url = f"{API_BASE_URL}/deputados"
    params = {"itens": ITENS_POR_PAGINA}
    deputados = []

    while url:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()

        payload = resp.json()
        deputados.extend(payload.get("dados", []))

        next_url = None
        for link in payload.get("links", []):
            if link.get("rel") == "next":
                next_url = link.get("href")

        url = next_url
        params = None

    return deputados


# ===============================
# MAIN
# ===============================
if __name__ == "__main__":
    print("🔹 Iniciando ingestão de deputados")

    deputados = get_all_deputados()
    print(f"👤 Total de deputados: {len(deputados)}")

    df = pd.DataFrame(deputados)

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    with engine.begin() as conn:
        print("🔹 Garantindo schema bronze...")
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))

        print("🔹 Gravando deputados no PostgreSQL...")
        df.to_sql(
            TABLE,
            conn,
            schema=SCHEMA,
            if_exists="replace",
            index=False,
            chunksize=1_000
        )

        print("🔹 Criando índice em bronze.deputados(id)...")
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS {INDEX_NAME}
            ON {SCHEMA}.{TABLE} (id)
        """))

    print("✅ Ingestão de deputados concluída com sucesso")
