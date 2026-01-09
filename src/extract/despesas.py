import os
import requests
import pandas as pd
from datetime import datetime
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
TABLE = "despesas"
INDEX_NAME = "idx_bronze_despesas_coddocumento"

ANO_ATUAL = datetime.now().year
ANO_MINIMO = ANO_ATUAL - 1


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


def get_despesas_deputado_por_ano(deputado_id, ano):
    despesas = []
    url = f"{API_BASE_URL}/deputados/{deputado_id}/despesas"
    params = {
        "itens": ITENS_POR_PAGINA,
        "ano": ano
    }

    while url:
        try:
            resp = requests.get(url, params=params, timeout=30)

            # 🚨 Bug conhecido da API (pagina inválida)
            if resp.status_code == 400:
                break

            resp.raise_for_status()
            payload = resp.json()

        except requests.RequestException as e:
            print(f"⚠️ Erro API | deputado={deputado_id} | ano={ano} | {e}")
            break

        dados = payload.get("dados", [])
        if not dados:
            break

        despesas.extend(dados)

        next_url = None
        for link in payload.get("links", []):
            if link.get("rel") == "next":
                next_url = link.get("href")

        url = next_url
        params = None

    return despesas


# ===============================
# MAIN
# ===============================
if __name__ == "__main__":
    print("🔹 Iniciando ingestão de despesas")

    deputados = get_all_deputados()
    print(f"👤 Deputados encontrados: {len(deputados)}")

    anos = list(range(ANO_MINIMO, ANO_ATUAL + 1))
    print(f"📅 Anos considerados: {anos}")

    all_despesas = []

    for idx, dep in enumerate(deputados, start=1):
        dep_id = dep.get("id")
        dep_nome = dep.get("nome")

        print(f"\n➡️ [{idx}/{len(deputados)}] Deputado: {dep_nome}")

        for ano in anos:
            print(f"   📆 Ano {ano}")

            despesas = get_despesas_deputado_por_ano(dep_id, ano)

            for d in despesas:
                d["deputado_id"] = dep_id
                d["deputado_nome"] = dep_nome
                d["ano_ref"] = ano

            all_despesas.extend(despesas)

    df = pd.DataFrame(all_despesas)
    print(f"\n💰 Total de despesas: {len(df)}")

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    with engine.begin() as conn:
        print("🔹 Garantindo schema bronze...")
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))

        print("🔹 Gravando despesas no PostgreSQL...")
        df.to_sql(
            TABLE,
            conn,
            schema=SCHEMA,
            if_exists="replace",
            index=False,
            chunksize=10_000
        )

        print("🔹 Criando índice em bronze.despesas(codDocumento)...")
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS {INDEX_NAME}
            ON {SCHEMA}.{TABLE} ("codDocumento")
        """))

    print("✅ Ingestão de despesas concluída com sucesso")
