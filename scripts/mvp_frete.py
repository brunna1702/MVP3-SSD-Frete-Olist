"""
mvp_frete.py — código de apoio do MVP "Onde subsidiar o frete no e-commerce brasileiro".

Disciplina: Sistemas de Suporte à Decisão — UnB / Departamento de Engenharia de Produção
Aluna: Brunna Gavião Moraes — 231025341

O módulo concentra tudo o que é reaproveitado pelos notebooks:

* ``Plataforma``  — camada fina que isola a nuvem. No Databricks grava tabelas Delta no
  Unity Catalog e arquivos em um Volume; fora dele (execução local de teste) grava Parquet
  e consulta com DuckDB. Os notebooks escrevem o mesmo código nos dois ambientes.
* coleta        — hash SHA-256 e metadados dos arquivos da camada bronze;
* qualidade     — perfil por atributo e verificações por dimensão de qualidade;
* transformação — regras R01–R16 (bronze → silver → gold), cada uma registrada no log de linhagem;
* catálogo      — Catálogo de Dados do modelo estrela, que gera a documentação, o esquema,
  os comentários e as restrições das tabelas e valida os dados antes da carga.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Parâmetros gerais
# ---------------------------------------------------------------------------
CATALOGO = "workspace"                     # catálogo padrão do Databricks Free Edition
SCHEMAS = {"bronze": "frete_bronze", "silver": "frete_silver", "gold": "frete_gold"}
VOLUME_BRONZE = "arquivos"                 # Volume (armazenamento de arquivos) na camada bronze

KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
IBGE_URL = "https://apisidra.ibge.gov.br/values/t/6579/n3/all/v/9324/p/2018"

ARQUIVOS_OLIST = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]
ARQUIVO_IBGE = "ibge_populacao_uf_2018.csv"

# Janela de análise (regra R08): compras de jan/2017 a ago/2018.
JANELA_INICIO = pd.Timestamp("2017-01-01")
JANELA_FIM = pd.Timestamp("2018-09-01")    # exclusivo
STATUS_FORA = ["canceled", "unavailable"]   # pedidos que não geraram envio

# Caixa que contém o território brasileiro (regra R04)
BR_LAT = (-33.75, 5.30)
BR_LON = (-74.00, -34.75)

FATOR_CUBAGEM = 6000  # cm³ por kg — fator usual das transportadoras e dos Correios (regra R06)

UF_INFO = {  # sigla: (código IBGE, nome, região)
    "RO": (11, "Rondônia", "Norte"), "AC": (12, "Acre", "Norte"), "AM": (13, "Amazonas", "Norte"),
    "RR": (14, "Roraima", "Norte"), "PA": (15, "Pará", "Norte"), "AP": (16, "Amapá", "Norte"),
    "TO": (17, "Tocantins", "Norte"), "MA": (21, "Maranhão", "Nordeste"), "PI": (22, "Piauí", "Nordeste"),
    "CE": (23, "Ceará", "Nordeste"), "RN": (24, "Rio Grande do Norte", "Nordeste"),
    "PB": (25, "Paraíba", "Nordeste"), "PE": (26, "Pernambuco", "Nordeste"), "AL": (27, "Alagoas", "Nordeste"),
    "SE": (28, "Sergipe", "Nordeste"), "BA": (29, "Bahia", "Nordeste"), "MG": (31, "Minas Gerais", "Sudeste"),
    "ES": (32, "Espírito Santo", "Sudeste"), "RJ": (33, "Rio de Janeiro", "Sudeste"),
    "SP": (35, "São Paulo", "Sudeste"), "PR": (41, "Paraná", "Sul"), "SC": (42, "Santa Catarina", "Sul"),
    "RS": (43, "Rio Grande do Sul", "Sul"), "MS": (50, "Mato Grosso do Sul", "Centro-Oeste"),
    "MT": (51, "Mato Grosso", "Centro-Oeste"), "GO": (52, "Goiás", "Centro-Oeste"),
    "DF": (53, "Distrito Federal", "Centro-Oeste"),
}
UFS = sorted(UF_INFO)
REGIOES = ["Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"]

RAIZ_REPO = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Plataforma: Databricks (nuvem) ou execução local de teste
# ---------------------------------------------------------------------------
class Plataforma:
    """Isola as chamadas específicas da nuvem.

    Databricks: tabelas Delta em ``workspace.frete_<camada>.<tabela>`` e arquivos no Volume
    ``/Volumes/workspace/frete_bronze/arquivos``. Local: Parquet em ``_local/`` + DuckDB.
    """

    def __init__(self, spark=None, raiz_local: str | os.PathLike | None = None):
        self.spark = spark
        self.databricks = spark is not None
        if self.databricks:
            self.dir_bronze = Path(f"/Volumes/{CATALOGO}/{SCHEMAS['bronze']}/{VOLUME_BRONZE}")
        else:
            self.raiz_local = Path(raiz_local) if raiz_local else RAIZ_REPO / "_local"
            self.dir_bronze = self.raiz_local / "bronze"
            import duckdb
            self._duck = duckdb.connect()
            self._registrar_views_locais()
        self.dir_evidencias = self._definir_dir_evidencias()

    # -- preparação ----------------------------------------------------------
    def preparar(self):
        """Cria os schemas (bronze, silver, gold) e o Volume da camada bronze."""
        if self.databricks:
            for camada, schema in SCHEMAS.items():
                self.spark.sql(
                    f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{schema} "
                    f"COMMENT 'MVP frete Olist — camada {camada}'"
                )
            self.spark.sql(
                f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMAS['bronze']}.{VOLUME_BRONZE} "
                "COMMENT 'Arquivos originais (CSV) como coletados, com metadados de coleta'"
            )
        self.dir_bronze.mkdir(parents=True, exist_ok=True)
        return self.ambiente()

    def ambiente(self) -> str:
        if self.databricks:
            return f"Databricks — catálogo {CATALOGO}, schemas {', '.join(SCHEMAS.values())}"
        return f"Execução local de teste — {self.raiz_local}"

    def _definir_dir_evidencias(self) -> Path:
        """Pasta /evidencias do repositório; se não for gravável, usa o Volume."""
        alvo = RAIZ_REPO / "evidencias"
        try:
            (alvo / "tabelas").mkdir(parents=True, exist_ok=True)
            teste = alvo / "tabelas" / ".teste_escrita"
            teste.write_text("ok")
            teste.unlink()
            return alvo
        except OSError:
            alt = self.dir_bronze.parent / "evidencias" if not self.databricks else Path(
                f"/Volumes/{CATALOGO}/{SCHEMAS['bronze']}/{VOLUME_BRONZE}/_evidencias")
            alt.mkdir(parents=True, exist_ok=True)
            return alt

    # -- tabelas ---------------------------------------------------------------
    def nome_tabela(self, camada: str, tabela: str) -> str:
        if self.databricks:
            return f"{CATALOGO}.{SCHEMAS[camada]}.{tabela}"
        return tabela if camada == "gold" else f"{camada}__{tabela}"

    def _parquet(self, camada, tabela) -> Path:
        return self.raiz_local / camada / f"{tabela}.parquet"

    def _registrar_views_locais(self):
        for camada in ("silver", "gold"):
            pasta = self.raiz_local / camada
            if pasta.exists():
                for arq in pasta.glob("*.parquet"):
                    self._view_local(camada, arq.stem)

    def _view_local(self, camada, tabela):
        caminho = self._parquet(camada, tabela).as_posix()
        self._duck.execute(
            f'CREATE OR REPLACE VIEW "{self.nome_tabela(camada, tabela)}" AS '
            f"SELECT * FROM read_parquet('{caminho}')"
        )

    def gravar(self, df: pd.DataFrame, camada: str, tabela: str, comentario: str | None = None,
               colunas_catalogo: list[dict] | None = None) -> int:
        """Grava (sobrescreve) uma tabela. Com ``colunas_catalogo``, o esquema vem do catálogo."""
        if self.databricks:
            nome = self.nome_tabela(camada, tabela)
            sdf = self.spark.createDataFrame(_para_spark(df))
            if colunas_catalogo:
                sdf = sdf.selectExpr(*[f"CAST(`{c['coluna']}` AS {TIPOS_SPARK[c['tipo']]}) AS `{c['coluna']}`"
                                       for c in colunas_catalogo])
            (sdf.write.format("delta").mode("overwrite")
                .option("overwriteSchema", "true").saveAsTable(nome))
            if comentario:
                self.spark.sql(f"COMMENT ON TABLE {nome} IS '{_esc(comentario)}'")
            return int(self.spark.table(nome).count())
        caminho = self._parquet(camada, tabela)
        caminho.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(caminho, index=False)
        self._view_local(camada, tabela)
        return int(len(pd.read_parquet(caminho, columns=[df.columns[0]])))

    def ler(self, camada: str, tabela: str) -> pd.DataFrame:
        if self.databricks:
            return self.spark.table(self.nome_tabela(camada, tabela)).toPandas()
        return pd.read_parquet(self._parquet(camada, tabela))

    def remover_tabelas(self, camada: str, tabelas: list[str]):
        for t in tabelas:
            if self.databricks:
                self.spark.sql(f"DROP TABLE IF EXISTS {self.nome_tabela(camada, t)}")
            else:
                p = self._parquet(camada, t)
                if p.exists():
                    self._duck.execute(f'DROP VIEW IF EXISTS "{self.nome_tabela(camada, t)}"')
                    p.unlink()

    def usar_gold(self):
        if self.databricks:
            self.spark.sql(f"USE CATALOG {CATALOGO}")
            self.spark.sql(f"USE SCHEMA {SCHEMAS['gold']}")

    def sql(self, consulta: str) -> pd.DataFrame:
        """Executa uma consulta SQL sobre a camada gold e devolve um DataFrame pandas."""
        if self.databricks:
            return self.spark.sql(consulta).toPandas()
        return self._duck.execute(consulta).df()

    def ddl(self, comando: str) -> bool:
        """Executa DDL que só existe no Databricks (comentários, restrições). Local: ignora."""
        if not self.databricks:
            return False
        self.spark.sql(comando)
        return True


TIPOS_SPARK = {"string": "STRING", "int": "INT", "bigint": "BIGINT", "double": "DOUBLE",
               "date": "DATE", "timestamp": "TIMESTAMP", "boolean": "BOOLEAN"}


def _esc(txt: str) -> str:
    return str(txt).replace("\\", "\\\\").replace("'", "\\'")


def _para_spark(df: pd.DataFrame) -> pd.DataFrame:
    """Converte tipos 'nullable' do pandas para tipos que o Spark entende sem ambiguidade."""
    out = df.copy()
    for c in out.columns:
        s = out[c]
        if pd.api.types.is_extension_array_dtype(s.dtype):
            if pd.api.types.is_bool_dtype(s.dtype):
                out[c] = s.astype(object).where(s.notna(), None)
            elif pd.api.types.is_integer_dtype(s.dtype):
                out[c] = s.astype("float64") if s.isna().any() else s.astype("int64")
            else:
                out[c] = s.astype(object).where(s.notna(), None)
        elif s.dtype == object:
            out[c] = s.where(s.notna(), None)
    return out


# ---------------------------------------------------------------------------
# Coleta
# ---------------------------------------------------------------------------
def sha256(caminho: os.PathLike) -> str:
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(1 << 20), b""):
            h.update(bloco)
    return h.hexdigest()


def metadados_arquivo(caminho: Path, fonte: str) -> dict:
    # registros lidos pelo parser CSV (textos de avaliação têm quebras de linha dentro de aspas)
    df = pd.read_csv(caminho, dtype=str)
    return {
        "arquivo": caminho.name, "fonte": fonte, "linhas": len(df), "colunas": df.shape[1],
        "bytes": caminho.stat().st_size, "mb": round(caminho.stat().st_size / 2**20, 2),
        "sha256": sha256(caminho),
    }


def baixar_ibge_populacao(destino: Path) -> Path:
    """Estimativa de população residente por UF em 2018 (IBGE/SIDRA, tabela 6579)."""
    import urllib.request
    with urllib.request.urlopen(IBGE_URL, timeout=60) as r:
        dados = json.loads(r.read().decode("utf-8"))
    linhas = [{"codigo_ibge": int(d["D1C"]), "uf_nome": d["D1N"], "ano": int(d["D3N"]),
               "populacao": int(d["V"])} for d in dados[1:]]
    df = pd.DataFrame(linhas)
    destino.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destino, index=False, encoding="utf-8")
    return destino


def ler_bronze(dir_coleta: Path) -> dict[str, pd.DataFrame]:
    """Lê os arquivos da bronze como texto bruto (sem conversão), preservando zeros à esquerda."""
    b = {}
    for arq in ARQUIVOS_OLIST + [ARQUIVO_IBGE]:
        nome = (arq.replace("olist_", "").replace("_dataset.csv", "").replace(".csv", ""))
        b[nome] = pd.read_csv(dir_coleta / arq, dtype=str, keep_default_na=False, na_values=[""])
    return b


# ---------------------------------------------------------------------------
# Qualidade de dados
# ---------------------------------------------------------------------------
TIPO_ESPERADO = {  # tipo esperado de cada atributo da bronze, para medir conformidade
    "order_purchase_timestamp": "data", "order_approved_at": "data",
    "order_delivered_carrier_date": "data", "order_delivered_customer_date": "data",
    "order_estimated_delivery_date": "data", "shipping_limit_date": "data",
    "review_creation_date": "data", "review_answer_timestamp": "data",
    "order_item_id": "inteiro", "price": "numero", "freight_value": "numero",
    "product_name_lenght": "inteiro", "product_description_lenght": "inteiro",
    "product_photos_qty": "inteiro", "product_weight_g": "numero", "product_length_cm": "numero",
    "product_height_cm": "numero", "product_width_cm": "numero", "review_score": "inteiro",
    "payment_sequential": "inteiro", "payment_installments": "inteiro", "payment_value": "numero",
    "geolocation_lat": "numero", "geolocation_lng": "numero",
    "customer_zip_code_prefix": "cep", "seller_zip_code_prefix": "cep", "geolocation_zip_code_prefix": "cep",
    "codigo_ibge": "inteiro", "ano": "inteiro", "populacao": "inteiro",
}


def _conformidade(s: pd.Series, tipo: str) -> pd.Series:
    v = s.dropna()
    if tipo == "data":
        return pd.to_datetime(v, errors="coerce").notna()
    if tipo == "inteiro":
        return v.str.fullmatch(r"-?\d+")
    if tipo == "numero":
        return pd.to_numeric(v, errors="coerce").notna()
    if tipo == "cep":
        return v.str.fullmatch(r"\d{4,5}")
    if tipo == "hash":
        return v.str.fullmatch(r"[0-9a-f]{32}")
    return pd.Series(True, index=v.index)


def perfil_atributos(b: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Perfil de todos os atributos de todos os arquivos da bronze."""
    linhas = []
    for tabela, df in b.items():
        for col in df.columns:
            s = df[col]
            tipo = TIPO_ESPERADO.get(col, "hash" if col.endswith("_id") else "texto")
            conf = _conformidade(s, tipo)
            mn = mx = None
            if tipo in ("numero", "inteiro"):
                num = pd.to_numeric(s, errors="coerce")
                mn, mx = num.min(), num.max()
            elif tipo == "data":
                dt = pd.to_datetime(s, errors="coerce")
                mn, mx = dt.min(), dt.max()
            elif tipo != "hash":
                vc = s.dropna()
                if len(vc):
                    mn, mx = vc.min(), vc.max()
            top = s.value_counts().head(3)
            linhas.append({
                "tabela": tabela, "atributo": col, "tipo_esperado": tipo, "linhas": len(s),
                "nulos": int(s.isna().sum()), "pct_nulos": round(100 * s.isna().mean(), 2),
                "distintos": int(s.nunique()),
                "pct_conformidade_tipo": round(100 * conf.mean(), 2) if len(conf) else None,
                "minimo": None if tipo == "hash" else (str(mn) if mn is not None else None),
                "maximo": None if tipo == "hash" else (str(mx) if mx is not None else None),
                "mais_frequentes": "; ".join(f"{k} ({v})" for k, v in top.items()) if tipo != "hash" else None,
            })
    return pd.DataFrame(linhas)


def verificacoes_qualidade(b: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Verificações por dimensão de qualidade; cada uma aponta o tratamento adotado."""
    o, it, c, s = b["orders"], b["order_items"], b["customers"], b["sellers"]
    p, r, g, ibge = b["products"], b["order_reviews"], b["geolocation"], b["ibge_populacao_uf_2018"]
    num = lambda x: pd.to_numeric(x, errors="coerce")  # noqa: E731
    dt = lambda x: pd.to_datetime(x, errors="coerce")  # noqa: E731
    lat, lng = num(g.geolocation_lat), num(g.geolocation_lng)
    fora_br = ~(lat.between(*BR_LAT) & lng.between(*BR_LON))
    ceps_geo = set(g.geolocation_zip_code_prefix.str.zfill(5))
    dims = ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    compra = dt(o.order_purchase_timestamp)
    preco, frete = num(it.price), num(it.freight_value)
    rel = frete / preco
    rev_por_pedido = r.groupby("order_id").size()

    V = []

    def add(dim, tabela, verif, ocorr, total, trat):
        V.append({"dimensao": dim, "tabela": tabela, "verificacao": verif, "ocorrencias": int(ocorr),
                  "total": int(total), "pct": round(100 * ocorr / total, 3) if total else 0.0,
                  "tratamento": trat})

    # Completude
    add("Completude", "products", "produto sem categoria", p.product_category_name.isna().sum(), len(p),
        "R05: categoria 'sem_categoria'")
    add("Completude", "products", "produto sem peso ou sem alguma dimensão", p[dims].isna().any(axis=1).sum(),
        len(p), "R06: medidas nulas + flag; peso taxado usa o que existir")
    add("Completude", "orders", "pedido sem data de aprovação", o.order_approved_at.isna().sum(), len(o),
        "Sem impacto: a análise usa a data da compra")
    add("Completude", "orders", "pedido sem nenhum item", (~o.order_id.isin(it.order_id)).sum(), len(o),
        "R08: fora do escopo (sem frete a analisar)")
    add("Completude", "order_items", "item sem preço ou sem frete", (preco.isna() | frete.isna()).sum(), len(it),
        "Nenhum caso")
    add("Completude", "order_reviews", "pedido sem avaliação", (~o.order_id.isin(r.order_id)).sum(), len(o),
        "nota nula na fato (atributo opcional)")
    # Unicidade
    add("Unicidade", "orders", "order_id duplicado", o.order_id.duplicated().sum(), len(o), "Nenhum caso")
    add("Unicidade", "order_items", "par (order_id, order_item_id) duplicado",
        it.duplicated(["order_id", "order_item_id"]).sum(), len(it), "Nenhum caso")
    add("Unicidade", "products", "product_id duplicado", p.product_id.duplicated().sum(), len(p), "Nenhum caso")
    add("Unicidade", "sellers", "seller_id duplicado", s.seller_id.duplicated().sum(), len(s), "Nenhum caso")
    add("Unicidade", "order_reviews", "pedido com mais de uma avaliação", (rev_por_pedido > 1).sum(),
        r.order_id.nunique(), "R07: mantém a avaliação respondida por último")
    add("Unicidade", "geolocation", "linha de geolocalização repetida", g.duplicated().sum(), len(g),
        "R04: centroide (mediana) por prefixo de CEP")
    # Conformidade
    for col, df, t in [("customer_zip_code_prefix", c, "customers"), ("seller_zip_code_prefix", s, "sellers")]:
        add("Conformidade", t, f"{col} com menos de 5 dígitos (zero à esquerda perdido)",
            (df[col].str.len() < 5).sum(), len(df), "R02: completa com zeros à esquerda")
    add("Conformidade", "customers", "UF do cliente fora das 27 UFs", (~c.customer_state.isin(UFS)).sum(), len(c),
        "R03: padroniza; nenhum caso")
    add("Conformidade", "sellers", "UF do vendedor fora das 27 UFs", (~s.seller_state.isin(UFS)).sum(), len(s),
        "R03: padroniza; nenhum caso")
    add("Conformidade", "order_reviews", "nota fora de 1 a 5", (~num(r.review_score).between(1, 5)).sum(), len(r),
        "Nenhum caso")
    add("Conformidade", "orders", "status fora do domínio conhecido",
        (~o.order_status.isin(["delivered", "shipped", "canceled", "unavailable", "invoiced", "processing",
                               "created", "approved"])).sum(), len(o), "Nenhum caso")
    add("Conformidade", "sellers", "mesma cidade com grafias diferentes (acentos/caixa)",
        s.seller_city.str.lower().nunique() - s.seller_city.str.lower().map(_sem_acento).nunique(),
        s.seller_city.nunique(), "Não usado nas perguntas (análise por UF)")
    # Consistência
    add("Consistência", "orders", "aprovação antes da compra",
        (dt(o.order_approved_at) < compra).sum(), len(o), "Nenhum caso")
    add("Consistência", "orders", "pedido 'delivered' sem data de entrega",
        ((o.order_status == "delivered") & o.order_delivered_customer_date.isna()).sum(),
        (o.order_status == "delivered").sum(), "Sem impacto: o frete é cobrado na compra")
    add("Consistência", "order_items", "frete maior que o preço do item", (rel > 1).sum(), len(it),
        "Mantido: é o fenômeno estudado (sinalizado na faixa de frete relativo)")
    # Acurácia
    add("Acurácia", "geolocation", "coordenada fora do território brasileiro", fora_br.sum(), len(g),
        "R04: descartada antes do centroide")
    add("Acurácia", "products", "peso igual a zero", (num(p.product_weight_g) == 0).sum(), len(p),
        "R06: tratado como ausente")
    add("Acurácia", "order_items", "frete igual a zero (frete grátis)", (frete == 0).sum(), len(it),
        "Mantido + flag frete_gratis")
    add("Acurácia", "order_items", "preço menor ou igual a zero", (preco <= 0).sum(), len(it), "Nenhum caso")
    # Integridade
    add("Integridade", "order_items", "item com produto inexistente", (~it.product_id.isin(p.product_id)).sum(),
        len(it), "Nenhum caso")
    add("Integridade", "order_items", "item com vendedor inexistente", (~it.seller_id.isin(s.seller_id)).sum(),
        len(it), "Nenhum caso")
    add("Integridade", "orders", "pedido com cliente inexistente", (~o.customer_id.isin(c.customer_id)).sum(),
        len(o), "Nenhum caso")
    add("Integridade", "customers", "CEP do cliente sem geolocalização",
        (~c.customer_zip_code_prefix.str.zfill(5).isin(ceps_geo)).sum(), len(c), "R10: distância nula")
    add("Integridade", "sellers", "CEP do vendedor sem geolocalização",
        (~s.seller_zip_code_prefix.str.zfill(5).isin(ceps_geo)).sum(), len(s), "R10: distância nula")
    add("Integridade", "ibge", "UF da Olist sem população no IBGE",
        len(set(c.customer_state) - {k for k, v in UF_INFO.items() if str(v[0]) in set(ibge.codigo_ibge)}),
        c.customer_state.nunique(), "R12: junção pelo código IBGE da UF")
    # Atualidade
    mes = compra.dt.to_period("M")
    fora = (compra < JANELA_INICIO) | (compra >= JANELA_FIM)
    add("Atualidade", "orders", "compra fora da janela jan/2017–ago/2018 (meses incompletos)", fora.sum(), len(o),
        f"R08: excluídos ({mes[fora].astype(str).value_counts().sort_index().to_dict()})")
    return pd.DataFrame(V)


def _sem_acento(txt):
    import unicodedata
    if not isinstance(txt, str):
        return txt
    return "".join(ch for ch in unicodedata.normalize("NFKD", txt) if not unicodedata.combining(ch))


def volume_mensal(b) -> pd.DataFrame:
    o = b["orders"]
    m = pd.to_datetime(o.order_purchase_timestamp).dt.to_period("M").astype(str)
    df = (o.assign(ano_mes=m, cancelado=o.order_status.isin(STATUS_FORA))
          .groupby("ano_mes").agg(pedidos=("order_id", "size"), cancelados_ou_indisponiveis=("cancelado", "sum"))
          .reset_index())
    df["na_janela"] = (df.ano_mes >= "2017-01") & (df.ano_mes <= "2018-08")
    return df


# ---------------------------------------------------------------------------
# Transformação (bronze → silver) — regras R01 a R12
# ---------------------------------------------------------------------------
class LogLinhagem:
    def __init__(self):
        self.linhas = []

    def reg(self, regra, camada, tabela, descricao, afetadas, resultantes):
        self.linhas.append({"regra": regra, "camada_destino": camada, "tabela": tabela, "descricao": descricao,
                            "linhas_afetadas": int(afetadas), "linhas_resultantes": int(resultantes),
                            "executado_em": _dt.datetime.now().isoformat(timespec="seconds")})

    def df(self):
        return pd.DataFrame(self.linhas)


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = (np.radians(np.asarray(x, dtype="float64")) for x in (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0088 * np.arcsin(np.sqrt(a))


def transformar_silver(b: dict[str, pd.DataFrame], log: LogLinhagem) -> dict[str, pd.DataFrame]:
    S = {}
    # R01 — tipagem
    o = b["orders"].copy()
    cols_dt = [c for c in o.columns if "date" in c or "timestamp" in c or c.endswith("_at")]
    for col in cols_dt:
        o[col] = pd.to_datetime(o[col], errors="coerce")
    it = b["order_items"].copy()
    it["order_item_id"] = it.order_item_id.astype(int)
    it["price"] = it.price.astype(float)
    it["freight_value"] = it.freight_value.astype(float)
    it["shipping_limit_date"] = pd.to_datetime(it.shipping_limit_date)
    log.reg("R01", "silver", "pedidos/itens", "Tipagem: datas como timestamp, preço e frete como decimal (R$), "
            "sequência do item como inteiro", len(o) + len(it), len(o) + len(it))

    # R02 — CEP com 5 dígitos
    c = b["customers"].copy()
    s = b["sellers"].copy()
    n2 = (c.customer_zip_code_prefix.str.len() < 5).sum() + (s.seller_zip_code_prefix.str.len() < 5).sum()
    c["cep_prefixo"] = c.customer_zip_code_prefix.str.zfill(5)
    s["cep_prefixo"] = s.seller_zip_code_prefix.str.zfill(5)
    log.reg("R02", "silver", "clientes/vendedores", "Prefixo de CEP padronizado com 5 dígitos (zeros à esquerda)",
            n2, len(c) + len(s))

    # R03 — UF e cidade padronizadas
    c["uf"] = c.customer_state.str.strip().str.upper()
    s["uf"] = s.seller_state.str.strip().str.upper()
    c["cidade"] = c.customer_city.str.strip().str.lower().map(_sem_acento)
    s["cidade"] = s.seller_city.str.strip().str.lower().map(_sem_acento)
    log.reg("R03", "silver", "clientes/vendedores", "UF em maiúsculas e validada nas 27 UFs; cidade em minúsculas "
            "sem acento", (~c.uf.isin(UFS)).sum() + (~s.uf.isin(UFS)).sum(), len(c) + len(s))

    # R04 — geolocalização: remove coordenadas fora do Brasil e calcula centroide por CEP
    g = b["geolocation"].copy()
    g["cep_prefixo"] = g.geolocation_zip_code_prefix.str.zfill(5)
    g["lat"] = g.geolocation_lat.astype(float)
    g["lon"] = g.geolocation_lng.astype(float)
    ok = g.lat.between(*BR_LAT) & g.lon.between(*BR_LON)
    geo = (g[ok].groupby("cep_prefixo").agg(lat=("lat", "median"), lon=("lon", "median"),
                                             uf=("geolocation_state", lambda x: x.mode().iat[0]),
                                             pontos=("lat", "size")).reset_index())
    log.reg("R04", "silver", "geo_cep", "Descarta coordenadas fora do território brasileiro e resume a "
            "geolocalização em um centroide (mediana) por prefixo de CEP", (~ok).sum() + (len(g[ok]) - len(geo)),
            len(geo))
    S["geo_cep"] = geo

    # R05 — categoria
    p = b["products"].copy()
    n5 = p.product_category_name.isna().sum()
    p["categoria"] = p.product_category_name.fillna("sem_categoria")
    log.reg("R05", "silver", "produtos", "Categoria ausente recebe 'sem_categoria' (mantém o item na análise)",
            n5, len(p))

    # R06 — medidas, peso cubado e peso taxado
    for orig, novo in [("product_weight_g", "peso_g"), ("product_length_cm", "comprimento_cm"),
                       ("product_height_cm", "altura_cm"), ("product_width_cm", "largura_cm")]:
        p[novo] = pd.to_numeric(p[orig], errors="coerce")
        p.loc[p[novo] <= 0, novo] = np.nan
    p["volume_cm3"] = p.comprimento_cm * p.altura_cm * p.largura_cm
    p["peso_real_kg"] = p.peso_g / 1000
    p["peso_cubado_kg"] = p.volume_cm3 / FATOR_CUBAGEM
    p["peso_taxado_kg"] = p[["peso_real_kg", "peso_cubado_kg"]].max(axis=1, skipna=True)
    p["medidas_incompletas"] = p[["peso_g", "comprimento_cm", "altura_cm", "largura_cm"]].isna().any(axis=1)
    log.reg("R06", "silver", "produtos", f"Converte g→kg; peso cubado = C×A×L (cm³) ÷ {FATOR_CUBAGEM}; peso taxado "
            "= maior entre peso real e cubado; medidas nulas ou zero viram nulo + flag",
            p.medidas_incompletas.sum(), len(p))
    S["produtos"] = p[["product_id", "categoria", "peso_g", "comprimento_cm", "altura_cm", "largura_cm",
                       "volume_cm3", "peso_real_kg", "peso_cubado_kg", "peso_taxado_kg", "medidas_incompletas"]
                      ].rename(columns={"product_id": "produto_id"})

    # R07 — uma avaliação por pedido
    r = b["order_reviews"].copy()
    r["review_answer_timestamp"] = pd.to_datetime(r.review_answer_timestamp)
    r["nota"] = r.review_score.astype(int)
    r = r.sort_values(["order_id", "review_answer_timestamp"])
    r1 = r.drop_duplicates("order_id", keep="last")
    log.reg("R07", "silver", "avaliacoes", "Uma avaliação por pedido: mantém a respondida por último; texto livre "
            "descartado (ver R13)", len(r) - len(r1), len(r1))
    S["avaliacoes"] = r1[["order_id", "nota", "review_answer_timestamp"]].rename(
        columns={"order_id": "pedido_id", "review_answer_timestamp": "data_resposta"})

    # R08 — escopo: janela de compras e pedidos que geraram envio
    no_escopo = (o.order_purchase_timestamp >= JANELA_INICIO) & (o.order_purchase_timestamp < JANELA_FIM) & \
        (~o.order_status.isin(STATUS_FORA)) & o.order_id.isin(it.order_id)
    o8 = o[no_escopo]
    log.reg("R08", "silver", "pedidos", "Escopo: compras de jan/2017 a ago/2018, exceto pedidos cancelados ou "
            "indisponíveis (não geraram envio) e pedidos sem itens", (~no_escopo).sum(), len(o8))
    S["pedidos"] = o8[["order_id", "customer_id", "order_status", "order_purchase_timestamp",
                       "order_estimated_delivery_date", "order_delivered_customer_date"]].rename(columns={
        "order_id": "pedido_id", "customer_id": "cliente_id", "order_status": "status",
        "order_purchase_timestamp": "data_compra", "order_estimated_delivery_date": "data_prometida",
        "order_delivered_customer_date": "data_entrega"})
    it8 = it[it.order_id.isin(o8.order_id)]
    S["itens"] = it8[["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value",
                      "shipping_limit_date"]].rename(columns={
        "order_id": "pedido_id", "order_item_id": "item_seq", "product_id": "produto_id", "seller_id": "vendedor_id",
        "price": "preco", "freight_value": "frete", "shipping_limit_date": "data_limite_postagem"})

    # R13 (antecipada na silver) — minimização LGPD
    S["clientes"] = c[["customer_id", "cep_prefixo", "cidade", "uf"]].rename(columns={"customer_id": "cliente_id"})
    S["vendedores"] = s[["seller_id", "cep_prefixo", "cidade", "uf"]].rename(columns={"seller_id": "vendedor_id"})
    log.reg("R13", "silver", "clientes/avaliacoes", "Minimização (LGPD): customer_unique_id, título e texto das "
            "avaliações não seguem para silver e gold", len(c) + len(r), len(c) + len(r1))

    # R12 — IBGE: população 2018 por UF, chaveada pela sigla
    ib = b["ibge_populacao_uf_2018"].copy()
    ib["codigo_ibge"] = ib.codigo_ibge.astype(int)
    ib["populacao"] = ib.populacao.astype(int)
    cod2uf = {v[0]: k for k, v in UF_INFO.items()}
    ib["uf"] = ib.codigo_ibge.map(cod2uf)
    ib["regiao"] = ib.uf.map(lambda u: UF_INFO[u][2])
    log.reg("R12", "silver", "ibge_uf", "Conciliação Olist × IBGE: código IBGE da UF convertido em sigla (chave "
            "comum às duas bases); região atribuída pela sigla", ib.uf.isna().sum(), len(ib))
    S["ibge_uf"] = ib[["uf", "codigo_ibge", "uf_nome", "regiao", "ano", "populacao"]]
    return S


# ---------------------------------------------------------------------------
# Modelo estrela (silver → gold) — regras R09 a R11, R14 a R16
# ---------------------------------------------------------------------------
def faixa_frete_relativo(x):
    return pd.cut(x, [-np.inf, 0, 0.10, 0.20, 0.30, 0.50, 1.0, np.inf],
                  labels=["0 (grátis)", "até 10%", "10–20%", "20–30%", "30–50%", "50–100%", "acima de 100%"],
                  right=True).astype(str)


def faixa_distancia(x):
    return pd.cut(x, [-0.001, 100, 500, 1000, 2000, np.inf],
                  labels=["até 100 km", "100–500 km", "500–1.000 km", "1.000–2.000 km", "2.000 km ou mais"]
                  ).astype(str).replace("nan", None)


def construir_estrela(S: dict[str, pd.DataFrame], log: LogLinhagem) -> dict[str, pd.DataFrame]:
    G = {}
    ped, it, cli, ven = S["pedidos"], S["itens"], S["clientes"], S["vendedores"]
    prod, geo, av, ib = S["produtos"], S["geo_cep"], S["avaliacoes"], S["ibge_uf"]

    # dim_uf (papéis: origem e destino) — centroide da UF a partir dos CEPs
    cent = geo.groupby("uf").agg(lat_centroide=("lat", "median"), lon_centroide=("lon", "median")).reset_index()
    dim_uf = ib.merge(cent, on="uf", how="left").rename(columns={"populacao": "populacao_2018"})
    dim_uf = dim_uf[["uf", "uf_nome", "codigo_ibge", "regiao", "populacao_2018", "lat_centroide", "lon_centroide"]]
    G["dim_uf"] = dim_uf.sort_values("uf").reset_index(drop=True)

    # dim_tempo — calendário completo da janela
    datas = pd.date_range(JANELA_INICIO, JANELA_FIM - pd.Timedelta(days=1), freq="D")
    dias = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]
    meses = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
    G["dim_tempo"] = pd.DataFrame({
        "data_sk": datas.strftime("%Y%m%d").astype(int), "data": datas.date, "ano": datas.year,
        "mes": datas.month, "ano_mes": datas.strftime("%Y-%m"), "nome_mes": [meses[m - 1] for m in datas.month],
        "trimestre": datas.quarter, "dia_semana": [dias[d] for d in datas.dayofweek],
        "fim_de_semana": datas.dayofweek >= 5,
    })

    # dim_produto
    dp = prod[prod.produto_id.isin(it.produto_id)].sort_values("produto_id").reset_index(drop=True)
    dp.insert(0, "produto_sk", np.arange(1, len(dp) + 1))
    G["dim_produto"] = dp

    # dim_vendedor — R11: porte pelo número de itens vendidos na janela
    itens_v = it.groupby("vendedor_id").size().rename("itens_vendidos_janela")
    dv = ven[ven.vendedor_id.isin(it.vendedor_id)].merge(itens_v, on="vendedor_id")
    dv["porte"] = pd.cut(dv.itens_vendidos_janela, [0, 9, 49, 199, np.inf],
                         labels=["micro (1–9 itens)", "pequeno (10–49)", "médio (50–199)", "grande (200+)"]
                         ).astype(str)
    dv = dv.sort_values("vendedor_id").reset_index(drop=True)
    dv.insert(0, "vendedor_sk", np.arange(1, len(dv) + 1))
    G["dim_vendedor"] = dv[["vendedor_sk", "vendedor_id", "uf", "cidade", "cep_prefixo", "itens_vendidos_janela",
                            "porte"]]
    log.reg("R11", "gold", "dim_vendedor", "Porte do vendedor pelo nº de itens vendidos na janela: micro (1–9), "
            "pequeno (10–49), médio (50–199), grande (200+)", len(dv), len(dv))

    # fato_item_frete — grão: item vendido (pedido × sequência do item)
    f = it.merge(ped[["pedido_id", "cliente_id", "data_compra", "status"]], on="pedido_id") \
          .merge(cli.rename(columns={"cep_prefixo": "cep_destino", "uf": "uf_destino"})[
              ["cliente_id", "cep_destino", "uf_destino"]], on="cliente_id") \
          .merge(ven.rename(columns={"cep_prefixo": "cep_origem", "uf": "uf_origem"})[
              ["vendedor_id", "cep_origem", "uf_origem"]], on="vendedor_id") \
          .merge(av[["pedido_id", "nota"]], on="pedido_id", how="left")
    # R09 — frete relativo
    f["frete_relativo"] = f.frete / f.preco
    f["frete_gratis"] = f.frete == 0
    f["faixa_frete_relativo"] = faixa_frete_relativo(f.frete_relativo)
    log.reg("R09", "gold", "fato_item_frete", "Frete relativo = frete do item ÷ preço do item (R$/R$); frete zero "
            "sinalizado como frete grátis", (f.frete == 0).sum(), len(f))
    # R10 — distância entre centroides de CEP (linha reta)
    gi = geo.set_index("cep_prefixo")
    lo, la = f.cep_origem.map(gi.lat), f.cep_origem.map(gi.lon)
    ld, lod = f.cep_destino.map(gi.lat), f.cep_destino.map(gi.lon)
    f["distancia_km"] = haversine_km(lo, la, ld, lod)
    f["faixa_distancia"] = faixa_distancia(f.distancia_km)
    f["mesma_uf"] = f.uf_origem == f.uf_destino
    log.reg("R10", "gold", "fato_item_frete", "Distância vendedor → cliente pela fórmula de Haversine entre os "
            "centroides dos CEPs (linha reta, km); nula quando um dos CEPs não tem geolocalização",
            f.distancia_km.isna().sum(), len(f))
    # R14 — itens no pedido (frete rateado entre itens) e chaves
    f["itens_no_pedido"] = f.groupby("pedido_id").item_seq.transform("size")
    f["data_compra_sk"] = f.data_compra.dt.strftime("%Y%m%d").astype(int)
    f = f.merge(dp[["produto_id", "produto_sk"]], on="produto_id") \
         .merge(G["dim_vendedor"][["vendedor_id", "vendedor_sk"]], on="vendedor_id")
    f = f.sort_values(["data_compra", "pedido_id", "item_seq"]).reset_index(drop=True)
    f.insert(0, "item_sk", np.arange(1, len(f) + 1))
    log.reg("R14", "gold", "fato_item_frete", "Chaves substitutas (produto_sk, vendedor_sk, item_sk), chave de data "
            "AAAAMMDD e contagem de itens no pedido (a Olist rateia o frete do pedido entre os itens)", len(f), len(f))
    # R15 — grão
    log.reg("R15", "gold", "fato_item_frete", "Grão = item vendido (pedido × sequência): é o nível em que a Olist "
            "registra preço e frete", 0, len(f))
    G["fato_item_frete"] = f[[
        "item_sk", "pedido_id", "item_seq", "data_compra_sk", "produto_sk", "vendedor_sk", "uf_origem",
        "uf_destino", "status", "preco", "frete", "frete_relativo", "frete_gratis", "faixa_frete_relativo",
        "distancia_km", "faixa_distancia", "mesma_uf", "itens_no_pedido", "nota"]].rename(
        columns={"status": "status_pedido", "nota": "nota_avaliacao"})
    log.reg("R16", "gold", "todas", "Validação contra o Catálogo de Dados antes da carga (tipos, obrigatoriedade, "
            "domínios); carga idempotente (DROP + CREATE)", 0, sum(len(v) for v in G.values()))
    return G


# ---------------------------------------------------------------------------
# Catálogo de Dados
# ---------------------------------------------------------------------------
_LIN_IT = "Kaggle olist_order_items_dataset.csv"
CATALOGO_GOLD: dict[str, dict] = {
    "dim_uf": {
        "descricao": "Unidades da Federação com região e população estimada 2018 (IBGE). Dimensão com dois papéis "
                     "na fato: UF de origem (vendedor) e UF de destino (cliente).",
        "pk": "uf",
        "colunas": [
            ("uf", "string", "Sigla da UF", {"valores": UFS}, True, None, "IBGE (código → sigla, R12)"),
            ("uf_nome", "string", "Nome da UF", None, True, None, "IBGE SIDRA 6579, campo D1N"),
            ("codigo_ibge", "int", "Código IBGE da UF", {"min": 11, "max": 53}, True, None, "IBGE SIDRA 6579, D1C"),
            ("regiao", "string", "Grande região", {"valores": REGIOES}, True, None, "Atribuída pela sigla (R12)"),
            ("populacao_2018", "bigint", "População residente estimada em 2018 (pessoas)",
             {"min": 400_000, "max": 50_000_000}, True, None, "IBGE SIDRA 6579, variável 9324, ano 2018"),
            ("lat_centroide", "double", "Latitude mediana dos CEPs da UF (graus)", {"min": -34, "max": 5.3}, False,
             "UF sem CEP geolocalizado", "Kaggle olist_geolocation (R04)"),
            ("lon_centroide", "double", "Longitude mediana dos CEPs da UF (graus)", {"min": -74, "max": -34.7},
             False, "UF sem CEP geolocalizado", "Kaggle olist_geolocation (R04)"),
        ]},
    "dim_tempo": {
        "descricao": "Calendário diário da janela de análise (jan/2017 a ago/2018).",
        "pk": "data_sk",
        "colunas": [
            ("data_sk", "int", "Chave da data no formato AAAAMMDD", {"min": 20170101, "max": 20180831}, True, None,
             "Gerada (R14)"),
            ("data", "date", "Data", {"min": "2017-01-01", "max": "2018-08-31"}, True, None, "Gerada"),
            ("ano", "int", "Ano", {"min": 2017, "max": 2018}, True, None, "Derivada de data"),
            ("mes", "int", "Mês (1–12)", {"min": 1, "max": 12}, True, None, "Derivada de data"),
            ("ano_mes", "string", "Ano e mês (AAAA-MM)", {"padrao": r"^20(17|18)-\d{2}$"}, True, None,
             "Derivada de data"),
            ("nome_mes", "string", "Mês abreviado", {"valores": ["jan", "fev", "mar", "abr", "mai", "jun", "jul",
                                                                  "ago", "set", "out", "nov", "dez"]}, True, None,
             "Derivada de data"),
            ("trimestre", "int", "Trimestre (1–4)", {"min": 1, "max": 4}, True, None, "Derivada de data"),
            ("dia_semana", "string", "Dia da semana", {"valores": ["segunda", "terça", "quarta", "quinta", "sexta",
                                                                    "sábado", "domingo"]}, True, None,
             "Derivada de data"),
            ("fim_de_semana", "boolean", "Sábado ou domingo", None, True, None, "Derivada de data"),
        ]},
    "dim_produto": {
        "descricao": "Produtos vendidos na janela, com categoria e medidas usadas no cálculo do frete.",
        "pk": "produto_sk",
        "colunas": [
            ("produto_sk", "int", "Chave substituta do produto", {"min": 1}, True, None, "Gerada (R14)"),
            ("produto_id", "string", "Identificador do produto na Olist (hash)", {"padrao": r"^[0-9a-f]{32}$"},
             True, None, "Kaggle olist_products_dataset.csv"),
            ("categoria", "string", "Categoria do produto (nome original em português)", {"padrao": r"^[a-z_0-9]+$"},
             True, None, "products.product_category_name; nulo → 'sem_categoria' (R05)"),
            ("peso_g", "double", "Peso informado (g)", {"min": 1, "max": 50_000}, False,
             "Peso não informado ou zero", "products.product_weight_g (R06)"),
            ("comprimento_cm", "double", "Comprimento da embalagem (cm)", {"min": 1, "max": 120}, False,
             "Medida não informada", "products.product_length_cm (R06)"),
            ("altura_cm", "double", "Altura da embalagem (cm)", {"min": 1, "max": 120}, False,
             "Medida não informada", "products.product_height_cm (R06)"),
            ("largura_cm", "double", "Largura da embalagem (cm)", {"min": 1, "max": 120}, False,
             "Medida não informada", "products.product_width_cm (R06)"),
            ("volume_cm3", "double", "Volume da embalagem (cm³)", {"min": 1, "max": 1_728_000}, False,
             "Alguma medida ausente", "C × A × L (R06)"),
            ("peso_real_kg", "double", "Peso real (kg)", {"min": 0.001, "max": 50}, False, "Peso ausente",
             "peso_g ÷ 1000 (R06)"),
            ("peso_cubado_kg", "double", f"Peso cubado (kg) = volume ÷ {FATOR_CUBAGEM}", {"min": 0, "max": 300},
             False, "Alguma medida ausente", "volume_cm3 ÷ 6000 (R06)"),
            ("peso_taxado_kg", "double", "Peso usado para cobrança: maior entre real e cubado (kg)",
             {"min": 0, "max": 300}, False, "Peso e medidas ausentes", "max(peso_real_kg, peso_cubado_kg) (R06)"),
            ("medidas_incompletas", "boolean", "Peso ou alguma dimensão ausente", None, True, None, "Flag (R06)"),
        ]},
    "dim_vendedor": {
        "descricao": "Vendedores com vendas na janela, localização de origem e porte.",
        "pk": "vendedor_sk",
        "colunas": [
            ("vendedor_sk", "int", "Chave substituta do vendedor", {"min": 1}, True, None, "Gerada (R14)"),
            ("vendedor_id", "string", "Identificador do vendedor na Olist (hash)", {"padrao": r"^[0-9a-f]{32}$"},
             True, None, "Kaggle olist_sellers_dataset.csv"),
            ("uf", "string", "UF do vendedor", {"valores": UFS}, True, None, "sellers.seller_state (R03)"),
            ("cidade", "string", "Cidade do vendedor (minúsculas, sem acento)", None, True, None,
             "sellers.seller_city (R03)"),
            ("cep_prefixo", "string", "Prefixo de CEP (5 dígitos)", {"padrao": r"^\d{5}$"}, True, None,
             "sellers.seller_zip_code_prefix (R02)"),
            ("itens_vendidos_janela", "int", "Itens vendidos na janela", {"min": 1, "max": 5000}, True, None,
             "Contagem em order_items (R11)"),
            ("porte", "string", "Porte pelo volume de itens", {"valores": ["micro (1–9 itens)", "pequeno (10–49)",
                                                                           "médio (50–199)", "grande (200+)"]},
             True, None, "Faixas de itens_vendidos_janela (R11)"),
        ]},
    "fato_item_frete": {
        "descricao": "Fato do frete. Grão: item vendido (pedido × sequência do item), compras de jan/2017 a ago/2018 "
                     "que geraram envio.",
        "pk": "item_sk",
        "fks": [("data_compra_sk", "dim_tempo", "data_sk"), ("produto_sk", "dim_produto", "produto_sk"),
                ("vendedor_sk", "dim_vendedor", "vendedor_sk"), ("uf_origem", "dim_uf", "uf"),
                ("uf_destino", "dim_uf", "uf")],
        "colunas": [
            ("item_sk", "int", "Chave substituta do item", {"min": 1}, True, None, "Gerada (R14)"),
            ("pedido_id", "string", "Identificador do pedido (hash)", {"padrao": r"^[0-9a-f]{32}$"}, True, None,
             _LIN_IT),
            ("item_seq", "int", "Sequência do item dentro do pedido", {"min": 1, "max": 25}, True, None,
             "order_items.order_item_id"),
            ("data_compra_sk", "int", "Data da compra (FK dim_tempo)", {"min": 20170101, "max": 20180831}, True,
             None, "orders.order_purchase_timestamp (R14)"),
            ("produto_sk", "int", "Produto (FK dim_produto)", {"min": 1}, True, None, "R14"),
            ("vendedor_sk", "int", "Vendedor (FK dim_vendedor)", {"min": 1}, True, None, "R14"),
            ("uf_origem", "string", "UF do vendedor — papel origem (FK dim_uf)", {"valores": UFS}, True, None,
             "sellers.seller_state (R03)"),
            ("uf_destino", "string", "UF do cliente — papel destino (FK dim_uf)", {"valores": UFS}, True, None,
             "customers.customer_state (R03)"),
            ("status_pedido", "string", "Status do pedido na extração",
             {"valores": ["delivered", "shipped", "invoiced", "processing", "created", "approved"]}, True, None,
             "orders.order_status (R08)"),
            ("preco", "double", "Preço do item (R$)", {"min": 0.01, "max": 10_000}, True, None, "order_items.price"),
            ("frete", "double", "Frete do item (R$); a Olist rateia o frete do pedido entre os itens",
             {"min": 0, "max": 1_000}, True, None, "order_items.freight_value"),
            ("frete_relativo", "double", "Frete ÷ preço do item (0,30 = frete equivale a 30% do preço)",
             {"min": 0, "max": 100}, True, None, "frete ÷ preco (R09)"),
            ("frete_gratis", "boolean", "Frete igual a zero", None, True, None, "frete = 0 (R09)"),
            ("faixa_frete_relativo", "string", "Faixa do frete relativo",
             {"valores": ["0 (grátis)", "até 10%", "10–20%", "20–30%", "30–50%", "50–100%", "acima de 100%"]},
             True, None, "Faixas de frete_relativo (R09)"),
            ("distancia_km", "double", "Distância em linha reta vendedor → cliente (km)", {"min": 0, "max": 4_500},
             False, "CEP de origem ou destino sem geolocalização", "Haversine entre centroides de CEP (R10)"),
            ("faixa_distancia", "string", "Faixa de distância",
             {"valores": ["até 100 km", "100–500 km", "500–1.000 km", "1.000–2.000 km", "2.000 km ou mais"]},
             False, "Distância nula", "Faixas de distancia_km (R10)"),
            ("mesma_uf", "boolean", "Vendedor e cliente na mesma UF", None, True, None, "uf_origem = uf_destino"),
            ("itens_no_pedido", "int", "Quantidade de itens no pedido", {"min": 1, "max": 25}, True, None,
             "Contagem por pedido (R14)"),
            ("nota_avaliacao", "int", "Nota da avaliação do pedido (1–5)", {"min": 1, "max": 5}, False,
             "Pedido sem avaliação", "order_reviews.review_score (R07)"),
        ]},
}
ORDEM_CARGA = ["dim_uf", "dim_tempo", "dim_produto", "dim_vendedor", "fato_item_frete"]


def colunas_catalogo(tabela: str) -> list[dict]:
    chaves = ["coluna", "tipo", "descricao", "dominio", "obrigatorio", "nulo_significa", "linhagem"]
    return [dict(zip(chaves, c)) for c in CATALOGO_GOLD[tabela]["colunas"]]


def _dominio_txt(d):
    if not d:
        return "—"
    if "valores" in d:
        return ", ".join(map(str, d["valores"]))
    if "padrao" in d:
        return f"padrão `{d['padrao']}`"
    lo, hi = d.get("min"), d.get("max")
    return f"{lo} a {hi}" if hi is not None else f"≥ {lo}"


def catalogo_df() -> pd.DataFrame:
    linhas = []
    for t, info in CATALOGO_GOLD.items():
        for c in colunas_catalogo(t):
            linhas.append({"tabela": t, "atributo": c["coluna"], "tipo": c["tipo"], "descricao": c["descricao"],
                           "dominio": _dominio_txt(c["dominio"]),
                           "obrigatorio": "sim" if c["obrigatorio"] else "não",
                           "significado_do_nulo": c["nulo_significa"] or "—", "linhagem": c["linhagem"]})
    return pd.DataFrame(linhas)


def catalogo_markdown() -> str:
    out = ["# Catálogo de Dados — camada gold (`workspace.frete_gold`)", "",
           "Gerado automaticamente a partir de `scripts/mvp_frete.py` (`CATALOGO_GOLD`). A mesma definição gera "
           "o esquema das tabelas Delta, os comentários de tabela e coluna, as restrições (NOT NULL, CHECK, "
           "PRIMARY KEY, FOREIGN KEY) e a validação automática dos dados antes da carga.", ""]
    for t, info in CATALOGO_GOLD.items():
        out += [f"## `{t}`", "", info["descricao"], "", f"Chave primária: `{info['pk']}`"]
        if info.get("fks"):
            out.append("Chaves estrangeiras: " + "; ".join(f"`{c}` → `{rt}.{rc}`" for c, rt, rc in info["fks"]))
        out += ["", "| Atributo | Tipo | Descrição | Domínio | Obrig. | Nulo significa | Linhagem |",
                "|---|---|---|---|---|---|---|"]
        for c in colunas_catalogo(t):
            out.append(f"| `{c['coluna']}` | {c['tipo']} | {c['descricao']} | {_dominio_txt(c['dominio'])} | "
                       f"{'sim' if c['obrigatorio'] else 'não'} | {c['nulo_significa'] or '—'} | {c['linhagem']} |")
        out.append("")
    return "\n".join(out)


def validar_catalogo(G: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Confere cada atributo da gold contra o catálogo (antes da carga)."""
    linhas = []
    for t in ORDEM_CARGA:
        df = G[t]
        faltando = set(c["coluna"] for c in colunas_catalogo(t)) - set(df.columns)
        sobrando = set(df.columns) - set(c["coluna"] for c in colunas_catalogo(t))
        if faltando or sobrando:
            raise ValueError(f"{t}: colunas fora do catálogo — faltando {faltando}, sobrando {sobrando}")
        for c in colunas_catalogo(t):
            s = df[c["coluna"]]
            nulos = int(s.isna().sum())
            d = c["dominio"] or {}
            fora = 0
            v = s.dropna()
            if "valores" in d:
                fora = int((~v.astype(str).isin([str(x) for x in d["valores"]])).sum())
            elif "padrao" in d:
                fora = int((~v.astype(str).str.match(d["padrao"])).sum())
            elif "min" in d or "max" in d:
                vv = pd.to_datetime(v) if c["tipo"] == "date" else pd.to_numeric(v)
                lo = pd.Timestamp(d["min"]) if c["tipo"] == "date" else d.get("min")
                hi = (pd.Timestamp(d["max"]) if c["tipo"] == "date" else d.get("max"))
                if lo is not None:
                    fora += int((vv < lo).sum())
                if hi is not None:
                    fora += int((vv > hi).sum())
            ok = fora == 0 and not (c["obrigatorio"] and nulos)
            linhas.append({"tabela": t, "atributo": c["coluna"], "linhas": len(s), "nulos": nulos,
                           "obrigatorio": c["obrigatorio"], "fora_do_dominio": fora,
                           "minimo": str(v.min()) if len(v) and c["tipo"] not in ("boolean",) else None,
                           "maximo": str(v.max()) if len(v) and c["tipo"] not in ("boolean",) else None,
                           "resultado": "OK" if ok else "FALHA"})
    return pd.DataFrame(linhas)


def ddl_restricoes(plat: Plataforma, tabela: str) -> list[str]:
    """Comandos de comentários e restrições derivados do catálogo (Databricks/Delta)."""
    nome = plat.nome_tabela("gold", tabela)
    info = CATALOGO_GOLD[tabela]
    cmds = [f"COMMENT ON TABLE {nome} IS '{_esc(info['descricao'])}'"]
    for c in colunas_catalogo(tabela):
        col = c["coluna"]
        cmds.append(f"ALTER TABLE {nome} ALTER COLUMN `{col}` COMMENT '{_esc(c['descricao'])}'")
        if c["obrigatorio"]:
            cmds.append(f"ALTER TABLE {nome} ALTER COLUMN `{col}` SET NOT NULL")
        d = c["dominio"] or {}
        expr = None
        if "valores" in d:
            expr = f"`{col}` IN ({', '.join(repr(str(x)) if c['tipo'] == 'string' else str(x) for x in d['valores'])})"
        elif ("min" in d or "max" in d) and c["tipo"] != "date":
            partes = []
            if d.get("min") is not None:
                partes.append(f"`{col}` >= {d['min']}")
            if d.get("max") is not None:
                partes.append(f"`{col}` <= {d['max']}")
            expr = " AND ".join(partes)
        if expr:
            ck = f"ck_{tabela}_{col}"[:120]
            cmds.append(f"ALTER TABLE {nome} DROP CONSTRAINT IF EXISTS {ck}")
            cmds.append(f"ALTER TABLE {nome} ADD CONSTRAINT {ck} CHECK ({expr})")
    cmds.append(f"ALTER TABLE {nome} ADD CONSTRAINT pk_{tabela} PRIMARY KEY (`{info['pk']}`)")
    return cmds


def ddl_chaves_estrangeiras(plat: Plataforma) -> list[str]:
    cmds = []
    for col, ref_t, ref_c in CATALOGO_GOLD["fato_item_frete"]["fks"]:
        cmds.append(f"ALTER TABLE {plat.nome_tabela('gold', 'fato_item_frete')} ADD CONSTRAINT "
                    f"fk_fato_{col} FOREIGN KEY (`{col}`) REFERENCES {plat.nome_tabela('gold', ref_t)} (`{ref_c}`)")
    return cmds


def ddl_script_completo() -> str:
    """DDL completo do modelo estrela (para scripts/ddl_modelo_estrela.sql)."""
    out = ["-- Modelo estrela do MVP de frete — gerado a partir do Catálogo de Dados (scripts/mvp_frete.py)",
           f"-- Destino: Databricks, catálogo {CATALOGO}, schema {SCHEMAS['gold']} (tabelas Delta)", "",
           f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMAS['gold']};", ""]
    for t in ORDEM_CARGA:
        info = CATALOGO_GOLD[t]
        cols = []
        for c in colunas_catalogo(t):
            nn = " NOT NULL" if c["obrigatorio"] else ""
            cols.append(f"  {c['coluna']} {TIPOS_SPARK[c['tipo']]}{nn} COMMENT '{_esc(c['descricao'])}'")
        cols.append(f"  CONSTRAINT pk_{t} PRIMARY KEY ({info['pk']})")
        for col, rt, rc in info.get("fks", []):
            cols.append(f"  CONSTRAINT fk_fato_{col} FOREIGN KEY ({col}) REFERENCES "
                        f"{CATALOGO}.{SCHEMAS['gold']}.{rt} ({rc})")
        out.append(f"CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMAS['gold']}.{t} (")
        out.append(",\n".join(cols))
        out.append(f") USING DELTA COMMENT '{_esc(info['descricao'])}';\n")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Utilidades de evidência
# ---------------------------------------------------------------------------
def salvar_tabela_evidencia(df: pd.DataFrame, plat: Plataforma, nome: str) -> Path:
    destino = plat.dir_evidencias / "tabelas" / f"{nome}.csv"
    destino.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destino, index=False, encoding="utf-8")
    return destino


def salvar_sql(consulta: str, nome: str) -> Path:
    destino = RAIZ_REPO / "scripts" / "sql" / f"{nome}.sql"
    try:
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_text(consulta.strip() + "\n", encoding="utf-8")
    except OSError:
        pass
    return destino


def salvar_figura(fig, plat: Plataforma, nome: str) -> Path:
    destino = plat.dir_evidencias / "graficos" / f"{nome}.png"
    destino.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(destino, dpi=150, bbox_inches="tight")
    return destino


def fmt_pct(x, casas=1):
    return f"{100 * x:.{casas}f}%".replace(".", ",")


def fmt_num(x, casas=0):
    s = f"{x:,.{casas}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


if __name__ == "__main__":
    print(catalogo_markdown()[:500])
