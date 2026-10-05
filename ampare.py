"""
Ampere — API de ingestão e análise de vendas.

Tudo em um arquivo: validação, banco, consultas, API e painel.

    pip install -r requirements.txt
    python ampere.py dados      # gera data/vendas_exemplo.csv
    python ampere.py            # sobe em http://localhost:8000

Autor: Diogo Dutra da Silva — MIT.
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import random
import re
import sqlite3
import sys
import unicodedata
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

RAIZ = Path(__file__).resolve().parent
VERSAO = "1.0.0"


def caminho_banco() -> Path:
    return Path(os.getenv("AMPERE_DB_PATH", RAIZ / "data" / "ampere.db"))


def limite_upload() -> int:
    return int(os.getenv("AMPERE_LIMITE_UPLOAD_MB", "10")) * 1024 * 1024


# =========================================================================== #
# 1. VALIDAÇÃO — o que é uma venda válida
# =========================================================================== #

UFS = {
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
    "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC",
    "SE", "SP", "TO",
}

FORMATOS_DATA = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d")


def normalizar_numero(valor: str) -> str:
    """Aceita 1.234,56 (padrão BR) e 1234.56 (padrão US)."""
    texto = re.sub(r"[^\d,.\-]", "", valor.strip())
    if "," in texto and "." in texto:
        if texto.rfind(",") > texto.rfind("."):
            texto = texto.replace(".", "").replace(",", ".")
        else:
            texto = texto.replace(",", "")
    elif "," in texto:
        texto = texto.replace(",", ".")
    return texto


class VendaIn(BaseModel):
    """Uma linha do arquivo de vendas, já validada."""

    data: date
    cliente: str = Field(min_length=2, max_length=120)
    produto: str = Field(min_length=2, max_length=120)
    categoria: str = Field(min_length=2, max_length=60)
    quantidade: int = Field(gt=0, le=100_000)
    preco_unitario: float = Field(gt=0, le=1_000_000)
    uf: str

    @field_validator("data", mode="before")
    @classmethod
    def aceitar_formatos_br(cls, valor: object) -> object:
        if isinstance(valor, str):
            for formato in FORMATOS_DATA:
                try:
                    return datetime.strptime(valor.strip(), formato).date()
                except ValueError:
                    continue
            raise ValueError(f"data '{valor}' não está em um formato reconhecido")
        return valor

    @field_validator("cliente", "produto", "categoria", mode="before")
    @classmethod
    def limpar_texto(cls, valor: object) -> object:
        return re.sub(r"\s+", " ", valor).strip() if isinstance(valor, str) else valor

    @field_validator("quantidade", mode="before")
    @classmethod
    def limpar_quantidade(cls, valor: object) -> object:
        if isinstance(valor, str):
            texto = normalizar_numero(valor)
            if not texto:
                raise ValueError("quantidade vazia")
            return int(float(texto))
        return valor

    @field_validator("preco_unitario", mode="before")
    @classmethod
    def limpar_preco(cls, valor: object) -> object:
        if isinstance(valor, str):
            texto = normalizar_numero(valor)
            if not texto:
                raise ValueError("preço vazio")
            return float(texto)
        return valor

    @field_validator("uf", mode="before")
    @classmethod
    def validar_uf(cls, valor: object) -> object:
        if isinstance(valor, str):
            sigla = valor.strip().upper()
            if sigla not in UFS:
                raise ValueError(f"UF '{valor}' não existe")
            return sigla
        return valor

    @model_validator(mode="after")
    def data_nao_pode_ser_futura(self) -> "VendaIn":
        if self.data > date.today():
            raise ValueError("data da venda está no futuro")
        return self

    @property
    def total(self) -> float:
        return round(self.quantidade * self.preco_unitario, 2)

    @property
    def fingerprint(self) -> str:
        """Identidade da venda: reimportar o mesmo arquivo não duplica nada."""
        bruto = "|".join([
            self.data.isoformat(),
            self.cliente.casefold(),
            self.produto.casefold(),
            str(self.quantidade),
            f"{self.preco_unitario:.2f}",
            self.uf,
        ])
        return hashlib.sha256(bruto.encode("utf-8")).hexdigest()


class LinhaRejeitada(BaseModel):
    linha: int
    erros: list[str]
    conteudo: dict[str, str]


class ResultadoIngestao(BaseModel):
    arquivo: str
    linhas_lidas: int
    aceitas: int
    duplicadas: int
    rejeitadas: list[LinhaRejeitada]

    @property
    def taxa_aproveitamento(self) -> float:
        if self.linhas_lidas == 0:
            return 0.0
        return round(self.aceitas / self.linhas_lidas * 100, 1)


# =========================================================================== #
# 2. BANCO — SQLite puro, sem ORM
# =========================================================================== #

SCHEMA = """
CREATE TABLE IF NOT EXISTS vendas (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint     TEXT    NOT NULL UNIQUE,
    data            TEXT    NOT NULL,
    cliente         TEXT    NOT NULL,
    produto         TEXT    NOT NULL,
    categoria       TEXT    NOT NULL,
    quantidade      INTEGER NOT NULL CHECK (quantidade > 0),
    preco_unitario  REAL    NOT NULL CHECK (preco_unitario > 0),
    total           REAL    NOT NULL,
    uf              TEXT    NOT NULL,
    criado_em       TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_vendas_data    ON vendas (data);
CREATE INDEX IF NOT EXISTS idx_vendas_produto ON vendas (produto);
CREATE INDEX IF NOT EXISTS idx_vendas_uf      ON vendas (uf);
CREATE INDEX IF NOT EXISTS idx_vendas_cat     ON vendas (categoria);
"""


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    destino = caminho_banco()
    destino.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(destino, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def criar_schema() -> None:
    with get_conn() as conn:
        conn.executescript(SCHEMA)


# =========================================================================== #
# 3. CONSULTAS — toda agregação roda no banco
# =========================================================================== #

CAMPOS_ORDENACAO = {"data", "total", "quantidade", "cliente", "produto"}


def inserir_vendas(vendas: list[VendaIn]) -> int:
    """Insere ignorando fingerprints já conhecidos. Retorna quantas entraram."""
    if not vendas:
        return 0
    linhas = [
        (v.fingerprint, v.data.isoformat(), v.cliente, v.produto, v.categoria,
         v.quantidade, v.preco_unitario, v.total, v.uf)
        for v in vendas
    ]
    with get_conn() as conn:
        antes = conn.execute("SELECT COUNT(*) AS n FROM vendas").fetchone()["n"]
        conn.executemany(
            """INSERT OR IGNORE INTO vendas
                   (fingerprint, data, cliente, produto, categoria,
                    quantidade, preco_unitario, total, uf)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            linhas,
        )
        depois = conn.execute("SELECT COUNT(*) AS n FROM vendas").fetchone()["n"]
    return depois - antes


def _filtros(f: dict[str, Any]) -> tuple[str, list[Any]]:
    clausulas: list[str] = []
    params: list[Any] = []
    if f.get("inicio"):
        clausulas.append("data >= ?")
        params.append(f["inicio"])
    if f.get("fim"):
        clausulas.append("data <= ?")
        params.append(f["fim"])
    if f.get("produto"):
        clausulas.append("produto LIKE ?")
        params.append(f"%{f['produto']}%")
    if f.get("categoria"):
        clausulas.append("categoria = ?")
        params.append(f["categoria"])
    if f.get("uf"):
        clausulas.append("uf = ?")
        params.append(str(f["uf"]).upper())
    where = f"WHERE {' AND '.join(clausulas)}" if clausulas else ""
    return where, params


def listar_vendas(ordenar_por="data", ordem="desc", limite=50, offset=0, **f) -> dict:
    if ordenar_por not in CAMPOS_ORDENACAO:
        ordenar_por = "data"
    direcao = "ASC" if str(ordem).lower() == "asc" else "DESC"
    where, params = _filtros(f)
    with get_conn() as conn:
        total = conn.execute(f"SELECT COUNT(*) AS n FROM vendas {where}", params).fetchone()["n"]
        linhas = conn.execute(
            f"""SELECT id, data, cliente, produto, categoria,
                       quantidade, preco_unitario, total, uf
                FROM vendas {where}
                ORDER BY {ordenar_por} {direcao}, id DESC
                LIMIT ? OFFSET ?""",
            [*params, limite, offset],
        ).fetchall()
    return {"total": total, "limite": limite, "offset": offset,
            "itens": [dict(linha) for linha in linhas]}


def resumo(**f) -> dict:
    where, params = _filtros(f)
    with get_conn() as conn:
        linha = conn.execute(
            f"""SELECT COUNT(*)                     AS pedidos,
                       COALESCE(SUM(total), 0)      AS faturamento,
                       COALESCE(SUM(quantidade), 0) AS itens,
                       COALESCE(AVG(total), 0)      AS ticket_medio,
                       COUNT(DISTINCT cliente)      AS clientes,
                       MIN(data)                    AS primeira_venda,
                       MAX(data)                    AS ultima_venda
                FROM vendas {where}""",
            params,
        ).fetchone()
    dados = dict(linha)
    dados["faturamento"] = round(dados["faturamento"], 2)
    dados["ticket_medio"] = round(dados["ticket_medio"], 2)
    return dados


def por_produto(limite: int = 10, **f) -> list[dict]:
    where, params = _filtros(f)
    with get_conn() as conn:
        linhas = conn.execute(
            f"""SELECT produto, categoria,
                       SUM(quantidade)      AS itens,
                       ROUND(SUM(total), 2) AS faturamento,
                       COUNT(*)             AS pedidos
                FROM vendas {where}
                GROUP BY produto, categoria
                ORDER BY faturamento DESC
                LIMIT ?""",
            [*params, limite],
        ).fetchall()
    return [dict(linha) for linha in linhas]


def por_uf(**f) -> list[dict]:
    where, params = _filtros(f)
    with get_conn() as conn:
        linhas = conn.execute(
            f"""SELECT uf,
                       ROUND(SUM(total), 2) AS faturamento,
                       COUNT(*)             AS pedidos
                FROM vendas {where}
                GROUP BY uf
                ORDER BY faturamento DESC""",
            params,
        ).fetchall()
    return [dict(linha) for linha in linhas]


def serie_temporal(granularidade: str = "mes", **f) -> list[dict]:
    corte = 10 if granularidade == "dia" else 7
    where, params = _filtros(f)
    with get_conn() as conn:
        linhas = conn.execute(
            f"""SELECT substr(data, 1, {corte}) AS periodo,
                       ROUND(SUM(total), 2)     AS faturamento,
                       COUNT(*)                 AS pedidos
                FROM vendas {where}
                GROUP BY periodo
                ORDER BY periodo""",
            params,
        ).fetchall()
    return [dict(linha) for linha in linhas]


def crescimento_mensal(**f) -> list[dict]:
    """Série mensal com variação percentual contra o mês anterior."""
    resultado: list[dict] = []
    anterior: float | None = None
    for ponto in serie_temporal("mes", **f):
        variacao = None
        if anterior:
            variacao = round((ponto["faturamento"] - anterior) / anterior * 100, 1)
        resultado.append({**ponto, "variacao_pct": variacao})
        anterior = ponto["faturamento"]
    return resultado


def limpar_tudo() -> int:
    with get_conn() as conn:
        n = conn.execute("SELECT COUNT(*) AS n FROM vendas").fetchone()["n"]
        conn.execute("DELETE FROM vendas")
    return n


# =========================================================================== #
# 4. LEITURA DO CSV — encoding, separador e apelidos de coluna
# =========================================================================== #

COLUNAS_OBRIGATORIAS = {
    "data", "cliente", "produto", "categoria", "quantidade", "preco_unitario", "uf",
}

APELIDOS = {
    "dt": "data", "data_venda": "data", "emissao": "data",
    "nome_cliente": "cliente", "razao_social": "cliente",
    "item": "produto", "descricao": "produto",
    "linha": "categoria", "familia": "categoria",
    "qtd": "quantidade", "qtde": "quantidade", "quantidade_vendida": "quantidade",
    "preco": "preco_unitario", "valor_unitario": "preco_unitario",
    "vl_unit": "preco_unitario",
    "estado": "uf",
}


class ArquivoInvalido(Exception):
    """O arquivo não tem as colunas necessárias ou está ilegível."""


def _slug(texto: str) -> str:
    sem_acento = unicodedata.normalize("NFKD", texto)
    sem_acento = "".join(c for c in sem_acento if not unicodedata.combining(c))
    return sem_acento.strip().lower().replace(" ", "_").replace("-", "_")


def decodificar(conteudo: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return conteudo.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ArquivoInvalido("não foi possível decodificar o arquivo")


def processar_csv(
    conteudo: bytes, nome_arquivo: str = "upload.csv"
) -> tuple[list[VendaIn], ResultadoIngestao]:
    texto = decodificar(conteudo)
    if not texto.strip():
        raise ArquivoInvalido("arquivo vazio")

    amostra = texto[:4096]
    try:
        separador = csv.Sniffer().sniff(amostra, delimiters=",;\t|").delimiter
    except csv.Error:
        separador = ";" if amostra.count(";") > amostra.count(",") else ","

    leitor = csv.DictReader(io.StringIO(texto), delimiter=separador)
    if not leitor.fieldnames:
        raise ArquivoInvalido("arquivo sem cabeçalho")

    mapa = {campo: APELIDOS.get(_slug(campo), _slug(campo)) for campo in leitor.fieldnames}
    faltando = COLUNAS_OBRIGATORIAS - set(mapa.values())
    if faltando:
        raise ArquivoInvalido("colunas ausentes: " + ", ".join(sorted(faltando)))

    validas: list[VendaIn] = []
    rejeitadas: list[LinhaRejeitada] = []
    lidas = 0

    for numero, bruta in enumerate(leitor, start=2):  # a linha 1 é o cabeçalho
        if all((v or "").strip() == "" for v in bruta.values()):
            continue
        lidas += 1
        linha = {mapa[k]: (v or "").strip() for k, v in bruta.items() if k in mapa}
        try:
            validas.append(VendaIn(**{c: linha.get(c, "") for c in COLUNAS_OBRIGATORIAS}))
        except ValidationError as exc:
            erros = [
                f"{'.'.join(str(p) for p in e['loc']) or 'linha'}: {e['msg']}"
                for e in exc.errors()
            ]
            rejeitadas.append(LinhaRejeitada(linha=numero, erros=erros, conteudo=linha))

    unicas: dict[str, VendaIn] = {}
    duplicadas = 0
    for venda in validas:
        if venda.fingerprint in unicas:
            duplicadas += 1
            continue
        unicas[venda.fingerprint] = venda

    resultado = ResultadoIngestao(
        arquivo=nome_arquivo,
        linhas_lidas=lidas,
        aceitas=len(unicas),
        duplicadas=duplicadas,
        rejeitadas=rejeitadas,
    )
    return list(unicas.values()), resultado


# =========================================================================== #
# 5. API
# =========================================================================== #

DESCRICAO = """
Importe o CSV de vendas, receba um relatório de qualidade dos dados e consulte
os indicadores prontos — faturamento, ticket médio, ranking de produtos,
distribuição por UF e crescimento mês a mês.

* Linha inválida não derruba o arquivo: ela volta no relatório com o motivo.
* Reimportar o mesmo arquivo não duplica venda nenhuma.
"""


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    criar_schema()
    yield


app = FastAPI(title="Ampere", version=VERSAO, description=DESCRICAO, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

Filtro = Annotated[str | None, Query(description="filtro opcional")]


@app.get("/", include_in_schema=False)
def painel() -> HTMLResponse:
    return HTMLResponse(PAINEL)


@app.get("/health", tags=["sistema"])
def health() -> dict[str, str]:
    return {"status": "ok", "versao": VERSAO}


@app.post("/api/v1/vendas/importar", tags=["ingestão"], status_code=201)
async def importar(arquivo: Annotated[UploadFile, File(description="CSV de vendas")]):
    conteudo = await arquivo.read()
    if len(conteudo) > limite_upload():
        raise HTTPException(413, "arquivo acima do limite configurado")
    try:
        vendas, resultado = processar_csv(conteudo, arquivo.filename or "upload.csv")
    except ArquivoInvalido as exc:
        raise HTTPException(422, str(exc)) from exc

    gravadas = inserir_vendas(vendas)
    return JSONResponse(
        status_code=201,
        content={
            "arquivo": resultado.arquivo,
            "linhas_lidas": resultado.linhas_lidas,
            "gravadas": gravadas,
            "duplicadas": resultado.duplicadas + (len(vendas) - gravadas),
            "rejeitadas": len(resultado.rejeitadas),
            "taxa_aproveitamento": resultado.taxa_aproveitamento,
            "detalhe_rejeicoes": [r.model_dump() for r in resultado.rejeitadas[:50]],
        },
    )


@app.delete("/api/v1/vendas", tags=["ingestão"])
def limpar() -> dict[str, int]:
    return {"removidas": limpar_tudo()}


@app.get("/api/v1/vendas", tags=["consultas"])
def listar(
    inicio: Filtro = None, fim: Filtro = None, produto: Filtro = None,
    categoria: Filtro = None, uf: Filtro = None,
    ordenar_por: str = "data", ordem: str = "desc",
    limite: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict:
    return listar_vendas(
        ordenar_por=ordenar_por, ordem=ordem, limite=limite, offset=offset,
        inicio=inicio, fim=fim, produto=produto, categoria=categoria, uf=uf,
    )


@app.get("/api/v1/metricas/resumo", tags=["métricas"])
def metricas_resumo(
    inicio: Filtro = None, fim: Filtro = None, produto: Filtro = None,
    categoria: Filtro = None, uf: Filtro = None,
) -> dict:
    return resumo(inicio=inicio, fim=fim, produto=produto, categoria=categoria, uf=uf)


@app.get("/api/v1/metricas/produtos", tags=["métricas"])
def metricas_produtos(
    limite: Annotated[int, Query(ge=1, le=100)] = 10,
    inicio: Filtro = None, fim: Filtro = None,
    categoria: Filtro = None, uf: Filtro = None,
) -> list[dict]:
    return por_produto(limite=limite, inicio=inicio, fim=fim, categoria=categoria, uf=uf)


@app.get("/api/v1/metricas/ufs", tags=["métricas"])
def metricas_ufs(
    inicio: Filtro = None, fim: Filtro = None, categoria: Filtro = None
) -> list[dict]:
    return por_uf(inicio=inicio, fim=fim, categoria=categoria)


@app.get("/api/v1/metricas/serie", tags=["métricas"])
def metricas_serie(
    granularidade: Annotated[str, Query(pattern="^(dia|mes)$")] = "mes",
    inicio: Filtro = None, fim: Filtro = None,
    categoria: Filtro = None, uf: Filtro = None,
) -> list[dict]:
    return serie_temporal(
        granularidade, inicio=inicio, fim=fim, categoria=categoria, uf=uf
    )


@app.get("/api/v1/metricas/crescimento", tags=["métricas"])
def metricas_crescimento(
    inicio: Filtro = None, fim: Filtro = None,
    categoria: Filtro = None, uf: Filtro = None,
) -> list[dict]:
    return crescimento_mensal(inicio=inicio, fim=fim, categoria=categoria, uf=uf)


# =========================================================================== #
# 6. GERADOR DE DADOS DE EXEMPLO
# =========================================================================== #

CATALOGO = [
    ("Bateria 60Ah selada", "Automotiva", 420, 610),
    ("Bateria 70Ah", "Automotiva", 500, 720),
    ("Bateria 100Ah", "Automotiva", 780, 1050),
    ("Bateria 150Ah estacionária", "Estacionária", 1100, 1600),
    ("Bateria 220Ah estacionária", "Estacionária", 1700, 2400),
    ("Bateria moto 5Ah", "Motocicleta", 110, 190),
    ("Bateria moto 12Ah", "Motocicleta", 180, 280),
    ("Carregador inteligente 12V", "Acessórios", 190, 330),
    ("Cabo chupeta 400A", "Acessórios", 60, 120),
    ("Teste de carga digital", "Acessórios", 240, 420),
]

CLIENTES = [
    "Auto Center Guaíba", "Oficina do Léo", "Distribuidora Sul Energia",
    "Mecânica Pampa", "Rede Pneus Canoas", "Transportes Cachoeirinha",
    "Loja Bateria Já", "Posto Avenida", "Frota Metropolitana", "Motos Centro",
    "Eletro Diesel RS", "Comercial Vale dos Sinos", "Garagem 24h",
    "Auto Peças Viamão", "Energia Total Caxias",
]

UFS_SORTEIO = ["RS"] * 6 + ["SC"] * 3 + ["PR"] * 2 + ["SP"] * 2 + ["MG", "RJ", "BA"]

CSV_COM_ERROS = """data;cliente;produto;categoria;quantidade;preco_unitario;uf
15/03/2026;Auto Center Guaíba;Bateria 60Ah selada;Automotiva;4;549,90;RS
15/03/2026;Oficina do Léo;Bateria moto 12Ah;Motocicleta;2;229,00;RS
32/03/2026;Mecânica Pampa;Bateria 70Ah;Automotiva;1;610,00;RS
16/03/2026;;Bateria 100Ah;Automotiva;2;980,00;SC
16/03/2026;Posto Avenida;Carregador inteligente 12V;Acessórios;-3;260,00;SC
17/03/2026;Rede Pneus Canoas;Bateria 150Ah estacionária;Estacionária;1;0;PR
17/03/2026;Garagem 24h;Cabo chupeta 400A;Acessórios;5;89,90;XX
18/03/2026;Energia Total Caxias;Bateria 220Ah estacionária;Estacionária;1;2.150,00;RS
18/03/2026;Motos Centro;Bateria moto 5Ah;Motocicleta;abc;150,00;RS
19/03/2026;Frota Metropolitana;Bateria 100Ah;Automotiva;3;1.020,50;MG
01/01/2099;Loja Bateria Já;Bateria 70Ah;Automotiva;2;700,00;RS
19/03/2026;Auto Peças Viamão;Teste de carga digital;Acessórios;1;319,90;RS
"""


def gerar_dados(linhas: int = 500, semente: int = 42) -> None:
    """Cria data/vendas_exemplo.csv e data/vendas_com_erros.csv."""
    random.seed(semente)
    inicio = date.today() - timedelta(days=540)
    registros = []

    for _ in range(linhas):
        produto, categoria, minimo, maximo = random.choice(CATALOGO)
        dia = inicio + timedelta(days=random.randint(0, 539))
        # Sazonalidade: no inverno vende mais bateria automotiva.
        peso = 1.6 if dia.month in (6, 7, 8) and categoria == "Automotiva" else 1.0
        quantidade = max(1, int(random.triangular(1, 14, 3) * peso))
        preco = round(random.uniform(minimo, maximo), 2)
        registros.append({
            "data": dia.strftime("%d/%m/%Y"),               # formato BR de propósito
            "cliente": random.choice(CLIENTES),
            "produto": produto,
            "categoria": categoria,
            "quantidade": quantidade,
            "preco_unitario": f"{preco:.2f}".replace(".", ","),   # vírgula decimal
            "uf": random.choice(UFS_SORTEIO),
        })

    registros.sort(key=lambda r: r["data"][6:] + r["data"][3:5] + r["data"][:2])
    pasta = RAIZ / "data"
    pasta.mkdir(parents=True, exist_ok=True)

    with (pasta / "vendas_exemplo.csv").open("w", newline="", encoding="utf-8") as saida:
        escritor = csv.DictWriter(saida, fieldnames=list(registros[0]), delimiter=";")
        escritor.writeheader()
        escritor.writerows(registros)

    (pasta / "vendas_com_erros.csv").write_text(CSV_COM_ERROS, encoding="utf-8")
    print(f"{len(registros)} linhas em data/vendas_exemplo.csv")
    print("12 linhas (7 com erro proposital) em data/vendas_com_erros.csv")


# =========================================================================== #
# 7. LINHA DE COMANDO
# =========================================================================== #

def main() -> None:
    comando = sys.argv[1] if len(sys.argv) > 1 else "rodar"

    if comando == "dados":
        quantidade = int(sys.argv[2]) if len(sys.argv) > 2 else 500
        gerar_dados(quantidade)
    elif comando == "rodar":
        import uvicorn

        criar_schema()
        print("Painel em http://localhost:8000  ·  documentação em /docs")
        uvicorn.run(app, host="127.0.0.1", port=8000)
    else:
        print(__doc__)


# =========================================================================== #
# 8. PAINEL — HTML, CSS e JS, sem dependência externa
# =========================================================================== #
PAINEL = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ampere · painel de vendas</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  :root{
    --papel:#e8ebec;
    --superficie:#f7f8f8;
    --tinta:#10171b;
    --grafite:#58666e;
    --linha:#cbd4d8;
    --carga:#116149;
    --polo:#1b4f72;
    --sinal:#a8700a;
    --raio:3px;
  }
  *{box-sizing:border-box}
  html,body{margin:0;padding:0}
  body{
    background:var(--papel);
    color:var(--tinta);
    font:400 16px/1.55 "IBM Plex Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
    -webkit-font-smoothing:antialiased;
  }
  .envelope{max-width:1080px;margin:0 auto;padding:32px 24px 72px}
  a{color:var(--carga)}

  /* ---------- topo ---------- */
  header{
    display:flex;flex-wrap:wrap;gap:20px;align-items:flex-end;justify-content:space-between;
    padding-bottom:20px;border-bottom:2px solid var(--tinta);
  }
  .marca{display:flex;align-items:baseline;gap:12px}
  .marca h1{font-size:30px;font-weight:600;letter-spacing:-.02em;margin:0}
  .marca span{color:var(--grafite);font-size:14px}
  .acoes{display:flex;flex-wrap:wrap;gap:8px;align-items:center}

  input[type=file]{
    font:inherit;font-size:14px;background:var(--superficie);
    border:1px solid var(--linha);border-radius:var(--raio);padding:7px 10px;max-width:240px;
  }
  button{
    font:inherit;font-size:14px;font-weight:500;cursor:pointer;
    border:1px solid var(--tinta);border-radius:var(--raio);
    background:var(--tinta);color:var(--superficie);padding:8px 16px;
  }
  button.secundario{background:transparent;color:var(--tinta);border-color:var(--linha)}
  button:disabled{opacity:.45;cursor:not-allowed}
  button:focus-visible,input:focus-visible,select:focus-visible{outline:2px solid var(--carga);outline-offset:2px}

  /* ---------- filtros ---------- */
  .filtros{display:flex;flex-wrap:wrap;gap:14px;align-items:flex-end;margin:22px 0 4px}
  .campo{display:flex;flex-direction:column;gap:4px}
  .campo label{font-size:12.5px;color:var(--grafite)}
  .campo input,.campo select{
    font:inherit;font-size:14px;padding:6px 9px;background:var(--superficie);
    border:1px solid var(--linha);border-radius:var(--raio);color:var(--tinta);
  }

  /* ---------- indicadores ---------- */
  .indicadores{display:flex;flex-wrap:wrap;margin:28px 0 8px;border-top:1px solid var(--linha)}
  .indicador{flex:1 1 180px;padding:18px 22px 16px;border-bottom:1px solid var(--linha)}
  .indicador + .indicador{border-left:1px solid var(--linha)}
  .indicador b{
    display:block;font-family:"IBM Plex Mono",ui-monospace,monospace;
    font-size:27px;font-weight:500;letter-spacing:-.03em;line-height:1.15;
  }
  .indicador small{display:block;margin-top:5px;color:var(--grafite);font-size:13px}

  /* ---------- blocos ---------- */
  section{margin-top:40px}
  section h2{font-size:17px;font-weight:600;margin:0 0 4px}
  section p.nota{margin:0 0 16px;color:var(--grafite);font-size:13.5px}
  .duplo{display:grid;grid-template-columns:1fr 1fr;gap:40px}
  @media (max-width:760px){.duplo{grid-template-columns:1fr}}

  svg{display:block;width:100%;height:auto;overflow:visible}
  .eixo{font-family:"IBM Plex Mono",monospace;font-size:10.5px;fill:var(--grafite)}

  .barras{display:flex;flex-direction:column;gap:11px}
  .barra{display:grid;grid-template-columns:1fr auto;gap:4px 12px;align-items:baseline}
  .barra .rotulo{font-size:14px}
  .barra .valor{font-family:"IBM Plex Mono",monospace;font-size:13px;color:var(--grafite)}
  .barra .trilho{grid-column:1/-1;height:7px;background:#dde3e5;border-radius:2px}
  .barra .preenchimento{height:100%;background:var(--carga);border-radius:2px}
  .barra.frio .preenchimento{background:var(--polo)}

  table{width:100%;border-collapse:collapse;font-size:14px}
  th,td{text-align:left;padding:9px 10px;border-bottom:1px solid var(--linha)}
  th{font-weight:500;color:var(--grafite);font-size:12.5px}
  td.num,th.num{text-align:right;font-family:"IBM Plex Mono",monospace}
  tbody tr:hover{background:var(--superficie)}

  /* ---------- avisos ---------- */
  .aviso{
    margin-top:18px;padding:14px 16px;background:var(--superficie);
    border-left:3px solid var(--carga);font-size:14px;
  }
  .aviso.erro{border-left-color:#9a2a2a}
  .aviso.atencao{border-left-color:var(--sinal)}
  .aviso ul{margin:8px 0 0;padding-left:18px;color:var(--grafite);font-size:13px}
  .vazio{padding:48px 0;max-width:46ch}
  .vazio h2{font-size:21px}
  code{font-family:"IBM Plex Mono",monospace;font-size:13px;background:var(--superficie);padding:1px 5px;border-radius:2px}

  .traco{stroke-dasharray:var(--comp);stroke-dashoffset:var(--comp);animation:desenhar 1.1s ease-out forwards}
  @keyframes desenhar{to{stroke-dashoffset:0}}
  @media (prefers-reduced-motion:reduce){.traco{animation:none;stroke-dashoffset:0}}
</style>
</head>
<body>
<div class="envelope">

  <header>
    <div class="marca">
      <h1>Ampere</h1>
      <span>vendas importadas e medidas</span>
    </div>
    <div class="acoes">
      <input type="file" id="arquivo" accept=".csv,text/csv" aria-label="Arquivo CSV de vendas">
      <button id="importar">Importar CSV</button>
      <button id="limpar" class="secundario">Limpar base</button>
    </div>
  </header>

  <div id="mensagem"></div>

  <div class="filtros" id="filtros" hidden>
    <div class="campo"><label for="inicio">De</label><input type="date" id="inicio"></div>
    <div class="campo"><label for="fim">Até</label><input type="date" id="fim"></div>
    <div class="campo"><label for="uf">Estado</label><select id="uf"><option value="">todos</option></select></div>
    <div class="campo"><label for="categoria">Categoria</label><select id="categoria"><option value="">todas</option></select></div>
    <button id="aplicar" class="secundario">Aplicar</button>
  </div>

  <main id="painel" hidden>
    <div class="indicadores" id="indicadores"></div>

    <section>
      <h2>Faturamento por mês</h2>
      <p class="nota" id="periodo"></p>
      <div id="grafico"></div>
    </section>

    <div class="duplo">
      <section>
        <h2>Produtos que mais faturam</h2>
        <p class="nota">Soma do valor vendido no período filtrado.</p>
        <div class="barras" id="produtos"></div>
      </section>
      <section>
        <h2>Distribuição por estado</h2>
        <p class="nota">Participação de cada UF no faturamento.</p>
        <div class="barras" id="ufs"></div>
      </section>
    </div>

    <section>
      <h2>Últimas vendas</h2>
      <p class="nota">As 15 mais recentes dentro do filtro.</p>
      <table>
        <thead><tr>
          <th>Data</th><th>Cliente</th><th>Produto</th><th>UF</th>
          <th class="num">Qtd</th><th class="num">Unitário</th><th class="num">Total</th>
        </tr></thead>
        <tbody id="tabela"></tbody>
      </table>
    </section>
  </main>

  <div class="vazio" id="vazio" hidden>
    <h2>Nada importado ainda</h2>
    <p>Escolha um arquivo CSV com as colunas <code>data</code>, <code>cliente</code>,
    <code>produto</code>, <code>categoria</code>, <code>quantidade</code>,
    <code>preco_unitario</code> e <code>uf</code>. Os exemplos prontos estão na pasta
    <code>data/</code> do repositório.</p>
  </div>

</div>

<script>
const api = (caminho, params) => {
  const url = new URL(caminho, window.location.origin);
  Object.entries(params || {}).forEach(([k, v]) => { if (v) url.searchParams.set(k, v); });
  return fetch(url).then(r => r.ok ? r.json() : r.json().then(e => Promise.reject(e)));
};

const dinheiro = n => n.toLocaleString('pt-BR', {style:'currency', currency:'BRL', maximumFractionDigits:0});
const numero  = n => n.toLocaleString('pt-BR');
const dataBR  = s => s.split('-').reverse().join('/');
const mesBR   = s => {
  const [ano, mes] = s.split('-');
  return ['jan','fev','mar','abr','mai','jun','jul','ago','set','out','nov','dez'][+mes-1] + '/' + ano.slice(2);
};

const el = id => document.getElementById(id);
const filtrosAtuais = () => ({
  inicio: el('inicio').value,
  fim: el('fim').value,
  uf: el('uf').value,
  categoria: el('categoria').value,
});

function mensagem(html, tipo = '') {
  el('mensagem').innerHTML = html ? `<div class="aviso ${tipo}">${html}</div>` : '';
}

/* ---------- gráfico de linha em SVG, sem biblioteca ---------- */
function desenharSerie(serie) {
  const alvo = el('grafico');
  if (serie.length < 2) {
    alvo.innerHTML = '<p class="nota">Poucos meses para desenhar uma série.</p>';
    return;
  }
  const L = 760, A = 230, m = {t:16, r:16, b:28, l:62};
  const maximo = Math.max(...serie.map(p => p.faturamento)) * 1.08;
  const x = i => m.l + i * (L - m.l - m.r) / (serie.length - 1);
  const y = v => m.t + (1 - v / maximo) * (A - m.t - m.b);

  const pontos = serie.map((p, i) => [x(i), y(p.faturamento)]);
  const traco = pontos.map((p, i) => (i ? 'L' : 'M') + p[0].toFixed(1) + ' ' + p[1].toFixed(1)).join(' ');
  const area = traco + ` L${x(serie.length-1).toFixed(1)} ${y(0)} L${x(0).toFixed(1)} ${y(0)} Z`;
  const comprimento = pontos.reduce((soma, p, i) =>
    i ? soma + Math.hypot(p[0]-pontos[i-1][0], p[1]-pontos[i-1][1]) : 0, 0);

  const grade = [0, .25, .5, .75, 1].map(f => {
    const vy = y(maximo * f);
    return `<line x1="${m.l}" y1="${vy}" x2="${L-m.r}" y2="${vy}" stroke="var(--linha)" stroke-width="1"/>
            <text class="eixo" x="${m.l-8}" y="${vy+3.5}" text-anchor="end">${dinheiro(maximo*f)}</text>`;
  }).join('');

  const passo = Math.ceil(serie.length / 12);
  const rotulos = serie.map((p, i) => i % passo === 0
    ? `<text class="eixo" x="${x(i)}" y="${A-m.b+18}" text-anchor="middle">${mesBR(p.periodo)}</text>` : '').join('');

  const marcas = pontos.map((p, i) =>
    `<circle cx="${p[0]}" cy="${p[1]}" r="3" fill="var(--carga)"><title>${mesBR(serie[i].periodo)} · ${dinheiro(serie[i].faturamento)} · ${serie[i].pedidos} pedidos</title></circle>`).join('');

  alvo.innerHTML = `<svg viewBox="0 0 ${L} ${A}" role="img" aria-label="Faturamento mensal">
    ${grade}${rotulos}
    <path d="${area}" fill="var(--carga)" opacity=".08"/>
    <path class="traco" style="--comp:${comprimento.toFixed(0)}" d="${traco}"
          fill="none" stroke="var(--carga)" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
    ${marcas}
  </svg>`;
}

function desenharBarras(alvo, itens, rotulo, valor, frio) {
  if (!itens.length) { el(alvo).innerHTML = '<p class="nota">Sem dados no filtro.</p>'; return; }
  const maximo = Math.max(...itens.map(valor));
  el(alvo).innerHTML = itens.map(i => `
    <div class="barra ${frio ? 'frio' : ''}">
      <span class="rotulo">${rotulo(i)}</span>
      <span class="valor">${dinheiro(valor(i))}</span>
      <span class="trilho"><span class="preenchimento" style="width:${(valor(i)/maximo*100).toFixed(1)}%"></span></span>
    </div>`).join('');
}

/* ---------- carga do painel ---------- */
async function carregar() {
  const f = filtrosAtuais();
  const resumo = await api('/api/v1/metricas/resumo', f);

  if (!resumo.pedidos) {
    el('painel').hidden = true;
    el('vazio').hidden = false;
    el('filtros').hidden = !(f.inicio || f.fim || f.uf || f.categoria);
    return;
  }
  el('vazio').hidden = true;
  el('painel').hidden = false;
  el('filtros').hidden = false;

  el('indicadores').innerHTML = [
    [dinheiro(resumo.faturamento), 'faturamento no período'],
    [numero(resumo.pedidos), 'vendas registradas'],
    [numero(resumo.itens), 'itens vendidos'],
    [dinheiro(resumo.ticket_medio), 'ticket médio'],
    [numero(resumo.clientes), 'clientes distintos'],
  ].map(([v, r]) => `<div class="indicador"><b>${v}</b><small>${r}</small></div>`).join('');

  el('periodo').textContent =
    `De ${dataBR(resumo.primeira_venda)} a ${dataBR(resumo.ultima_venda)}.`;

  const [serie, produtos, ufs, vendas] = await Promise.all([
    api('/api/v1/metricas/serie', {...f, granularidade: 'mes'}),
    api('/api/v1/metricas/produtos', {...f, limite: 7}),
    api('/api/v1/metricas/ufs', f),
    api('/api/v1/vendas', {...f, limite: 15}),
  ]);

  desenharSerie(serie);
  desenharBarras('produtos', produtos, p => p.produto, p => p.faturamento, false);
  desenharBarras('ufs', ufs, u => u.uf, u => u.faturamento, true);

  el('tabela').innerHTML = vendas.itens.map(v => `
    <tr>
      <td>${dataBR(v.data)}</td><td>${v.cliente}</td><td>${v.produto}</td><td>${v.uf}</td>
      <td class="num">${v.quantidade}</td>
      <td class="num">${dinheiro(v.preco_unitario)}</td>
      <td class="num">${dinheiro(v.total)}</td>
    </tr>`).join('');

  preencherSelects(ufs, produtos);
}

function preencherSelects(ufs, produtos) {
  const selUf = el('uf');
  if (selUf.options.length <= 1) {
    ufs.forEach(u => selUf.add(new Option(u.uf, u.uf)));
  }
  const selCat = el('categoria');
  if (selCat.options.length <= 1) {
    [...new Set(produtos.map(p => p.categoria))].forEach(c => selCat.add(new Option(c, c)));
  }
}

/* ---------- ações ---------- */
el('importar').addEventListener('click', async () => {
  const arquivo = el('arquivo').files[0];
  if (!arquivo) { mensagem('Escolha um arquivo CSV antes de importar.', 'atencao'); return; }

  const botao = el('importar');
  botao.disabled = true; botao.textContent = 'Importando…';
  try {
    const corpo = new FormData();
    corpo.append('arquivo', arquivo);
    const r = await fetch('/api/v1/vendas/importar', {method: 'POST', body: corpo});
    const dados = await r.json();
    if (!r.ok) throw dados;

    const amostra = dados.detalhe_rejeicoes.slice(0, 5)
      .map(x => `linha ${x.linha} — ${x.erros.join('; ')}`).join('</li><li>');
    mensagem(
      `<strong>${dados.gravadas}</strong> vendas gravadas de ${dados.linhas_lidas} linhas lidas ·
       ${dados.duplicadas} duplicadas ignoradas · ${dados.rejeitadas} rejeitadas
       (aproveitamento de ${dados.taxa_aproveitamento}%).
       ${amostra ? `<ul><li>${amostra}</li></ul>` : ''}`,
      dados.rejeitadas ? 'atencao' : ''
    );
    await carregar();
  } catch (erro) {
    mensagem('Não foi possível importar: ' + (erro.detail || erro.message || 'erro desconhecido'), 'erro');
  } finally {
    botao.disabled = false; botao.textContent = 'Importar CSV';
  }
});

el('limpar').addEventListener('click', async () => {
  if (!confirm('Remover todas as vendas da base?')) return;
  const r = await fetch('/api/v1/vendas', {method: 'DELETE'});
  const d = await r.json();
  mensagem(`${d.removidas} vendas removidas.`);
  await carregar();
});

el('aplicar').addEventListener('click', carregar);
carregar().catch(() => mensagem('A API não respondeu. Confira se o servidor está rodando.', 'erro'));
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
