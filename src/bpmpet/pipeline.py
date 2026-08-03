# -*- coding: utf-8 -*-
"""
BPMPET - Pipeline de monitoramento da producao de petroleo e gas (ANP).

Cada funcao publica carrega no docstring o identificador do elemento BPMN
correspondente, no formato [POOL.TIPO.N], replicado na matriz de
rastreabilidade (matriz_rastreabilidade.csv).

Fonte de dados: Portal de Dados Abertos da ANP, conjunto "Producao de
Petroleo e Gas Natural por Poco" (tres arquivos mensais: terra, mar,
pre-sal; volumes em bbl/dia e Mm3/dia; atributos de bacia, campo,
operador e periodo).
"""
from __future__ import annotations

import hashlib
import io
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import requests

from .lineage import LineageLogger
from .log_excecoes import registrar_erro

# ---------------------------------------------------------------------------
# Configuracao de schema
# ---------------------------------------------------------------------------

# Aliases tolerantes: nomes reais variam entre competencias/edicoes do layout.
ALIASES = {
    "periodo": {"periodo", "mes_ano", "mes_referencia", "periodo_referencia"},
    "ano": {"ano"},
    "mes": {"mes"},
    "estado": {"estado", "uf"},
    "bacia": {"bacia"},
    "campo": {"campo"},
    "poco": {"poco"},
    "operador": {"operador", "empresa_operadora", "concessionario"},
    "petroleo_bbl_dia": {
        "petroleo_bbl_dia", "producao_de_petroleo_bbl_dia", "petroleo_bbl_d",
    },
    "oleo_bbl_dia": {"oleo_bbl_dia", "producao_de_oleo_bbl_dia"},
    "gas_mm3_dia": {
        "gas_mm3_dia", "producao_de_gas_natural_mm3_dia", "gas_natural_mm3_dia",
        "producao_de_gas_mm3_dia",
    },
    "agua_bbl_dia": {"agua_bbl_dia", "producao_de_agua_bbl_dia", "agua_m3_dia"},
    "grau_api": {"grau_api", "api"},
}

COLUNAS_OBRIGATORIAS = ["bacia", "campo", "operador", "petroleo_bbl_dia", "gas_mm3_dia"]
CHAVES_NAO_NULAS = ["bacia", "campo"]

BACIAS_ESCOPO = {"SANTOS", "CAMPOS", "SOLIMOES", "SOLIMÕES"}


def _snake(nome: str) -> str:
    s = unicodedata.normalize("NFKD", str(nome)).encode("ascii", "ignore").decode()
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s).strip("_").lower()
    return s


def _resolver_colunas(colunas) -> dict:
    """Mapeia nomes reais -> nomes canonicos via ALIASES (tolerante a layout)."""
    resolvido = {}
    normalizadas = {c: _snake(c) for c in colunas}
    for canonico, apelidos in ALIASES.items():
        for original, norm in normalizadas.items():
            if norm in apelidos:
                resolvido[original] = canonico
                break
    return resolvido


@dataclass
class ResultadoValidacao:
    aprovado: bool
    faltantes: list = field(default_factory=list)
    nulos_em_chave: dict = field(default_factory=dict)
    colunas_resolvidas: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# [PIP.T1] Ingestao
# ---------------------------------------------------------------------------

def descobrir_links(pagina_url: str, sessao: requests.Session | None = None) -> dict:
    """Apoio a [PIP.T1]: varre a pagina do conjunto de dados e extrai os
    links .csv publicados, indexando por nome de arquivo. Evita hardcode de
    caminhos profundos, que a ANP altera entre edicoes."""
    s = sessao or requests.Session()
    resp = s.get(pagina_url, timeout=60)
    resp.raise_for_status()
    hrefs = re.findall(r'href="([^"]+?\.csv)"', resp.text, flags=re.IGNORECASE)
    links = {}
    for h in hrefs:
        url = h if h.startswith("http") else requests.compat.urljoin(pagina_url, h)
        links[Path(url).name.lower()] = url
    return links


def ingerir_dados(url: str, sessao: requests.Session | None = None) -> tuple[pd.DataFrame, int]:
    """[PIP.T1] Ingerir arquivo CSV via HTTP (biblioteca Requests).

    Retorna (DataFrame bruto, bytes baixados). Falhas HTTP conduzem ao
    gateway [PIP.GW1] no orquestrador; o registro em log e feito por
    [PIP.T1B] via registrar_erro().
    """
    s = sessao or requests.Session()
    resp = s.get(url, timeout=180)
    resp.raise_for_status()
    conteudo = resp.content
    for kwargs in (
        dict(sep=";", decimal=",", encoding="utf-8-sig"),
        dict(sep=";", decimal=",", encoding="latin-1"),
        dict(sep=",", decimal=".", encoding="utf-8-sig"),
    ):
        try:
            df = pd.read_csv(io.BytesIO(conteudo), low_memory=False, **kwargs)
            if df.shape[1] > 1:
                return df, len(conteudo)
        except Exception:
            continue
    raise ValueError("Nao foi possivel interpretar o CSV com os dialetos conhecidos")


# ---------------------------------------------------------------------------
# [PIP.T2] Validacao de schema
# ---------------------------------------------------------------------------

def limpar_rodape_cabecalho(df: pd.DataFrame, resolvidas: dict) -> tuple[pd.DataFrame, int]:
    """Remove linhas de metadados (cabecalhos duplicados e linhas em branco)
    que a ANP insere no topo de cada CSV. Sao identificadas pela ausencia
    simultanea de bacia e campo, que jamais falta em registro real de
    producao. Retorna (df_limpo, n_linhas_removidas) para registro auditavel."""
    inv = {v: k for k, v in resolvidas.items()}
    cols = [inv[c] for c in ("bacia", "campo") if c in inv]
    if not cols:
        return df, 0
    antes = len(df)
    limpo = df.dropna(subset=cols, how="all").reset_index(drop=True)
    return limpo, antes - len(limpo)


def validar_schema(df: pd.DataFrame) -> ResultadoValidacao:
    """[PIP.T2] Validar schema e integridade.

    Criterios (Batini; Scannapieco, 2016): presenca das colunas
    obrigatorias (apos resolucao de aliases), ausencia de nulos nas
    chaves. Decisao de fluxo em [PIP.GW2]; falha registrada por [PIP.T2B].
    """
    resolvidas = _resolver_colunas(df.columns)
    canonicos = set(resolvidas.values())
    faltantes = [c for c in COLUNAS_OBRIGATORIAS if c not in canonicos]
    nulos = {}
    if not faltantes:
        inv = {v: k for k, v in resolvidas.items()}
        for chave in CHAVES_NAO_NULAS:
            col = inv[chave]
            n = int(df[col].isna().sum())
            if n:
                nulos[chave] = n
    aprovado = not faltantes and not nulos
    return ResultadoValidacao(aprovado, faltantes, nulos, resolvidas)


# ---------------------------------------------------------------------------
# [PIP.T3] Transformacao
# ---------------------------------------------------------------------------

def transformar(df: pd.DataFrame, resolvidas: dict, competencia: str,
                lineage: LineageLogger | None = None) -> pd.DataFrame:
    """[PIP.T3] Transformar e padronizar (Pandas).

    Renomeia para o schema canonico, filtra bacias do escopo, forca tipos
    numericos e anota a competencia. Linhagem operacional registrada por
    tarefa (contagens, schema, transformacoes)."""
    antes = df.shape
    out = df.rename(columns=resolvidas).copy()
    out = out.loc[:, ~out.columns.duplicated()]
    out = out[[c for c in out.columns if c in ALIASES]]
    out["bacia_norm"] = (
        out["bacia"].astype(str).str.upper().map(lambda x: _snake(x).upper())
    )
    # conversao numerica em formato brasileiro (milhar '.', decimal ',')
    for c in ("petroleo_bbl_dia", "gas_mm3_dia", "agua_bbl_dia", "grau_api"):
        if c in out.columns:
            serie = out[c]
            if serie.dtype == object:
                serie = (serie.astype(str)
                              .str.replace(".", "", regex=False)
                              .str.replace(",", ".", regex=False))
            out[c] = pd.to_numeric(serie, errors="coerce")
    out = out[out["bacia_norm"].isin({_snake(b).upper() for b in BACIAS_ESCOPO})]
    out["competencia"] = competencia
    if lineage:
        colmap = {c: [orig] for orig, c in resolvidas.items()
                  if c in out.columns}
        colmap["bacia_norm"] = ["bacia"]
        colmap["competencia"] = []
        lineage.registrar(
            task_id="PIP.T3", competencia=competencia,
            linhas_in=antes[0], linhas_out=len(out),
            colunas_in=list(df.columns), colunas_out=list(out.columns),
            transformacao="rename_aliases; filtro_bacias(Santos,Campos,Solimoes); "
                          "coercao_numerica; anotacao_competencia",
            colmap=colmap,
        )
    return out


# ---------------------------------------------------------------------------
# [PIP.T4] Indicadores
# ---------------------------------------------------------------------------

def calcular_kpis(df: pd.DataFrame, competencia: str,
                  lineage: LineageLogger | None = None) -> pd.DataFrame:
    """[PIP.T4] Calcular indicadores de dominio (Pandas).

    Alem dos volumes agregados por bacia e operador, produz os
    indicadores acompanhados na rotina de engenharia de producao:
    water cut (agua sobre liquido total), razao gas-oleo (RGO, em
    m3/m3, com petroleo convertido de bbl a m3 pelo fator 0.158987),
    produtividade media por poco e participacao percentual na
    competencia."""
    agg = dict(petroleo_bbl_dia=("petroleo_bbl_dia", "sum"),
               gas_mm3_dia=("gas_mm3_dia", "sum"))
    if "agua_bbl_dia" in df.columns:
        agg["agua_bbl_dia"] = ("agua_bbl_dia", "sum")
    agg["pocos"] = ("poco", "nunique") if "poco" in df.columns \
        else ("campo", "count")
    g = (df.groupby(["competencia", "bacia_norm", "operador"], dropna=False)
           .agg(**agg).reset_index())
    tot = g.groupby("competencia")["petroleo_bbl_dia"].transform("sum")
    g["participacao_pct"] = (g["petroleo_bbl_dia"] / tot * 100).round(3)
    petroleo_m3 = g["petroleo_bbl_dia"] * 0.158987
    g["rgo_m3_m3"] = ((g["gas_mm3_dia"] * 1000) / petroleo_m3)\
        .where(petroleo_m3 > 0).round(2)
    if "agua_bbl_dia" in g.columns:
        liquido = g["agua_bbl_dia"] + g["petroleo_bbl_dia"]
        g["water_cut_pct"] = (g["agua_bbl_dia"] / liquido * 100)\
            .where(liquido > 0).round(2)
    g["produtividade_bbl_dia_poco"] = (g["petroleo_bbl_dia"] / g["pocos"])\
        .where(g["pocos"] > 0).round(2)
    if lineage:
        chave_poco = "poco" if "poco" in df.columns else "campo"
        colmap = {"petroleo_bbl_dia": ["petroleo_bbl_dia"],
                  "gas_mm3_dia": ["gas_mm3_dia"],
                  "pocos": [chave_poco],
                  "participacao_pct": ["petroleo_bbl_dia"],
                  "rgo_m3_m3": ["gas_mm3_dia", "petroleo_bbl_dia"],
                  "produtividade_bbl_dia_poco": ["petroleo_bbl_dia", chave_poco]}
        if "agua_bbl_dia" in g.columns:
            colmap["agua_bbl_dia"] = ["agua_bbl_dia"]
            colmap["water_cut_pct"] = ["agua_bbl_dia", "petroleo_bbl_dia"]
        lineage.registrar(
            task_id="PIP.T4", competencia=competencia,
            linhas_in=len(df), linhas_out=len(g),
            colunas_in=list(df.columns), colunas_out=list(g.columns),
            transformacao="groupby(competencia,bacia,operador); soma volumes; "
                          "contagem pocos; kpis de dominio",
            colmap=colmap,
        )
    return g


# ---------------------------------------------------------------------------
# Persistencia idempotente
# ---------------------------------------------------------------------------

def persistir_idempotente(kpis: pd.DataFrame, competencia: str, destino: Path) -> Path:
    """Escrita idempotente: a chave e a competencia. Reprocessar o mesmo
    mes substitui integralmente a particao anterior, nunca duplica.
    Propriedade verificada formalmente em tests/test_idempotencia.py e no
    benchmark (M5)."""
    destino.mkdir(parents=True, exist_ok=True)
    caminho = destino / f"kpis_{competencia}.csv"
    kpis.sort_values(list(kpis.columns)).to_csv(caminho, index=False)
    return caminho


def hash_particao(caminho: Path) -> str:
    """Evidencia do teste de idempotencia: SHA-256 do conteudo persistido."""
    return hashlib.sha256(caminho.read_bytes()).hexdigest()


# ---------------------------------------------------------------------------
# [PIP.T5] Relatorio
# ---------------------------------------------------------------------------

def gerar_relatorio(consolidado: pd.DataFrame, saida_html: Path) -> Path:
    """[PIP.T5] Gerar visualizacoes (Plotly): serie temporal de petroleo
    por bacia e participacao por operador, exportadas em HTML."""
    import plotly.express as px
    serie = (consolidado.groupby(["competencia", "bacia_norm"], as_index=False)
             ["petroleo_bbl_dia"].sum())
    fig = px.line(serie, x="competencia", y="petroleo_bbl_dia",
                  color="bacia_norm",
                  title="Producao de petroleo por bacia (bbl/dia)")
    saida_html.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(saida_html)
    return saida_html


# ---------------------------------------------------------------------------
# Orquestracao de uma competencia (GW1/GW2 + fins de erro)
# ---------------------------------------------------------------------------

def processar_competencia(url: str, competencia: str, destino: Path,
                          lineage: LineageLogger | None = None,
                          log_path: Path | None = None) -> dict:
    """Executa o fluxo completo de uma competencia, materializando os
    gateways do diagrama: [PIP.GW1] arquivo disponivel? e [PIP.GW2]
    dados validos? Retorna metricas por etapa para o benchmark."""
    import time
    m = {"competencia": competencia, "url": url, "status": None,
         "bytes": 0, "linhas_brutas": 0, "linhas_escopo": 0,
         "t_ingestao": 0.0, "t_validacao": 0.0, "t_transf": 0.0,
         "t_kpis": 0.0, "t_persist": 0.0}
    t0 = time.perf_counter()
    try:
        df, nbytes = ingerir_dados(url)
    except Exception as exc:  # [PIP.GW1] Nao -> [PIP.T1B] -> fim de erro
        registrar_erro("PIP.T1B", competencia, "falha_ingestao", str(exc), log_path)
        m["status"] = "erro_ingestao"
        return m
    m["t_ingestao"] = time.perf_counter() - t0
    m["bytes"], m["linhas_brutas"] = nbytes, len(df)
    if lineage:
        lineage.registrar(task_id="PIP.T1", competencia=competencia,
                          linhas_in=0, linhas_out=len(df),
                          colunas_in=[], colunas_out=list(df.columns),
                          transformacao=f"download_http({nbytes} bytes)")

    t0 = time.perf_counter()
    val = validar_schema(df)
    m["t_validacao"] = time.perf_counter() - t0
    if lineage:
        lineage.registrar(task_id="PIP.T2", competencia=competencia,
                          linhas_in=len(df), linhas_out=len(df),
                          colunas_in=list(df.columns), colunas_out=list(df.columns),
                          transformacao=f"validacao(aprovado={val.aprovado}; "
                                        f"faltantes={val.faltantes}; nulos={val.nulos_em_chave})")
    if not val.aprovado:  # [PIP.GW2] Nao -> [PIP.T2B] -> fim de erro
        registrar_erro("PIP.T2B", competencia, "falha_validacao",
                       f"faltantes={val.faltantes}; nulos={val.nulos_em_chave}", log_path)
        m["status"] = "erro_validacao"
        return m

    t0 = time.perf_counter()
    tratado = transformar(df, val.colunas_resolvidas, competencia, lineage)
    m["t_transf"] = time.perf_counter() - t0
    m["linhas_escopo"] = len(tratado)

    t0 = time.perf_counter()
    kpis = calcular_kpis(tratado, competencia, lineage)
    m["t_kpis"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    caminho = persistir_idempotente(kpis, competencia, destino)
    m["t_persist"] = time.perf_counter() - t0
    m["hash"] = hash_particao(caminho)
    m["status"] = "ok"
    return m
