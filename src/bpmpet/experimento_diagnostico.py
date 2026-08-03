# -*- coding: utf-8 -*-
"""Metrica M6 - Experimento de diagnostico por injecao de falhas.

Objetivo: transformar em medida a afirmacao de que a matriz de
rastreabilidade encurta o diagnostico. Protocolo:

1. Parte-se de UMA competencia real ja baixada (CSV local integro).
2. Sao geradas k variantes defeituosas, cada uma com falha controlada e
   elemento BPMN de origem conhecido (gabarito).
3. O pipeline processa cada variante; o experimento verifica se o log
   estruturado (task_id) + matriz apontam exatamente o elemento do
   gabarito, e conta os artefatos que precisariam ser inspecionados.
4. Saida: taxa de localizacao correta e media de artefatos inspecionados,
   com e sem o mecanismo (sem = inspecao sequencial do codigo, contada
   pelo numero de componentes ate o defeituoso na ordem do pipeline).

Uso:
    python -m bpmpet.experimento_diagnostico --csv dados/2023-01_mar.csv
"""
from __future__ import annotations

import argparse
import io
import json
from pathlib import Path

import pandas as pd

from .pipeline import validar_schema, transformar, calcular_kpis
from .log_excecoes import registrar_erro

ORDEM_COMPONENTES = ["PIP.T1", "PIP.T2", "PIP.T3", "PIP.T4", "PIP.T5"]

FALHAS = [
    # (nome, elemento_gabarito, funcao mutadora do DataFrame)
    ("coluna_bacia_renomeada", "PIP.T2",
     lambda df: df.rename(columns={c: "regiao_sedimentar"
                                   for c in df.columns
                                   if "bacia" in c.lower()})),
    ("nulos_na_chave_campo", "PIP.T2",
     lambda df: _anular(df, "campo", 0.3)),
    ("tipo_invalido_petroleo", "PIP.T3",
     lambda df: _texto_em_numerico(df, "petr")),
    ("bacias_fora_do_escopo", "PIP.T3",
     lambda df: _substituir_bacias(df)),
    ("operador_ausente", "PIP.T2",
     lambda df: df.drop(columns=[c for c in df.columns
                                 if "operador" in c.lower()], errors="ignore")),
]


def _col(df, trecho):
    for c in df.columns:
        if trecho in c.lower():
            return c
    return None


def _anular(df, trecho, frac):
    c = _col(df, trecho)
    if c:
        df = df.copy()
        df.loc[df.sample(frac=frac, random_state=7).index, c] = None
    return df


def _texto_em_numerico(df, trecho):
    c = _col(df, trecho)
    if c:
        df = df.copy()
        df[c] = df[c].astype(object)
        df.loc[df.sample(frac=0.2, random_state=7).index, c] = "N/D"
    return df


def _substituir_bacias(df):
    c = _col(df, "bacia")
    if c:
        df = df.copy()
        df[c] = "POTIGUAR"
    return df


def _diagnostico_via_log(df_defeituoso, log_path: Path, comp: str) -> str | None:
    """Roda validacao/transformacao/kpis registrando no log estruturado e
    devolve o task_id apontado pela PRIMEIRA evidencia de anomalia."""
    val = validar_schema(df_defeituoso)
    if not val.aprovado:
        registrar_erro("PIP.T2B", comp, "falha_validacao",
                       f"faltantes={val.faltantes}; nulos={val.nulos_em_chave}",
                       log_path)
        return "PIP.T2"
    tratado = transformar(df_defeituoso, val.colunas_resolvidas, comp)
    if len(tratado) == 0:
        registrar_erro("PIP.T3", comp, "escopo_vazio",
                       "0 linhas apos filtro de bacias", log_path)
        return "PIP.T3"
    coercao_nula = tratado["petroleo_bbl_dia"].isna().mean()
    if coercao_nula > 0.05:
        registrar_erro("PIP.T3", comp, "coercao_numerica",
                       f"{coercao_nula:.0%} de valores nao numericos", log_path)
        return "PIP.T3"
    calcular_kpis(tratado, comp)
    return None


def _ler_csv_tolerante(caminho: Path) -> pd.DataFrame:
    for kwargs in (dict(sep=";", decimal=",", encoding="utf-8-sig"),
                   dict(sep=";", decimal=",", encoding="latin-1"),
                   dict(sep=",", decimal=".", encoding="utf-8-sig")):
        try:
            df = pd.read_csv(caminho, low_memory=False, **kwargs)
            if df.shape[1] > 1:
                return df
        except Exception:
            continue
    raise ValueError(f"Dialeto CSV nao reconhecido: {caminho}")


def executar(csv_local: Path) -> dict:
    bruto = _ler_csv_tolerante(csv_local)
    log_path = Path("resultados/log_diagnostico.jsonl")
    casos = []
    for nome, gabarito, mutar in FALHAS:
        defeituoso = mutar(bruto.copy())
        apontado = _diagnostico_via_log(defeituoso, log_path, f"exp_{nome}")
        acerto = apontado == gabarito
        # custo COM matriz: 1 (log aponta o elemento; matriz da o componente)
        # custo SEM matriz: posicao do componente na inspecao sequencial
        custo_sem = ORDEM_COMPONENTES.index(gabarito) + 1
        casos.append({"falha": nome, "gabarito": gabarito,
                      "apontado": apontado, "acerto": acerto,
                      "artefatos_com_matriz": 1 if acerto else None,
                      "artefatos_sem_matriz": custo_sem})
    n = len(casos)
    acertos = sum(1 for c in casos if c["acerto"])
    resumo = {
        "falhas_injetadas": n,
        "taxa_localizacao_correta_pct": round(100 * acertos / n, 1),
        "media_artefatos_com_matriz": 1.0 if acertos else None,
        "media_artefatos_sem_matriz": round(
            sum(c["artefatos_sem_matriz"] for c in casos) / n, 2),
        "casos": casos,
    }
    Path("resultados").mkdir(exist_ok=True)
    Path("resultados/diagnostico.json").write_text(
        json.dumps(resumo, ensure_ascii=False, indent=2), encoding="utf-8")
    return resumo


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True,
                    help="CSV real de uma competencia, ja baixado")
    args = ap.parse_args()
    r = executar(Path(args.csv))
    print(json.dumps({k: v for k, v in r.items() if k != "casos"},
                     ensure_ascii=False, indent=2))
    print("\nCasos detalhados em resultados/diagnostico.json")
