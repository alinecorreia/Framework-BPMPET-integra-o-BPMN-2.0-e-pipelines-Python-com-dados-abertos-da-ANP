# -*- coding: utf-8 -*-
"""Suporte a fontes ZIP da ANP (cada ZIP mensal contem CSVs terra/mar/pre-sal).
Estende [PIP.T1]: baixa o ZIP, extrai os CSVs internos e devolve DataFrames
por ambiente, com o mesmo leitor tolerante de dialeto do pipeline."""
from __future__ import annotations
import io, zipfile
import pandas as pd
import requests


def _ler_csv_bytes(raw: bytes) -> pd.DataFrame:
    for kw in (dict(sep=";", decimal=",", encoding="utf-8-sig"),
               dict(sep=";", decimal=",", encoding="latin-1"),
               dict(sep=",", decimal=".", encoding="utf-8-sig")):
        try:
            df = pd.read_csv(io.BytesIO(raw), low_memory=False, **kw)
            if df.shape[1] > 1:
                return df
        except Exception:
            continue
    raise ValueError("dialeto CSV nao reconhecido")


def baixar_zip(url: str, sessao: requests.Session | None = None) -> dict:
    """Retorna {nome_interno: DataFrame} para cada CSV dentro do ZIP."""
    s = sessao or requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (pesquisa academica UFRJ)"})
    r = s.get(url, timeout=180)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    out = {}
    for nome in z.namelist():
        if nome.lower().endswith(".csv"):
            out[nome] = _ler_csv_bytes(z.read(nome))
    return out
