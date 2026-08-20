# -*- coding: utf-8 -*-
"""Testes formais do framework BPMPET.

Cobrem as propriedades que a monografia declara e o protocolo mede:
idempotencia (M5), filtro de escopo, indicadores de dominio calculados
em [PIP.T4] e linhagem em nivel de coluna. Rodam offline, sobre fixture
local, sem depender da disponibilidade do portal da ANP.

    pytest tests/ -q
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

from bpmpet.pipeline import (validar_schema, transformar, calcular_kpis,
                             persistir_idempotente, hash_particao)

# Fixture minima com duas bacias no escopo e uma fora, para exercitar
# o filtro de [PIP.T3]. Os numeros vem em formato brasileiro, como na fonte.
FIXTURE = pd.DataFrame({
    "Bacia": ["Santos", "Santos", "Campos", "Potiguar"],
    "Campo": ["A", "A", "B", "C"],
    "Poco": ["P1", "P2", "P3", "P4"],
    "Operador": ["OP1", "OP1", "OP2", "OP3"],
    "Petroleo (bbl/dia)": ["10,5", "4,5", "7,0", "9,9"],
    "Gas Natural (Mm3/dia)": ["1,0", "0,5", "2,0", "3,0"],
    "Agua (bbl/dia)": ["2,0", "2,0", "3,0", "1,0"],
})


def _preparar() -> pd.DataFrame:
    return FIXTURE.copy()


def _rodar(tmp: Path) -> str:
    df = _preparar()
    val = validar_schema(df)
    assert val.aprovado, val
    tratado = transformar(df, val.colunas_resolvidas, "2099-01")
    kpis = calcular_kpis(tratado, "2099-01")
    caminho = persistir_idempotente(kpis, "2099-01", tmp)
    return hash_particao(caminho)


def test_reexecucao_produz_hash_identico(tmp_path):
    """M5: reprocessar a mesma competencia substitui a particao, nao duplica."""
    h1 = _rodar(tmp_path)
    h2 = _rodar(tmp_path)
    assert h1 == h2


def test_filtro_de_escopo_exclui_bacia_fora():
    """[PIP.T3] mantem apenas as bacias declaradas no escopo do estudo."""
    df = _preparar()
    val = validar_schema(df)
    tratado = transformar(df, val.colunas_resolvidas, "2099-01")
    assert set(tratado["bacia_norm"]) == {"SANTOS", "CAMPOS"}


def test_conversao_de_dialeto_numerico_misto():
    """A fonte alterna entre 1.273,34 (BR) e 247.2023 (US) ao longo da
    serie; a coercao infere o dialeto por valor, nao por arquivo."""
    df = _preparar()
    df.loc[0, "Petroleo (bbl/dia)"] = "1.273,3439"   # padrao brasileiro
    df.loc[1, "Petroleo (bbl/dia)"] = "247.2023"     # padrao anglofono
    val = validar_schema(df)
    tratado = transformar(df, val.colunas_resolvidas, "2099-01")
    valores = sorted(tratado["petroleo_bbl_dia"].dropna().tolist())
    assert any(abs(v - 247.2023) < 0.01 for v in valores)
    assert any(abs(v - 1273.3439) < 0.01 for v in valores)


def test_kpis_de_dominio():
    """[PIP.T4] entrega indicadores com semantica de engenharia de producao."""
    df = _preparar()
    val = validar_schema(df)
    tratado = transformar(df, val.colunas_resolvidas, "2099-01")
    k = calcular_kpis(tratado, "2099-01")
    for col in ("water_cut_pct", "rgo_m3_m3",
                "produtividade_bbl_dia_poco", "participacao_pct"):
        assert col in k.columns, col
    assert k["water_cut_pct"].between(0, 100).all()
    assert (k["pocos"] >= 1).all()


def test_linhagem_em_nivel_de_coluna(tmp_path):
    """O facet columnLineage declara de quais colunas cada indicador deriva."""
    import json
    from bpmpet.lineage import LineageLogger

    df = _preparar()
    val = validar_schema(df)
    registro = LineageLogger(tmp_path / "lin.jsonl")
    tratado = transformar(df, val.colunas_resolvidas, "2099-01", registro)
    calcular_kpis(tratado, "2099-01", registro)

    eventos = [json.loads(l) for l
               in (tmp_path / "lin.jsonl").read_text().splitlines()]
    t4 = [e for e in eventos if e["task_id"] == "PIP.T4"][0]
    colunas = t4["openlineage"]["outputs"][0]["facets"]["columnLineage"]
    assert colunas["water_cut_pct"] == ["agua_bbl_dia", "petroleo_bbl_dia"]
    assert "rgo_m3_m3" in colunas
