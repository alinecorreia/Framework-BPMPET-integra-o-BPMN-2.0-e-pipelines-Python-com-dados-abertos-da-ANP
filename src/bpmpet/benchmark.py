# -*- coding: utf-8 -*-
"""Protocolo experimental do artigo (metricas M1 a M5).

M1 Volumetria  : n arquivos, n registros, bytes baixados
M2 Desempenho  : tempo total, tempo medio por competencia, por etapa
M3 Memoria     : pico de alocacao Python (tracemalloc)
M4 Qualidade   : % competencias aprovadas na validacao; excecoes por tipo
M5 Idempotencia: reexecucao de uma mesma competencia deve produzir
                 particao byte-a-byte identica (SHA-256 igual)

Uso:
    python -m bpmpet.benchmark --config config_fontes.json

config_fontes.json:
    {"competencias": {"2023-01": "https://.../2023_01_producao_mar.csv", ...}}

Os links sao obtidos na pagina oficial do conjunto (ver README) ou via
pipeline.descobrir_links(). A saida inclui uma tabela Markdown pronta
para transcricao na secao 2.4 do artigo, alem de resultados/benchmark.json.
"""
from __future__ import annotations

import argparse
import json
import platform
import time
import tracemalloc
from pathlib import Path

import pandas as pd

from .lineage import LineageLogger
from .pipeline import (hash_particao, persistir_idempotente,
                       processar_competencia, gerar_relatorio)

RES = Path("resultados")


def _ambiente() -> dict:
    import requests, plotly
    return {
        "python": platform.python_version(),
        "so": f"{platform.system()} {platform.release()}",
        "processador": platform.processor() or platform.machine(),
        "pandas": pd.__version__,
        "requests": requests.__version__,
        "plotly": plotly.__version__,
    }


def executar(config: dict) -> dict:
    competencias = config["competencias"]
    lineage = LineageLogger(RES / "lineage.jsonl")
    log_exc = RES / "log_excecoes.jsonl"
    destino = RES / "particoes"

    tracemalloc.start()
    t_ini = time.perf_counter()
    execucoes = []
    for comp, url in sorted(competencias.items()):
        execucoes.append(processar_competencia(url, comp, destino,
                                               lineage, log_exc))
    t_total = time.perf_counter() - t_ini
    _, pico = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    ok = [e for e in execucoes if e["status"] == "ok"]
    falhas = [e for e in execucoes if e["status"] != "ok"]

    # M5 - idempotencia: reexecuta a primeira competencia aprovada
    idem = {"testado": False}
    if ok:
        alvo = ok[0]
        h1 = alvo["hash"]
        re_exec = processar_competencia(alvo["url"], alvo["competencia"],
                                        destino, lineage, log_exc)
        idem = {"testado": True, "competencia": alvo["competencia"],
                "hash_execucao_1": h1, "hash_execucao_2": re_exec.get("hash"),
                "identicos": h1 == re_exec.get("hash")}

    # consolidado + relatorio [PIP.T5]
    if ok:
        frames = [pd.read_csv(destino / f"kpis_{e['competencia']}.csv")
                  for e in ok]
        consolidado = pd.concat(frames, ignore_index=True)
        consolidado.to_csv(RES / "kpis_consolidado.csv", index=False)
        gerar_relatorio(consolidado, RES / "relatorio.html")

    resultado = {
        "ambiente": _ambiente(),
        "M1_volumetria": {
            "arquivos_processados": len(execucoes),
            "arquivos_aprovados": len(ok),
            "registros_brutos": int(sum(e["linhas_brutas"] for e in execucoes)),
            "registros_escopo_3bacias": int(sum(e["linhas_escopo"] for e in ok)),
            "bytes_baixados": int(sum(e["bytes"] for e in execucoes)),
            "mb_baixados": round(sum(e["bytes"] for e in execucoes) / 1e6, 2),
        },
        "M2_desempenho": {
            "tempo_total_s": round(t_total, 2),
            "tempo_medio_por_competencia_s": round(t_total / max(len(execucoes), 1), 2),
            "media_por_etapa_s": {
                k: round(sum(e[k] for e in ok) / max(len(ok), 1), 3)
                for k in ("t_ingestao", "t_validacao", "t_transf",
                          "t_kpis", "t_persist")
            },
            "throughput_registros_por_s": round(
                sum(e["linhas_brutas"] for e in ok) / max(t_total, 1e-9), 1),
        },
        "M3_memoria": {"pico_alocacao_mb": round(pico / 1e6, 1)},
        "M4_qualidade": {
            "pct_competencias_aprovadas": round(100 * len(ok) / max(len(execucoes), 1), 1),
            "falhas_por_tipo": {
                t: sum(1 for e in falhas if e["status"] == t)
                for t in {e["status"] for e in falhas}
            },
        },
        "M5_idempotencia": idem,
        "execucoes": execucoes,
    }
    RES.mkdir(exist_ok=True)
    (RES / "benchmark.json").write_text(
        json.dumps(resultado, ensure_ascii=False, indent=2), encoding="utf-8")
    return resultado


def tabela_markdown(r: dict) -> str:
    m1, m2 = r["M1_volumetria"], r["M2_desempenho"]
    m3, m4, m5 = r["M3_memoria"], r["M4_qualidade"], r["M5_idempotencia"]
    linhas = [
        "| Metrica | Valor |", "|---|---|",
        f"| Arquivos processados | {m1['arquivos_processados']} |",
        f"| Registros brutos | {m1['registros_brutos']:,} |".replace(",", "."),
        f"| Registros no escopo (3 bacias) | {m1['registros_escopo_3bacias']:,} |".replace(",", "."),
        f"| Volume baixado | {m1['mb_baixados']} MB |",
        f"| Tempo total | {m2['tempo_total_s']} s |",
        f"| Tempo medio por competencia | {m2['tempo_medio_por_competencia_s']} s |",
        f"| Throughput | {m2['throughput_registros_por_s']} registros/s |",
        f"| Pico de memoria (alocacao Python) | {m3['pico_alocacao_mb']} MB |",
        f"| Competencias aprovadas na validacao | {m4['pct_competencias_aprovadas']}% |",
        f"| Idempotencia (hashes identicos) | {m5.get('identicos')} |",
    ]
    return "\n".join(linhas)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    r = executar(cfg)
    print("\n== RESULTADOS (transcrever na secao 2.4 do artigo) ==\n")
    print(tabela_markdown(r))
    print("\nDetalhes em resultados/benchmark.json, lineage.jsonl, "
          "log_excecoes.jsonl, kpis_consolidado.csv e relatorio.html")
