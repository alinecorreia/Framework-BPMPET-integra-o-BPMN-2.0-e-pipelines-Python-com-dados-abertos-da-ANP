# -*- coding: utf-8 -*-
"""Benchmark sobre fontes ZIP mensais da ANP (M1-M5).
Cada competencia e um ZIP contendo CSVs terra/mar/pre-sal, concatenados
antes do fluxo de validacao/transformacao/indicadores do pipeline."""
from __future__ import annotations
import argparse, json, platform, time, tracemalloc
from pathlib import Path
import pandas as pd
from .fonte_zip import baixar_zip
from .lineage import LineageLogger
from .log_excecoes import registrar_erro
from .pipeline import (validar_schema, transformar, calcular_kpis,
                       persistir_idempotente, hash_particao, gerar_relatorio,
                       limpar_rodape_cabecalho, _resolver_colunas)

RES = Path("resultados")


def _ambiente():
    import requests, plotly
    return {"python": platform.python_version(),
            "so": f"{platform.system()} {platform.release()}",
            "processador": platform.processor() or platform.machine(),
            "pandas": pd.__version__, "requests": requests.__version__,
            "plotly": plotly.__version__}


def processar_zip(url, competencia, destino, lineage, log_path):
    m = {"competencia": competencia, "url": url, "status": None, "bytes": 0,
         "linhas_brutas": 0, "linhas_escopo": 0, "t_ingestao": 0.0,
         "t_validacao": 0.0, "t_transf": 0.0, "t_kpis": 0.0, "t_persist": 0.0}
    t0 = time.perf_counter()
    try:
        import requests
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0 (pesquisa academica UFRJ)"})
        r = s.get(url, timeout=180); r.raise_for_status()
        m["bytes"] = len(r.content)
        import io, zipfile
        z = zipfile.ZipFile(io.BytesIO(r.content))
        from .fonte_zip import _ler_csv_bytes
        partes = [_ler_csv_bytes(z.read(n)) for n in z.namelist()
                  if n.lower().endswith(".csv")]
        df = pd.concat(partes, ignore_index=True)
    except Exception as exc:
        registrar_erro("PIP.T1B", competencia, "falha_ingestao", str(exc), log_path)
        m["status"] = "erro_ingestao"; return m
    m["t_ingestao"] = time.perf_counter() - t0
    m["linhas_brutas"] = len(df)
    df, removidas = limpar_rodape_cabecalho(df, _resolver_colunas(df.columns))
    m["linhas_metadados_removidas"] = removidas
    if lineage:
        lineage.registrar("PIP.T1", competencia, 0, len(df), [],
                          list(df.columns), f"download_zip({m['bytes']} bytes)")
    t0 = time.perf_counter()
    val = validar_schema(df)
    m["t_validacao"] = time.perf_counter() - t0
    if not val.aprovado:
        registrar_erro("PIP.T2B", competencia, "falha_validacao",
                       f"faltantes={val.faltantes}; nulos={val.nulos_em_chave}", log_path)
        m["status"] = "erro_validacao"; return m
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
    m["hash"] = hash_particao(caminho); m["status"] = "ok"
    return m


def executar(config):
    comps = config["competencias"]
    lineage = LineageLogger(RES / "lineage.jsonl")
    log_exc = RES / "log_excecoes.jsonl"
    destino = RES / "particoes"
    tracemalloc.start(); t_ini = time.perf_counter()
    execucoes = [processar_zip(u, c, destino, lineage, log_exc)
                 for c, u in sorted(comps.items())]
    t_total = time.perf_counter() - t_ini
    _, pico = tracemalloc.get_traced_memory(); tracemalloc.stop()
    ok = [e for e in execucoes if e["status"] == "ok"]
    falhas = [e for e in execucoes if e["status"] != "ok"]
    idem = {"testado": False}
    if ok:
        alvo = ok[0]; h1 = alvo["hash"]
        re_ex = processar_zip(alvo["url"], alvo["competencia"], destino, lineage, log_exc)
        idem = {"testado": True, "competencia": alvo["competencia"],
                "hash_1": h1[:16], "hash_2": re_ex.get("hash", "")[:16],
                "identicos": h1 == re_ex.get("hash")}
    if ok:
        frames = [pd.read_csv(destino / f"kpis_{e['competencia']}.csv") for e in ok]
        consolidado = pd.concat(frames, ignore_index=True)
        consolidado.to_csv(RES / "kpis_consolidado.csv", index=False)
        gerar_relatorio(consolidado, RES / "relatorio.html")
    res = {
        "ambiente": _ambiente(),
        "M1_volumetria": {"arquivos_processados": len(execucoes),
            "arquivos_aprovados": len(ok),
            "registros_brutos": int(sum(e["linhas_brutas"] for e in execucoes)),
            "registros_escopo_3bacias": int(sum(e["linhas_escopo"] for e in ok)),
            "mb_baixados": round(sum(e["bytes"] for e in execucoes)/1e6, 2)},
        "M2_desempenho": {"tempo_total_s": round(t_total, 2),
            "tempo_medio_por_competencia_s": round(t_total/max(len(execucoes),1), 2),
            "media_por_etapa_s": {k: round(sum(e[k] for e in ok)/max(len(ok),1), 3)
                for k in ("t_ingestao","t_validacao","t_transf","t_kpis","t_persist")},
            "throughput_registros_por_s": round(
                sum(e["linhas_brutas"] for e in ok)/max(t_total,1e-9), 1)},
        "M3_memoria": {"pico_alocacao_mb": round(pico/1e6, 1)},
        "M4_qualidade": {"pct_competencias_aprovadas": round(100*len(ok)/max(len(execucoes),1), 1),
            "falhas_por_tipo": {t: sum(1 for e in falhas if e["status"]==t)
                for t in {e["status"] for e in falhas}}},
        "M5_idempotencia": idem, "execucoes": execucoes}
    RES.mkdir(exist_ok=True)
    (RES/"benchmark.json").write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    return res


def tabela(r):
    m1,m2,m3,m4,m5 = (r["M1_volumetria"],r["M2_desempenho"],r["M3_memoria"],
                      r["M4_qualidade"],r["M5_idempotencia"])
    L = ["| Metrica | Valor |","|---|---|",
         f"| Arquivos processados | {m1['arquivos_processados']} |",
         f"| Registros brutos | {m1['registros_brutos']} |",
         f"| Registros no escopo (3 bacias) | {m1['registros_escopo_3bacias']} |",
         f"| Volume baixado | {m1['mb_baixados']} MB |",
         f"| Tempo total | {m2['tempo_total_s']} s |",
         f"| Tempo medio/competencia | {m2['tempo_medio_por_competencia_s']} s |",
         f"| Throughput | {m2['throughput_registros_por_s']} reg/s |",
         f"| Pico de memoria | {m3['pico_alocacao_mb']} MB |",
         f"| Competencias aprovadas | {m4['pct_competencias_aprovadas']}% |",
         f"| Idempotencia (hashes identicos) | {m5.get('identicos')} |"]
    return "\n".join(L)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--config", required=True)
    a = ap.parse_args()
    cfg = json.loads(Path(a.config).read_text(encoding="utf-8"))
    r = executar(cfg)
    print("\n== RESULTADOS (secao 2.4 do artigo) ==\n")
    print(tabela(r))
    print(f"\nFalhas: {r['M4_qualidade']['falhas_por_tipo']}")
