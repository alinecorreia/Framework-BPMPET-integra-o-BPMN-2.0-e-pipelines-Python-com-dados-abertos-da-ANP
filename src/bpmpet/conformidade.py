# -*- coding: utf-8 -*-
"""Metrica M7 - Verificacao de conformidade (conformance checking).

Fecha o ciclo de BPM descrito por Van der Aalst (2016): o event log das
execucoes do pipeline e confrontado com o modelo BPMN do processo, via
PM4Py (BERTI; VAN ZELST; VAN DER AALST, 2019). O modelo e convertido em
rede de Petri e o log e reproduzido por token-based replay, produzindo o
fitness medio e o percentual de traces aderentes.

Construcao do log: um trace por competencia, com as atividades extraidas
das execucoes registradas pelo benchmark (benchmark.json). Convencao:
traces com status "ok" incluem PIP.T5, pois o relatorio consolidado
contempla cada competencia aprovada; traces de erro terminam na tarefa de
registro correspondente (PIP.T1B ou PIP.T2B), como no diagrama.

Uso:
    PYTHONPATH=src python -m bpmpet.conformidade --benchmark resultados/benchmark.json
"""
from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

import pandas as pd

# BPMN em nivel de processo (pool do pipeline), identico em fluxo ao
# diagrama do artigo; nomes de atividade = identificadores da matriz.
BPMN_PROCESSO = """<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  id="Defs_BPMPET" targetNamespace="http://bpmpet/anp">
  <bpmn:process id="ProcPipeline" isExecutable="false">
    <bpmn:startEvent id="ev_ini" name="Receber CSV">
      <bpmn:outgoing>f01</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:task id="t1" name="PIP.T1">
      <bpmn:incoming>f01</bpmn:incoming><bpmn:outgoing>f02</bpmn:outgoing>
    </bpmn:task>
    <bpmn:exclusiveGateway id="gw1" name="Arquivo disponivel?">
      <bpmn:incoming>f02</bpmn:incoming>
      <bpmn:outgoing>f03</bpmn:outgoing><bpmn:outgoing>f04</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:task id="t1b" name="PIP.T1B">
      <bpmn:incoming>f04</bpmn:incoming><bpmn:outgoing>f05</bpmn:outgoing>
    </bpmn:task>
    <bpmn:endEvent id="ev_err1" name="Fim erro ingestao">
      <bpmn:incoming>f05</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:task id="t2" name="PIP.T2">
      <bpmn:incoming>f03</bpmn:incoming><bpmn:outgoing>f06</bpmn:outgoing>
    </bpmn:task>
    <bpmn:exclusiveGateway id="gw2" name="Dados validos?">
      <bpmn:incoming>f06</bpmn:incoming>
      <bpmn:outgoing>f07</bpmn:outgoing><bpmn:outgoing>f08</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:task id="t2b" name="PIP.T2B">
      <bpmn:incoming>f08</bpmn:incoming><bpmn:outgoing>f09</bpmn:outgoing>
    </bpmn:task>
    <bpmn:endEvent id="ev_err2" name="Fim erro validacao">
      <bpmn:incoming>f09</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:task id="t3" name="PIP.T3">
      <bpmn:incoming>f07</bpmn:incoming><bpmn:outgoing>f10</bpmn:outgoing>
    </bpmn:task>
    <bpmn:task id="t4" name="PIP.T4">
      <bpmn:incoming>f10</bpmn:incoming><bpmn:outgoing>f11</bpmn:outgoing>
    </bpmn:task>
    <bpmn:task id="t5" name="PIP.T5">
      <bpmn:incoming>f11</bpmn:incoming><bpmn:outgoing>f12</bpmn:outgoing>
    </bpmn:task>
    <bpmn:endEvent id="ev_fim" name="Fim sucesso">
      <bpmn:incoming>f12</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f01" sourceRef="ev_ini" targetRef="t1"/>
    <bpmn:sequenceFlow id="f02" sourceRef="t1" targetRef="gw1"/>
    <bpmn:sequenceFlow id="f03" sourceRef="gw1" targetRef="t2"/>
    <bpmn:sequenceFlow id="f04" sourceRef="gw1" targetRef="t1b"/>
    <bpmn:sequenceFlow id="f05" sourceRef="t1b" targetRef="ev_err1"/>
    <bpmn:sequenceFlow id="f06" sourceRef="t2" targetRef="gw2"/>
    <bpmn:sequenceFlow id="f07" sourceRef="gw2" targetRef="t3"/>
    <bpmn:sequenceFlow id="f08" sourceRef="gw2" targetRef="t2b"/>
    <bpmn:sequenceFlow id="f09" sourceRef="t2b" targetRef="ev_err2"/>
    <bpmn:sequenceFlow id="f10" sourceRef="t3" targetRef="t4"/>
    <bpmn:sequenceFlow id="f11" sourceRef="t4" targetRef="t5"/>
    <bpmn:sequenceFlow id="f12" sourceRef="t5" targetRef="ev_fim"/>
  </bpmn:process>
</bpmn:definitions>
"""

TRACE_POR_STATUS = {
    "ok": ["PIP.T1", "PIP.T2", "PIP.T3", "PIP.T4", "PIP.T5"],
    "erro_validacao": ["PIP.T1", "PIP.T2", "PIP.T2B"],
    "erro_ingestao": ["PIP.T1", "PIP.T1B"],
}


def montar_log(execucoes: list) -> pd.DataFrame:
    linhas = []
    t = pd.Timestamp("2026-01-01")
    for e in execucoes:
        atividades = TRACE_POR_STATUS.get(e["status"])
        if not atividades:
            continue
        for i, a in enumerate(atividades):
            linhas.append({"case:concept:name": e["competencia"],
                           "concept:name": a,
                           "time:timestamp": t + pd.Timedelta(seconds=i)})
        t += pd.Timedelta(minutes=1)
    return pd.DataFrame(linhas)


def executar(benchmark_json: Path) -> dict:
    import pm4py
    execucoes = json.loads(Path(benchmark_json).read_text(
        encoding="utf-8"))["execucoes"]
    df_log = montar_log(execucoes)
    log = pm4py.convert_to_event_log(df_log)

    with tempfile.NamedTemporaryFile("w", suffix=".bpmn",
                                     delete=False, encoding="utf-8") as f:
        f.write(BPMN_PROCESSO)
        caminho_bpmn = f.name
    bpmn = pm4py.read_bpmn(caminho_bpmn)
    net, im, fm = pm4py.convert_to_petri_net(bpmn)

    fit = pm4py.fitness_token_based_replay(log, net, im, fm)
    try:
        aligns = pm4py.conformance_diagnostics_alignments(log, net, im, fm)
        fits = [a.get("fitness", 0.0) for a in aligns]
        alinhamento = {
            "disponivel": True,
            "fitness_medio": round(sum(fits) / max(len(fits), 1), 4),
            "traces_com_desvio": int(sum(1 for f in fits if f < 1.0)),
        }
    except Exception as exc:
        alinhamento = {"disponivel": False, "erro": str(exc)[:200]}
    resumo = {
        "traces_avaliados": int(df_log["case:concept:name"].nunique()),
        "fitness_medio": round(float(fit.get("average_trace_fitness",
                                             fit.get("log_fitness", 0))), 4),
        "pct_traces_aderentes": round(float(fit.get(
            "percentage_of_fitting_traces", 0)), 1),
        "alignments": alinhamento,
        "detalhe": {k: (round(float(v), 4) if isinstance(v, (int, float))
                        else v) for k, v in fit.items()},
    }
    Path("resultados").mkdir(exist_ok=True)
    Path("resultados/conformidade.json").write_text(
        json.dumps(resumo, ensure_ascii=False, indent=2), encoding="utf-8")
    return resumo


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--benchmark", default="resultados/benchmark.json")
    args = ap.parse_args()
    r = executar(Path(args.benchmark))
    print(json.dumps(r, ensure_ascii=False, indent=2))
    print("\nDetalhes em resultados/conformidade.json")
