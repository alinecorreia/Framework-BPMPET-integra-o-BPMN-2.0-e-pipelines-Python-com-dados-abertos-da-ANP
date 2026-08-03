# -*- coding: utf-8 -*-
"""[PIP.T1B] / [PIP.T2B] Registro estruturado de excecoes (observabilidade).

O log JSONL nao e apenas registro de erro: cada evento carrega o task_id
BPMN de origem, o que permite (i) localizar a etapa da falha via matriz de
rastreabilidade e (ii) monitorar a estabilidade da fonte ao longo do tempo
(ex.: competencias em que a ANP alterou o layout dos arquivos).
"""
from __future__ import annotations
import json, time
from pathlib import Path

PADRAO = Path("resultados/log_excecoes.jsonl")

def registrar_erro(task_id: str, competencia: str, tipo: str, mensagem: str,
                   caminho: Path | None = None) -> None:
    caminho = Path(caminho) if caminho else PADRAO
    caminho.parent.mkdir(parents=True, exist_ok=True)
    evento = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S"), "task_id": task_id,
              "competencia": competencia, "tipo": tipo, "mensagem": mensagem[:500]}
    with caminho.open("a", encoding="utf-8") as f:
        f.write(json.dumps(evento, ensure_ascii=False) + "\n")
