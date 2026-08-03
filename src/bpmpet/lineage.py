# -*- coding: utf-8 -*-
"""Linhagem operacional de dados (por tarefa BPMN).

Distincao conceitual adotada no artigo: a matriz de rastreabilidade liga
PROCESSO a CODIGO (elemento BPMN <-> componente Python); a linhagem liga
DADOS a DADOS (o percurso de cada conjunto atraves das transformacoes).
Este modulo captura a linhagem no nivel de tarefa: para cada execucao de
[PIP.T1..T4], registra entradas, saidas, schemas e a transformacao
aplicada, em JSONL append-only, com eventos estruturados segundo o\nmodelo do padrao aberto OpenLineage (job, run, inputs/outputs e facets). Linhagem em nivel de coluna/registro e
evolucao prevista para a monografia (fundamentos em Reis; Housley, 2022).
"""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path


class LineageLogger:
    def __init__(self, caminho: Path):
        self.caminho = Path(caminho)
        self.caminho.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _schema_hash(colunas) -> str:
        base = "|".join(map(str, colunas)).encode()
        return hashlib.sha256(base).hexdigest()[:16]

    def registrar(self, task_id: str, competencia: str,
                  linhas_in: int, linhas_out: int,
                  colunas_in, colunas_out, transformacao: str,
                  colmap: dict | None = None) -> None:
        """colmap: mapeamento de linhagem em nivel de coluna
        {coluna_saida: [colunas_entrada]}, emitido como facet
        columnLineage (modelo OpenLineage) no output do evento."""
        evento = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "task_id": task_id,
            "competencia": competencia,
            "linhas_in": linhas_in,
            "linhas_out": linhas_out,
            "schema_in": self._schema_hash(colunas_in),
            "schema_out": self._schema_hash(colunas_out),
            "colunas_out": list(map(str, colunas_out))[:40],
            "transformacao": transformacao,
        }
        evento["openlineage"] = {
            "eventType": "COMPLETE", "eventTime": evento["ts"],
            "producer": "bpmpet",
            "job": {"namespace": "bpmpet", "name": task_id},
            "run": {"runId": competencia},
            "inputs": [{"namespace": "bpmpet",
                        "name": f"{competencia}:{task_id}:in",
                        "facets": {"rowCount": linhas_in,
                                   "schemaHash": evento["schema_in"]}}],
            "outputs": [{"namespace": "bpmpet",
                         "name": f"{competencia}:{task_id}:out",
                         "facets": {"rowCount": linhas_out,
                                    "schemaHash": evento["schema_out"]}}],
        }
        if colmap:
            evento["openlineage"]["outputs"][0]["facets"][
                "columnLineage"] = {k: list(v) for k, v in colmap.items()}
        with self.caminho.open("a", encoding="utf-8") as f:
            f.write(json.dumps(evento, ensure_ascii=False) + "\n")
