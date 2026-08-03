# -*- coding: utf-8 -*-
"""Metrica M8 - Comparativo de motores (Pandas x PySpark, modo local).

Executa as mesmas transformacoes e agregacoes sobre a mesma amostra nos
dois motores e reporta tempo de parede, linhas processadas e throughput.
A memoria e reportada apenas para o Pandas (tracemalloc); no Spark, o
gerenciamento ocorre na JVM e nao e comparavel pela mesma medida.

Requisito local: pip install pyspark (JVM instalada).
AVISO DE VALIDACAO: este modulo NAO foi executado no ambiente de
empacotamento (sem JVM); rode-o integralmente na sua maquina e confira a
saida antes de transcrever qualquer numero para o artigo.

Uso:
    PYTHONPATH=src python -m bpmpet.motor_spark --csv dados/2023-01_mar.csv
"""
from __future__ import annotations

import argparse
import time
import tracemalloc
from pathlib import Path

from .experimento_diagnostico import _ler_csv_tolerante
from .pipeline import validar_schema, transformar, calcular_kpis, ALIASES, _snake


def _rodada_pandas(caminho: Path) -> dict:
    tracemalloc.start()
    t0 = time.perf_counter()
    df = _ler_csv_tolerante(caminho)
    val = validar_schema(df)
    tratado = transformar(df, val.colunas_resolvidas, "M8")
    kpis = calcular_kpis(tratado, "M8")
    dt = time.perf_counter() - t0
    _, pico = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {"motor": "pandas", "linhas": len(df), "grupos": len(kpis),
            "tempo_s": round(dt, 3), "pico_mem_mb": round(pico / 1e6, 1),
            "throughput_l_s": round(len(df) / max(dt, 1e-9), 1)}


def _rodada_spark(caminho: Path) -> dict:
    from pyspark.sql import SparkSession, functions as F
    spark = (SparkSession.builder.master("local[*]")
             .appName("bpmpet_m8").getOrCreate())
    t0 = time.perf_counter()
    df = (spark.read.option("sep", ";").option("header", True)
          .option("encoding", "UTF-8").csv(str(caminho)))
    mapa = {}
    for c in df.columns:
        s = _snake(c)
        for canon, apelidos in ALIASES.items():
            if s in apelidos:
                mapa[c] = canon
    for orig, canon in mapa.items():
        df = df.withColumnRenamed(orig, canon)
    df = df.filter(F.upper(F.col("bacia")).rlike("SANTOS|CAMPOS|SOLIM"))
    for c in ("petroleo_bbl_dia", "gas_mm3_dia"):
        df = df.withColumn(
            c, F.regexp_replace(F.col(c), ",", ".").cast("double"))
    agg = (df.groupBy("bacia", "operador")
             .agg(F.sum("petroleo_bbl_dia").alias("petroleo_bbl_dia"),
                  F.sum("gas_mm3_dia").alias("gas_mm3_dia"),
                  F.countDistinct("poco").alias("pocos")))
    linhas, grupos = df.count(), agg.count()
    dt = time.perf_counter() - t0
    spark.stop()
    return {"motor": "pyspark_local", "linhas": linhas, "grupos": grupos,
            "tempo_s": round(dt, 3), "pico_mem_mb": None,
            "throughput_l_s": round(linhas / max(dt, 1e-9), 1)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    args = ap.parse_args()
    rp = _rodada_pandas(Path(args.csv))
    rs = _rodada_spark(Path(args.csv))
    print("| Motor | Linhas | Tempo (s) | Throughput (l/s) | Pico mem (MB) |")
    print("|---|---|---|---|---|")
    for r in (rp, rs):
        print(f"| {r['motor']} | {r['linhas']} | {r['tempo_s']} | "
              f"{r['throughput_l_s']} | {r['pico_mem_mb']} |")
