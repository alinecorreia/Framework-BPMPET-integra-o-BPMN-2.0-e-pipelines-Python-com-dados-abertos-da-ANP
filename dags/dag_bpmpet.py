# -*- coding: utf-8 -*-
"""Materializacao da correspondencia BPMN -> orquestrador.

Cada elemento do diagrama tem uma task equivalente neste DAG, e a
coluna dag_task da matriz de rastreabilidade registra esse vinculo:
tarefas de servico viram tasks, o evento temporizado vira o schedule
mensal e os gateways exclusivos viram ramificacoes condicionais.

Demonstracao de viabilidade. Nao foi colocado em operacao agendada;
a monografia registra essa passagem entre os trabalhos futuros.
"""
from __future__ import annotations

try:
    import pendulum
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import BranchPythonOperator, PythonOperator
    AIRFLOW_DISPONIVEL = True
except ImportError:
    # Sem Airflow instalado o arquivo permanece legivel como documentacao.
    AIRFLOW_DISPONIVEL = False


if AIRFLOW_DISPONIVEL:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

    def _ingerir(**contexto):
        """[PIP.T1] Baixa o pacote da competencia e extrai os CSVs."""
        raise NotImplementedError("ligar a bpmpet.fonte_zip.baixar_zip")

    def _arquivo_disponivel(**contexto):
        """[PIP.GW1] Decide entre seguir para a validacao ou registrar erro."""
        return "task_validar"

    def _validar(**contexto):
        """[PIP.T2] Resolve colunas e confere chaves obrigatorias."""
        raise NotImplementedError("ligar a bpmpet.pipeline.validar_schema")

    def _dados_validos(**contexto):
        """[PIP.GW2] Decide entre transformar ou registrar erro."""
        return "task_transformar"

    with DAG(
        dag_id="bpmpet_monitoramento_anp",
        description="Monitoramento mensal da producao ANP (framework BPMPET)",
        schedule="@monthly",                      # [ANP.EV.01]
        start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
        catchup=False,
        tags=["bpmpet", "anp", "bpmn"],
    ) as dag:

        task_ingestao = PythonOperator(
            task_id="task_ingestao", python_callable=_ingerir)
        branch_arquivo = BranchPythonOperator(
            task_id="branch_arquivo_disponivel",
            python_callable=_arquivo_disponivel)
        task_erro_ingestao = EmptyOperator(
            task_id="task_registrar_erro_ingestao")

        task_validar = PythonOperator(
            task_id="task_validar", python_callable=_validar)
        branch_validos = BranchPythonOperator(
            task_id="branch_dados_validos", python_callable=_dados_validos)
        task_erro_validacao = EmptyOperator(
            task_id="task_registrar_erro_validacao")

        task_transformar = EmptyOperator(task_id="task_transformar")
        task_kpis = EmptyOperator(task_id="task_kpis")
        task_relatorio = EmptyOperator(task_id="task_relatorio")

        task_ingestao >> branch_arquivo >> [task_validar, task_erro_ingestao]
        task_validar >> branch_validos >> [task_transformar, task_erro_validacao]
        task_transformar >> task_kpis >> task_relatorio
