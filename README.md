# BPMPET

Framework de integração entre modelagem de processos em **BPMN 2.0** e
**pipelines de dados em Python**, aplicado ao monitoramento da produção de
petróleo e gás natural com dados abertos da ANP.

Parte prática do trabalho de conclusão do curso de Pós-Graduação em Engenharia
de Dados do ITLab/POLI/UFRJ.

## A ideia

Diagramas de processo costumam virar documentação estática enquanto o código
segue outro caminho, e ninguém consegue demonstrar que a execução faz o que o
modelo especifica. Aqui cada elemento do diagrama tem um componente
correspondente no pipeline, o vínculo fica registrado numa matriz que funciona
nos dois sentidos, e a aderência entre o especificado e o executado é medida,
não afirmada.

## Como reproduzir

```bash
pip install -r requirements.txt
pytest tests/ -q

export PYTHONPATH=src
python -m bpmpet.benchmark_zip --config config_fontes.json     # M1 a M5
python -m bpmpet.conformidade --benchmark resultados/benchmark.json   # M7
python -m bpmpet.experimento_diagnostico --csv dados/ARQUIVO.csv      # M6
```

Os arquivos da ANP são baixados em tempo de execução a partir das URLs em
`config_fontes.json`. Nenhum dado precisa ser obtido por outro meio.

## Protocolo de avaliação

| Métrica | O que mede | Módulo |
|---|---|---|
| M1 | Volumetria: arquivos, registros, volume transferido | `benchmark_zip.py` |
| M2 | Desempenho: tempo por etapa e throughput | `benchmark_zip.py` |
| M3 | Memória: pico de alocação do interpretador | `benchmark_zip.py` |
| M4 | Qualidade: aprovação na validação e taxonomia de falhas | `benchmark_zip.py` |
| M5 | Idempotência: hashes SHA-256 entre reexecuções | `benchmark_zip.py` |
| M6 | Diagnóstico: injeção de falhas, com e sem a matriz | `experimento_diagnostico.py` |
| M7 | Conformidade: replay e alignments contra o modelo | `conformidade.py` |

Resultados medidos sobre as doze competências de 2022, nas bacias de Santos,
Campos e Solimões: 123.106 registros brutos e 16.928 no escopo, 100% das
competências aprovadas na validação, idempotência confirmada, fitness de
conformidade 1,0 sem desvios nos alignments, e 100% de localização correta no
experimento de diagnóstico, com 1 artefato inspecionado contra 2,4 sem o
mecanismo. As saídas brutas estão em `resultados/`.

## Estrutura

```
src/bpmpet/
  pipeline.py                 tarefas PIP.T1 a PIP.T5, com os identificadores
                              BPMN nos docstrings
  fonte_zip.py                leitura dos pacotes mensais da ANP
  lineage.py                  linhagem operacional no modelo OpenLineage,
                              com facet de coluna em T3 e T4
  log_excecoes.py             log estruturado por elemento de processo
  benchmark_zip.py            protocolo M1 a M5
  conformidade.py             protocolo M7, via PM4Py
  experimento_diagnostico.py  protocolo M6, injeção de falhas
  motor_spark.py              comparativo entre motores (não executado)
tests/                        testes das propriedades declaradas
dags/                         correspondência BPMN → Airflow (demonstração)
resultados/                   evidências de execução em JSON
matriz_rastreabilidade.csv    elemento BPMN ↔ componente Python ↔ task do DAG
monitoramento_anp.bpmn        modelo do processo
```

## Sobre os dados da fonte

Duas particularidades da publicação da ANP condicionaram o código e estão
tratadas no pipeline. A primeira são linhas de metadados que a agência insere
no topo de cada arquivo interno, suprimidas por `limpar_rodape_cabecalho()`
com a contagem registrada como evidência. A segunda é a alternância do dialeto
numérico ao longo da série, que passa do padrão brasileiro ao anglófono no
meio de 2022: a coerção infere o dialeto valor a valor, e não por arquivo,
porque assumir um único formato produz valores inflados em ordens de grandeza
sem lançar exceção alguma.

## Aviso sobre módulos não executados

`motor_spark.py` requer PySpark com JVM e o DAG requer ambiente Airflow.
Nenhum dos dois foi executado na avaliação, e nenhum número deles é reportado
na monografia.

## Licença e uso

Trabalho acadêmico. Os dados são públicos, publicados pela ANP no Portal de
Dados Abertos do governo federal.
