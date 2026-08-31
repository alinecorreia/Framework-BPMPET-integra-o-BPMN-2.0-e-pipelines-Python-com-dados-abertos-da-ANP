# BPMPET

[![testes](https://github.com/alinecorreia/Framework-BPMPET-integra-o-BPMN-2.0-e-pipelines-Python-com-dados-abertos-da-ANP/actions/workflows/testes.yml/badge.svg)](https://github.com/alinecorreia/Framework-BPMPET-integra-o-BPMN-2.0-e-pipelines-Python-com-dados-abertos-da-ANP/actions/workflows/testes.yml)

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

A suite de testes roda automaticamente a cada push, pela configuracao em
`.github/workflows/testes.yml`; o selo acima reflete o estado da ultima execucao.
Para rodar localmente:

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

Os critérios abaixo foram fixados antes da execução, para que os números
pudessem ser lidos como aprovação ou reprovação, e não apenas como descrição.

| Métrica | O que mede | Critério de sucesso | Módulo |
|---|---|---|---|
| M1 | Volumetria: arquivos, registros, volume transferido | 12 competências sem perda por falha de ingestão | `benchmark_zip.py` |
| M2 | Desempenho: tempo por etapa e throughput | menos de 60 s por competência | `benchmark_zip.py` |
| M3 | Memória: pico de alocação do interpretador | menos de 2 GB | `benchmark_zip.py` |
| M4 | Qualidade: aprovação e inventário de inconsistências | 100% aprovadas após tratamento | `benchmark_zip.py` |
| M5 | Idempotência: hashes SHA-256 entre reexecuções | identidade em 100% das reexecuções | `benchmark_zip.py` |
| M6 | Diagnóstico: injeção de falhas, com e sem a matriz | ao menos 80% de localização correta | `experimento_diagnostico.py` |
| M7 | Conformidade: replay e alignments contra o modelo | fitness maior ou igual a 0,95, sem desvio inexplicado | `conformidade.py` |

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
  benchmark_zip.py            protocolo M1 a M5, com os critérios de sucesso
  conformidade.py             protocolo M7, via PM4Py
  experimento_diagnostico.py  protocolo M6, injeção de falhas
tests/                        testes das propriedades declaradas
resultados/                   evidências de execução em JSON
matriz_rastreabilidade.csv    elemento BPMN ↔ componente Python
monitoramento_anp.bpmn        modelo do processo
```

## Sobre os dados da fonte

Duas particularidades da publicação da ANP condicionaram o código e estão
tratadas no pipeline.

A agência insere linhas de metadados no topo de cada arquivo interno, e a
concatenação dos três ambientes as multiplica: seis registros por competência,
72 na série de 2022, suficientes para reprovar todas as doze na validação de
schema. São suprimidas por `limpar_rodape_cabecalho()`, com a contagem
registrada como evidência auditável.

A segunda particularidade é mais discreta. O dialeto numérico alterna no meio
do ano, passando do padrão brasileiro ao anglófono a partir de julho, o que
afeta seis das doze competências. Assumir um único formato produz, sobre o
outro, valores inflados em ordens de grandeza sem lançar exceção alguma, e por
isso a coerção infere o dialeto valor a valor, e não por arquivo. O caso está
coberto por teste de regressão em `tests/`.

## Licença e uso

Trabalho acadêmico. Os dados são públicos, publicados pela ANP no Portal de
Dados Abertos do governo federal.

