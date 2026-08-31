# BPMPET

[![testes](https://github.com/alinecorreia/Framework-BPMPET-integra-o-BPMN-2.0-e-pipelines-Python-com-dados-abertos-da-ANP/actions/workflows/testes.yml/badge.svg)](https://github.com/alinecorreia/Framework-BPMPET-integra-o-BPMN-2.0-e-pipelines-Python-com-dados-abertos-da-ANP/actions/workflows/testes.yml)

Framework de integração entre modelagem de processos em **BPMN 2.0** e
**pipelines de dados em Python**, aplicado ao monitoramento da produção de
petróleo e gás natural com dados abertos da ANP.

Parte prática da monografia do MBA em Engenharia de Dados da Escola
Politécnica da UFRJ.

## O problema que este código resolve

Diagramas de processo costumam virar documentação estática enquanto o código
segue outro caminho, até que ninguém mais consegue demonstrar que a execução
faz o que o modelo especifica. Aqui cada elemento do diagrama tem um componente
correspondente no pipeline, o vínculo fica registrado numa matriz que funciona
nos dois sentidos, e a aderência entre o especificado e o executado é submetida
a medição.

## Como rodar

```bash
pip install -r requirements.txt
pytest tests/ -q

export PYTHONPATH=src
python -m bpmpet.benchmark_zip --config config_fontes.json                # M1 a M5
python -m bpmpet.conformidade --benchmark resultados/benchmark.json       # M7
python -m bpmpet.experimento_diagnostico --csv dados/ARQUIVO.csv          # M6
```

O benchmark baixa os doze pacotes mensais direto do portal da ANP a partir das
URLs em `config_fontes.json` e leva cerca de 25 segundos numa máquina modesta.
Nenhum dado precisa ser obtido por outro meio, e nada além do `requirements.txt`
precisa estar instalado.

A suíte de testes roda offline, sobre fixture local, e é executada
automaticamente a cada push pela configuração em `.github/workflows/testes.yml`.
O selo acima mostra o resultado da última execução.

## Protocolo de avaliação

Os critérios abaixo foram fixados antes de rodar qualquer coisa, para que os
números pudessem ser lidos como aprovação ou reprovação em vez de simples
descrição.

| Métrica | O que mede | Critério de sucesso | Módulo |
|---|---|---|---|
| M1 | Volumetria: arquivos, registros, volume transferido | 12 competências sem perda por falha de ingestão | `benchmark_zip.py` |
| M2 | Desempenho: tempo por etapa e throughput | menos de 60 s por competência | `benchmark_zip.py` |
| M3 | Memória: pico de alocação do interpretador | menos de 2 GB | `benchmark_zip.py` |
| M4 | Qualidade: aprovação e inventário de inconsistências | 100% aprovadas após tratamento | `benchmark_zip.py` |
| M5 | Idempotência: hashes SHA-256 entre reexecuções | identidade em 100% das reexecuções | `benchmark_zip.py` |
| M6 | Diagnóstico: injeção de falhas, com e sem a matriz | ao menos 80% de localização correta | `experimento_diagnostico.py` |
| M7 | Conformidade: replay e alignments contra o modelo | fitness maior ou igual a 0,95, sem desvio inexplicado | `conformidade.py` |

Todos foram atendidos na execução sobre as doze competências de 2022, nas
bacias de Santos, Campos e Solimões. Foram 123.106 registros brutos, 16.928
dentro do escopo, aprovação integral na validação, fitness de conformidade 1,0
sem desvios nos alignments, e 100% de acerto no experimento de diagnóstico,
inspecionando 1 artefato por falha contra 2,4 quando o mecanismo é desligado.
As saídas brutas ficam em `resultados/`.

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
tests/                        idempotência, escopo, dialeto numérico e linhagem
resultados/                   evidências de execução em JSON
matriz_rastreabilidade.csv    elemento BPMN ↔ componente Python
monitoramento_anp.bpmn        modelo do processo
```

## Armadilhas dos arquivos da ANP

Quem for reprocessar essa fonte vai esbarrar em duas coisas que já estão
tratadas aqui. A agência insere linhas de metadados no topo de cada arquivo interno, e a
concatenação dos três ambientes as multiplica. São seis registros por
competência, 72 na série de 2022, o bastante para reprovar todas as doze na
validação de schema. Ficam a cargo de `limpar_rodape_cabecalho()`, que registra
a contagem como evidência auditável.

A segunda é mais discreta, o dialeto numérico alterna no meio do ano. Até junho os volumes 
vêm no padrão brasileiro, com vírgula decimal, e de julho em diante no padrão anglófono, com ponto. 
Seis das doze competências são afetadas. Um conversor que assuma um único formato produz,
sobre o outro, valores inflados em ordens de grandeza sem lançar exceção
alguma, então a coerção precisa inferir o dialeto valor a valor. O caso virou
teste de regressão em `tests/`.

## Licença e uso

Trabalho acadêmico. Os dados são públicos, publicados pela ANP no Portal de
Dados Abertos do governo federal.
