# FraudShield

![Status](https://img.shields.io/badge/status-em%20desenvolvimento-f0ad4e)
![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?logo=apachekafka&logoColor=white)

## Sobre o projeto

O **FraudShield** é uma junção de engenharia de dados e machine learning simulando
um sistema de detecção de fraudes em streaming para que o modelo detecte em tempo real dados fraudulentos e utilizando como inspiração **Alexandria** uma plataforma completa que permite auditoria, observalidade e integridade dos logs feita pelo time de engenharia da __Nubank__.

## Objetivo

Construir uma solução reproduzível que seja capaz de:

- preparar e validar dados da competição **IEEE-CIS Fraud Detection**;
- treinar e comparar um baseline com um modelo XGBoost;
- avaliar o modelo em períodos temporais separados, evitando vazamento de dados;
- publicar transações no Kafka e processá-las com um serviço de scoring;
- classificar cada transação como `APPROVE`, `REVIEW` ou `BLOCK`;
- persistir transações e decisões em arquivos Parquet de forma idempotente;
- coletar e processar logs estruturados em um fluxo inspirado no padrão Alexandria;
- acompanhar latência, throughput, filas e erros por meio de dashboards;
- testar reentrega, recuperação de falhas, DLQ e ausência de duplicatas lógicas.

## Arquitetura proposta

O diagrama apresenta a arquitetura-alvo ao final dos marcos. No primeiro fluxo
de streaming, o sink gravará Parquet localmente; a integração com o SeaweedFS
será acrescentada junto ao pipeline de armazenamento de objetos.

```mermaid
flowchart LR
    D[(IEEE-CIS)] --> P[Preparação com DuckDB]
    P --> T[Treino e validação]
    T --> M[(Modelo e schema)]
    P --> R[Replay de transações]
    R --> K[Kafka<br/>transactions.raw]
    K --> S[Scoring XGBoost]
    M --> S
    S --> F[Kafka<br/>fraud.decisions]
    K --> Q[Sink Parquet]
    F --> Q
    Q --> O[(SeaweedFS)]

    A[Aplicações] -. logs JSON .-> FB[Fluent Bit]
    FB --> AC[Acumulador]
    AC --> LR[(SeaweedFS<br/>logs-raw)]
    AC --> E[ElasticMQ]
    LR --> W[Worker de logs]
    E --> W
    W --> LC[(SeaweedFS<br/>logs-curated)]

    A -. métricas .-> PR[Prometheus]
    PR --> G[Grafana]
    O --> DB[DuckDB]
    LC --> DB
```

O fluxo adota processamento com reentrega e efeitos idempotentes. Os offsets ou
mensagens só deverão ser confirmados depois da persistência correspondente. No
pipeline de logs, uma rotina de reconciliação recuperará objetos gravados que
eventualmente não tenham sido publicados na fila após uma interrupção.

## Tecnologias

| Área | Tecnologias | Responsabilidade |
| --- | --- | --- |
| Linguagem | Python 3.11 ou 3.12 | Preparação, treinamento e serviços da aplicação |
| Machine learning | scikit-learn e XGBoost | Pré-processamento, baseline, treino e inferência |
| Dados analíticos | DuckDB e Apache Parquet | Recortes, consultas e persistência colunar |
| Streaming | Apache Kafka em modo KRaft | Transporte de transações e decisões |
| Armazenamento | SeaweedFS com API compatível com S3 | Objetos de transações, decisões e logs |
| Filas | ElasticMQ com API compatível com SQS | Notificações, retentativas e DLQ de logs |
| Coleta de logs | Fluent Bit | Encaminhamento de logs JSON dos serviços |
| Observabilidade | Prometheus e Grafana | Métricas, alertas e dashboards |
| Infraestrutura | Docker Desktop, Docker Compose e WSL2 | Execução local e isolamento dos componentes |
| Testes | pytest | Testes unitários, de integração e de aceite |

As versões exatas das imagens e bibliotecas serão fixadas conforme cada
componente for implementado e validado.

## Dados e avaliação

O modelo utilizará os arquivos `train_transaction.csv` e
`train_identity.csv` da competição
[IEEE-CIS Fraud Detection](https://www.kaggle.com/competitions/ieee-fraud-detection/data).

Os registros serão associados por `TransactionID`, ordenados por
`TransactionDT` e divididos inicialmente da seguinte forma:

| Período | Quantidade inicial | Uso |
| --- | ---: | --- |
| Primeiro recorte | 120.000 | Treinamento |
| Segundo recorte | 30.000 | Validação e escolha dos limiares |
| Terceiro recorte | 50.000 | Teste final e replay no Kafka |

A coluna `isFraud` será mantida fora das features e dos eventos de inferência.
As métricas previstas incluem PR-AUC, precision, recall, matriz de confusão e
desempenho por janela temporal.

## Contratos principais

| Tópico Kafka | Chave | Conteúdo |
| --- | --- | --- |
| `transactions.raw` | `transaction_id` | Transação sem o rótulo `isFraud` |
| `fraud.decisions` | `transaction_id` | Score, decisão, modelo e correlação |
| `transactions.dlq` | `transaction_id` | Evento inválido ou falha irrecuperável |

Cada decisão será vinculada a um `schema_version`, `model_version`,
`correlation_id` e `run_id`. O mesmo pipeline de pré-processamento deverá ser
usado no treinamento e na inferência em streaming.

## Roadmap

- [ ] **M1 — Ambiente e dados:** validar recursos, preparar os recortes e gerar
  o relatório de qualidade.
- [ ] **M2 — Modelo:** treinar baseline e XGBoost, selecionar limiares e
  versionar o pipeline e suas métricas.
- [ ] **M3 — Streaming:** executar Kafka, replay, scoring e sink Parquet
  local com 1.000 eventos, incluindo testes de repetição e reinício.
- [ ] **M4 — Objetos e pipeline de logs:** integrar SeaweedFS e implementar
  Fluent Bit, ElasticMQ, worker, retentativas, DLQ e reconciliação.
- [ ] **M5 — Observabilidade e carga:** consolidar dashboards, consultas e o
  teste de 50 mil eventos.
- [ ] **M6 — Entrega reproduzível:** finalizar documentação, evidências,
  runbook e rotinas seguras de limpeza.

## Documentação

Os requisitos, critérios de aceite, decisões técnicas e recomendações estão no
[planejamento inicial](docs/planejamento.md).

---

