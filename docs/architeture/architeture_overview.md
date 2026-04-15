# Architecture Overview — Retail Lakehouse on AWS

## 1. Contexto

O projeto **Retail Lakehouse on AWS** foi desenhado para simular uma arquitetura moderna de engenharia de dados aplicada a analytics de varejo digital.

O cenário de negócio parte de dados transacionais de compras, produtos e estrutura comercial, com foco em responder perguntas analíticas sobre comportamento de compra, recorrência, categorias, departamentos e padrões temporais.

O projeto foi concebido com dois objetivos centrais:

1. **objetivo técnico**: demonstrar domínio de uma arquitetura lakehouse moderna na AWS
2. **objetivo de portfólio**: construir um projeto forte, explicável em entrevistas e alinhado com stack real de mercado

---

## 2. Objetivo técnico

Construir um pipeline ponta a ponta com:

- ingestão automatizada
- separação por camadas
- armazenamento central no S3
- processamento com PySpark
- tabelas Iceberg no data lake
- catálogo no AWS Glue Data Catalog
- consultas via Athena
- camada analítica final no Redshift Serverless
- modelagem com dbt Core
- visualização no Looker Studio
- orquestração com Apache Airflow
- ambiente local reproduzível com Docker

---

## 3. Objetivo de negócio

Transformar dados brutos de varejo em uma base analítica capaz de responder perguntas como:

- quais produtos possuem maior recorrência
- quais departamentos apresentam maior concentração de pedidos
- quais padrões temporais de compra existem
- como medir recompra e frequência de clientes
- como a cesta de compra varia ao longo do tempo
- quais datasets finais são mais adequados para consumo em BI

---

## 4. Dataset escolhido

O dataset principal do projeto é o **Instacart Market Basket Analysis**.

Ele foi escolhido por apresentar:

- volume suficiente para simular pipeline real
- múltiplas entidades relacionadas
- bom potencial para modelagem dimensional
- forte aderência a storytelling de varejo digital
- possibilidade de gerar métricas úteis para dashboards executivos

Entidades esperadas no dataset:

- orders
- order_products__prior
- order_products__train
- products
- aisles
- departments

---

## 5. Princípios arquiteturais

Durante a construção do projeto, as decisões seguirão estes princípios:

### 5.1 Clareza antes de sofisticação
Primeiro o pipeline deve funcionar corretamente de ponta a ponta.  
Depois entram refinamentos de performance, governança e automação.

### 5.2 Separação explícita de responsabilidades
Cada ferramenta deve ter um papel claro, evitando acoplamento desnecessário.

### 5.3 Camadas bem definidas
O projeto será organizado em **raw**, **bronze**, **silver** e **gold**, com responsabilidades distintas.

### 5.4 Baixo custo operacional
A arquitetura será desenhada para maximizar aprendizado e valor de portfólio com o menor custo possível.

### 5.5 Reprodutibilidade
O ambiente local deve ser reproduzível via Docker, sem depender de instalação manual dispersa.

---

## 6. Arquitetura lógica ponta a ponta

```text
Fonte de dados (Instacart CSVs)
        |
        v
Python Ingestion
        |
        v
S3 Raw
        |
        v
PySpark Bronze
        |
        v
S3 Bronze (Parquet)
        |
        v
PySpark Silver
        |
        v
S3 Silver (Iceberg)
        |
        +-----------------------> Glue Data Catalog
        |                                |
        |                                v
        |                           Athena
        |
        v
Gold / curated analytical datasets
        |
        +-----------------------> Athena
        |
        v
Redshift Serverless
        |
        v
dbt Core
        |
        v
Looker Studio