# Retail Lakehouse on AWS

**Airflow, PySpark, Iceberg, Athena, Redshift e dbt para analytics de varejo digital**

## Visão geral

O **Retail Lakehouse on AWS** é um projeto de engenharia de dados ponta a ponta construído para simular uma arquitetura moderna de dados utilizada em ambientes corporativos.

O projeto tem como objetivo processar, organizar, catalogar e disponibilizar dados de varejo digital em uma arquitetura **Lakehouse**, utilizando ferramentas amplamente presentes no mercado, com foco em boas práticas de engenharia, baixo custo operacional e forte valor para portfólio.

## Objetivos do projeto

### Objetivo técnico

Construir um pipeline moderno de dados com:

- ingestão automatizada
- processamento em camadas
- armazenamento central em S3
- tabelas analíticas com Apache Iceberg
- catálogo no AWS Glue Data Catalog
- consultas serverless no Athena
- serving analítico no Redshift Serverless
- modelagem com dbt Core
- visualização final com Looker Studio
- orquestração com Apache Airflow
- processamento com PySpark
- ambiente reproduzível com Docker

### Objetivo de negócio

Analisar o comportamento de compra em um contexto de varejo digital, respondendo perguntas como:

- quais produtos são mais recorrentes
- quais departamentos concentram maior volume
- como os clientes recompõem suas cestas ao longo do tempo
- quais padrões temporais de compra existem
- como medir recorrência, frequência e retenção de clientes

## Dataset

O projeto utilizará o dataset **Instacart Market Basket Analysis**, que contém informações de pedidos, produtos, departamentos e corredores de supermercado digital.

Esse dataset foi escolhido por permitir modelagem analítica rica em comportamento de compra, recorrência e estrutura de varejo.

## Arquitetura macro

```text
Fonte de dados (Instacart)
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