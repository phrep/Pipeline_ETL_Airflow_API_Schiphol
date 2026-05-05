# ✈️ Pipeline ETL Distribuído com Apache Airflow, Celery e API Schiphol

Projeto de Engenharia de Dados que implementa um pipeline **ETL distribuído e escalável** para ingestão de dados da API do Aeroporto de Schiphol, utilizando **Apache Airflow com CeleryExecutor**, mensageria com Redis e armazenamento em PostgreSQL.

---

## 🎯 Objetivo

Construir um pipeline robusto capaz de:

* Extrair dados de uma API REST real
* Processar e transformar dados em múltiplas entidades
* Orquestrar workflows com dependências complexas
* Executar tarefas de forma distribuída
* Persistir dados estruturados para análise

---

## 🧠 Contexto (Storytelling)

Em cenários reais de engenharia de dados, pipelines precisam lidar com:

* APIs paginadas
* múltiplas entidades interdependentes
* necessidade de paralelismo
* controle de falhas e reprocessamento

Este projeto simula esse cenário ao integrar dados de voos, companhias aéreas, destinos e aeronaves da API de Schiphol em um ambiente orquestrado.

---

## 🏗️ Arquitetura

O projeto utiliza uma arquitetura distribuída baseada em Airflow + Celery:

* **Airflow Scheduler** → Agenda e gerencia execuções
* **Airflow Webserver** → Interface de monitoramento
* **Celery Workers** → Execução paralela das tarefas
* **Redis** → Broker de mensagens (fila de tarefas)
* **PostgreSQL** → Armazenamento de dados e metadados
* **DBeaver** → Interface para exploração e validação dos dados

### 🔄 Fluxo de Execução

1. O Scheduler dispara as DAGs
2. As tarefas são enviadas para o Redis
3. Workers Celery consomem e executam as tarefas
4. Os dados são transformados e carregados no PostgreSQL
5. O Airflow monitora status, retries e dependências

---

## 📂 Estrutura do Projeto

```bash
.
├── dags/schiphol/
│   ├── flights.py
│   ├── airlines.py
│   ├── destinations.py
│   ├── aircrafttypes.py
│   ├── *_help.py
│   ├── airflow_database.py
│   ├── utils/
│   │   └── pagination.py
│   └── sql/
│       ├── create_table_*.sql
├── docker-compose.yaml
├── Dockerfile
├── requirements/
├── scripts/
└── .devcontainer/
```

---

## ⚙️ Pipeline ETL

### 🔹 Extract

* Consumo de API REST (Schiphol)
* Tratamento de paginação
* Coleta de múltiplas entidades

### 🔹 Transform

* Normalização de dados
* Tratamento de inconsistências
* Preparação para modelo relacional

### 🔹 Load

* Criação automatizada de tabelas SQL
* Inserção estruturada no PostgreSQL

---

## 🚀 Tecnologias Utilizadas

* Python
* Apache Airflow
* Celery Executor
* Redis
* PostgreSQL
* Docker & Docker Compose
* SQL
* DBeaver

---

## 📊 Diferenciais Técnicos

✔️ Execução distribuída com Celery
✔️ Arquitetura próxima de ambiente produtivo
✔️ Modularização por domínio (flights, airlines, etc.)
✔️ Uso de SQL versionado
✔️ Tratamento de paginação de API
✔️ Separação clara entre camadas ETL

---

## 📈 Impacto (Simulação de Cenário Real)

Este projeto demonstra capacidade de:

* Construir pipelines escaláveis
* Trabalhar com dados externos (API)
* Orquestrar workflows complexos
* Implementar processamento paralelo
* Estruturar dados para analytics

---

## 🐳 Como Executar

```bash
git clone https://github.com/phrep/Pipeline_ETL_Airflow_API_Schiphol.git
cd Pipeline_ETL_Airflow_API_Schiphol
docker compose up -d
```

Acesse:

```
http://localhost:8080
```

---

## 🔐 Boas Práticas Aplicadas

* Uso de `.gitignore` para evitar vazamento de credenciais
* Estrutura modular e organizada
* Separação de responsabilidades
* Pipeline reproduzível via Docker

---
