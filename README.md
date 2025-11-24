# 📊 Data Warehouse & ETL Pipeline: AdventureWorks

> **Projeto Acadêmico de Engenharia de Dados** | UniSales
> **Autor:** Mikael

---

## 📑 Resumo do Projeto
Este projeto consiste na implementação de um pipeline de dados completo (**End-to-End**) para a construção de um Data Warehouse (OLAP). O objetivo foi extrair dados transacionais do ERP **AdventureWorks** (Microsoft SQL Server), transformá-los utilizando **Python/Pandas** e carregá-los em um modelo dimensional (Star Schema) no **PostgreSQL**, utilizando o **Apache Airflow** como orquestrador.

O diferencial deste projeto foi a utilização de uma arquitetura 100% baseada em contêineres **Docker**, simulando um ambiente de produção isolado e reprodutível.

---

## 🏗️ Arquitetura da Solução

A infraestrutura foi definida como código (IaC) através do `docker-compose`, integrando três serviços principais:

| Serviço | Tecnologia | Função |
| :--- | :--- | :--- |
| **Origem (OLTP)** | Microsoft SQL Server 2019 (Linux) | Banco transacional contendo os dados brutos de vendas e compras (`AdventureWorks2019`). |
| **Orquestrador** | Apache Airflow 2.10 | Responsável pelo agendamento, execução e monitoramento das DAGs de ETL. |
| **Destino (OLAP)** | PostgreSQL 13 | Data Warehouse modelado em *Star Schema* para análises de BI. |
| **Processamento** | Python 3.12 + Pandas | Engine de transformação e limpeza de dados executada nos *Workers* do Airflow. |

---

## 💎 Modelagem Dimensional (Star Schema)

O foco analítico deste projeto foi o setor de **Compras (Purchasing)**, visando analisar a eficiência de fornecedores e custos de aquisição.

### 🔹 Tabela Fato
* **`Fato_Compras`**: Granularidade por item de pedido de compra.
    * Métricas: `Qtd_Pedida`, `Qtd_Recebida`, `Qtd_Rejeitada` (Qualidade), `Valor_Unitario`, `Frete_Rateado`.

### 🔸 Tabelas Dimensão
* **`Dim_Produto`**: Dados descritivos dos produtos, categorias e subcategorias.
* **`Dim_Fornecedor`**: Informações sobre os parceiros comerciais, localização e classificação de crédito.
* **`Dim_Tempo`**: Calendário canônico para análises temporais (Ano, Mês, Trimestre, Dia da Semana).

---

## ⚙️ O Processo ETL (Extract, Transform, Load)

O pipeline foi codificado na DAG `etl_compras_adventureworks`, seguindo as etapas:

1.  **Extraction (Extração):** Conexão ao SQL Server via driver ODBC (`mssql-tools18`) utilizando `MsSqlHook`. Leitura dos dados brutos das tabelas `Purchasing.PurchaseOrderHeader` e `Detail`.
2.  **Transformation (Transformação):**
    * Limpeza de dados e tratamento de nulos com Pandas.
    * Geração de Chaves Substitutas (Surrogate Keys) para a dimensão Tempo.
    * Cálculo de métricas derivadas (ex: Custo Total da Linha).
3.  **Loading (Carga):**
    * Inserção dos dados nas tabelas do PostgreSQL.
    * Uso de estratégia *Truncate-Insert* para garantir a consistência dos dados em ambiente de desenvolvimento.

---

## 🚧 Desafios Técnicos e Troubleshooting

Durante o desenvolvimento, o projeto superou desafios significativos relacionados à infraestrutura local:

### 1. OOM Killed (Out of Memory)
A execução simultânea do SQL Server, PostgreSQL e da stack completa do Airflow (Webserver, Scheduler, Triggerer) excedeu os recursos de hardware disponíveis no ambiente WSL2.
* **Impacto:** O container do SQL Server entrava em estado de reinicialização constante (Exit Code 137).
* **Contorno:** Foi necessário implementar estratégias de reinicialização controlada e otimização da alocação de memória no `.wslconfig`.

### 2. Drivers e Conectividade
A comunicação entre o Airflow (baseado em Debian Linux) e o SQL Server exigiu a customização da imagem Docker.
* **Solução:** Criação de um `Dockerfile` personalizado para instalação das bibliotecas de sistema `unixodbc-dev` e o driver proprietário `msodbcsql17` da Microsoft.

### 3. Instabilidade da Interface Gráfica
Devido à carga no sistema, a UI do Airflow apresentou latência.
* **Solução:** Gerenciamento das DAGs (Unpause/Trigger) e criação de conexões realizados via **Airflow CLI** diretamente no terminal do container, garantindo a execução mesmo sem acesso à interface web.

---

## 🚀 Como Executar o Projeto

Requisitos: Docker e Docker Desktop instalados.

1.  **Clonar o repositório:**
    ```bash
    git clone [https://github.com/SEU-USUARIO/etl-adventureworks-airflow.git](https://github.com/SEU-USUARIO/etl-adventureworks-airflow.git)
    ```

2.  **Adicionar o Dataset:**
    Baixe o arquivo `AdventureWorks2019.bak` e coloque na raiz do projeto (o arquivo é ignorado pelo Git devido ao tamanho).

3.  **Subir o Ambiente:**
    ```bash
    docker compose up --build -d
    ```

4.  **Restaurar o Banco de Origem:**
    Executar o script de restore via `docker exec` para popular o SQL Server containerizado.

5.  **Acessar o Airflow:**
    Navegar para `http://localhost:8080` (Login: `airflow` / `airflow`) e ativar a DAG.

---

**Nota:** Este projeto foi desenvolvido com foco acadêmico para demonstrar a proficiência em ferramentas de Engenharia de Dados.
