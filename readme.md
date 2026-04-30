# Aero Clima — Pipeline de Dados Near Real-Time

**Data Engineering | Databricks, PySpark, Delta Lake e Power BI**

## Visão Geral

O Aero Clima é um pipeline de dados desenvolvido para monitoramento de Nível de Serviço (SLA) e risco operacional em aeroportos estratégicos: Congonhas (CGH), Guarulhos (GRU) e Porto Alegre (POA).

A solução integra dados de tráfego aéreo (OpenSky Network) com métricas meteorológicas em tempo quase real (OpenWeatherMap), permitindo a geração de indicadores operacionais voltados à tomada de decisão.

O pipeline foi estruturado seguindo princípios de engenharia de dados moderna, com separação por camadas, validação de qualidade (Data Contracts) e modelagem dimensional estrita (Star Schema) orientada ao consumo analítico.

---

## Engenharia de Performance e Confiabilidade (Destaque)

O projeto passou por uma refatoração arquitetural com foco não apenas em velocidade, mas em **resiliência e idempotência**.

Foram aplicadas as seguintes melhorias:
* **Resiliência na Ingestão:** Implementação de motor HTTP com *Exponential Backoff* para lidar com oscilações de rede nas APIs externas.
* **Idempotência Real:** Geração de Surrogate Keys atreladas ao momento da coleta na origem (`timestamp_coleta`), garantindo que reprocessamentos não gerem duplicidade no Data Lake.
* **Data Contracts Orientados a Objetos:** Isolamento da lógica de qualidade de dados em classes dedicadas, bloqueando schemas inválidos antes do processamento.
* **Otimização de I/O:** Remoção do uso de `mergeSchema`, evitando overhead de metadados, e implementação de escrita em modo `append` estrito.

**Resultado:** O tempo de processamento foi reduzido de aproximadamente **165 minutos** para **menos de 2 minutos**, agora com garantia total de consistência em cenários de falha.

---

## Orquestração e Arquitetura de Dados

O pipeline segue o padrão **Medallion Architecture (Bronze, Silver, Gold)**, com orquestração via *Databricks Workflows*.

A ingestão dos domínios de dados (Voos e Clima) é executada de forma paralela, otimizando o uso de recursos computacionais e reduzindo o tempo total de processamento.

### Camadas do Lakehouse:

* **Bronze:** Ingestão de dados brutos em formato JSON com rotinas de *retry* automático. Persistência em Volumes do Unity Catalog.
* **Silver:** Padronização, tipagem forte, validação rigorosa de contrato de dados via OOP e normalização de estruturas aninhadas.
* **Gold:** Integração entre domínios, aplicação de geofencing, execução do motor de regras de negócio e consolidação das dimensões.
* **Serving (Star Schema Raiz):** Construção da tabela fato puramente baseada em métricas e Foreign Keys (FKs), garantindo altíssima performance no motor tabular do Power BI.

---

## Decisões de Arquitetura Avançada

### Verdadeiro Star Schema (Modelagem Dimensional)
A arquitetura foi refatorada para isolar strings longas (como descrições climáticas e status de SLA) em tabelas dimensionais estáticas e dinâmicas (`dim_risco`, `dim_condicao`). A `fato_operacoes` armazena exclusivamente métricas numéricas e IDs, otimizando a compressão em motores como o VertiPaq.

### Data Contract via Classe (OOP)
A validação explícita do schema não depende de condicionais soltas. Foi implementada uma classe `DataContractValidator` que simula o comportamento de frameworks de Data Quality, garantindo estabilidade no consumo e prevenindo a quebra de dashboards.

### Surrogate Key Determinística
A chave primária (`id_operacao`) é gerada via hash (SHA-256) combinando a identificação da aeronave (`icao24`) com o seu respectivo `timestamp_coleta`. Isso blinda a tabela Fato contra duplicações, independentemente de quantas vezes o job do Databricks seja re-executado.

---

## Stack Técnico

* **Linguagem:** Python (OOP e decorators) e PySpark
* **Ambiente:** Databricks (Serverless Compute)
* **Armazenamento:** Delta Lake (transações ACID)
* **Orquestração:** Databricks Workflows
* **Consumo:** Power BI via Databricks SQL Warehouse

---

## Evolução e Roadmap

* [x] **Arquitetura Medallion:** Separação em camadas Bronze, Silver e Gold
* [x] **Performance:** Redução de latência superior a 95%
* [x] **Data Quality:** Implementação de Data Contracts via Programação Orientada a Objetos
* [x] **Resiliência:** Exponential Backoff nas APIs de origem
* [x] **Modelagem Dimensional:** Construção de Star Schema puro (remoção de strings da tabela Fato)
* [ ] **Geofencing Avançado:** Implementação da fórmula de Haversine
* [ ] **Observabilidade:** Integração com alertas reais (Slack/Teams via Webhook)