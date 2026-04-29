# Aero Clima — Pipeline de Dados Near Real-Time
**Data Engineering | Databricks, PySpark, Delta Lake e Power BI**

## Visão Geral
O Aero Clima é um pipeline de dados corporativo desenvolvido para monitoramento de Nível de Serviço (SLA) e risco operacional nos aeroportos de Congonhas (CGH), Guarulhos (GRU) e Porto Alegre (POA). O sistema realiza o cruzamento de dados de tráfego aéreo (OpenSky Network) com métricas meteorológicas em tempo real (OpenWeatherMap).

## Engenharia de Performance e Otimização (Destaque)
O projeto passou por uma refatoração crítica focada em escalabilidade e redução de custos operacionais. Através da remoção do `mergeSchema` (custoso para metadados) e implementação de escrita em modo `append` estrito com tipagem forte, houve uma redução drástica na latência.

**Resultado:** O tempo de processamento caiu de **~165 minutos** para **menos de 2 minutos**.

![Gráfico de Redução de Tempo](notebooks/_images/time.png)

### Monitoramento e Estabilidade (Near Real-Time)
Após a estabilização, o pipeline mantém um regime de execução constante e saudável, operando de forma autônoma a cada 15 minutos com 100% de sucesso nas tarefas de integração e qualidade.

![Muralha Verde de Execuções](notebooks/_images/pipesucess.png)

## Orquestração e Arquitetura de Dados
O fluxo de processamento segue o padrão **Medallion (Bronze, Silver, Gold)**, orquestrado de forma paralela via *Databricks Workflows*. Isso permite que a ingestão de diferentes domínios (Voos e Clima) ocorra simultaneamente, otimizando o uso do cluster Serverless.

![Orquestração em Y](notebooks/_images/orch.png)

### Camadas do Lakehouse:
* **Bronze:** Ingestão de payloads JSON brutos via Volumes do Unity Catalog.
* **Silver:** Limpeza de dados, aplicação de tipagem forte e normalização de estruturas complexas.
* **Gold:** Cruzamento de domínios, Geofencing e aplicação do motor de regras de negócio.
* **Serving (Star Schema):** Implementação de **Data Contracts** para garantir a integridade da Tabela Fato antes do consumo pelo Power BI.

## Regras de Negócio e Compliance
O motor de análise avalia o risco operacional baseado em limites técnicos de vento e condições climáticas severas:
* **Risco ALTO:** Ventos acima de 9~13 m/s ou condições de tempestade, neve e tornados.
* **Risco MÉDIO:** Ventos acima de 6~9 m/s ou condições de baixa visibilidade (nevoeiro, chuva moderada).

## Stack Técnico
* **Linguagem:** Python e PySpark.
* **Ambiente:** Databricks (Serverless Compute).
* **Armazenamento:** Delta Lake com transações ACID.
* **Visualização:** Microsoft Power BI via Databricks SQL Warehouse.

## Estrutura do Repositório
* `/notebooks/BRONZE`: Scripts de ingestão e autenticação OAuth2.
* `/notebooks/SILVER`: Transformação e limpeza de dados.
* `/notebooks/GOLD`: Motor de regras, SLA e validação de contratos.
* `/legacy_local_ingestion`: Versão inicial para execução local (Fase 1).

## Evolução e Roadmap
- [x] **Modularização:** Notebooks divididos por camadas e domínios.
- [x] **Performance:** Otimização de I/O e redução de latência em >95%.
- [x] **Data Quality:** Validação de schema no final da camada Gold.
- [ ] **Geofencing Avançado:** Implementação da fórmula de Haversine para precisão de distância.
- [ ] **Monitoramento:** Alertas automáticos via Webhook (Slack/Teams).