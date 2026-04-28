# Aero Clima — Pipeline de Dados Near Real-Time
**Data Engineering | Databricks, PySpark, Delta Lake e Power BI**

## Visão Geral
O Aero Clima é um pipeline de dados corporativo desenvolvido para monitoramento de Nível de Serviço (SLA) e risco operacional nos aeroportos de Congonhas (CGH), Guarulhos (GRU) e Porto Alegre (POA). O sistema realiza o cruzamento de dados de tráfego aéreo (OpenSky Network) com métricas meteorológicas em tempo real (OpenWeatherMap).

## Evolução Arquitetural
O projeto demonstra a maturidade de uma solução de dados que evoluiu de um ambiente de execução local para uma infraestrutura escalável em nuvem:

1. **Fase 1 (On-premises):** Ingestão baseada em scripts Python locais, utilizando bibliotecas de requisição e persistência em disco (disponíveis no diretório `/legacy_local_ingestion`).
2. **Fase 2 (Cloud Native):** Pipeline end-to-end orquestrado no Databricks, utilizando PySpark para processamento distribuído e a arquitetura Medallion para governança de dados.

## Arquitetura de Dados (Medallion)
O fluxo de processamento segue o padrão de camadas do Lakehouse:

* **Bronze:** Ingestão de payloads JSON brutos via Volumes do Unity Catalog.
* **Silver:** Limpeza de dados, aplicação de tipagem forte, normalização de estruturas complexas (unnesting) e deduplicação.
* **Gold:** Enriquecimento via geofencing, integração relacional e aplicação do motor de regras de negócio para classificação de risco operacional.
* **Serving (Star Schema):** Modelagem dimensional (Fato e Dimensões) preparada para consumo analítico de alta performance.

## Regras de Negócio e Compliance
O motor de análise avalia o risco operacional baseado em limites técnicos de vento e condições climáticas severas:
* **Risco ALTO:** Ventos acima de 9~13 m/s ou condições de tempestade, neve e tornados.
* **Risco MÉDIO:** Ventos acima de 6~9 m/s ou condições de baixa visibilidade (nevoeiro, chuva moderada).

## Stack Técnico
* **Linguagem:** Python e PySpark.
* **Ambiente:** Databricks (Runtime 13.3 LTS ou superior).
* **Armazenamento:** Delta Lake com suporte a transações ACID.
* **Segurança:** Databricks Secrets para gestão de chaves de API e tokens OAuth2.
* **Visualização:** Microsoft Power BI via Databricks SQL Warehouse (DirectQuery).

## Estrutura do Repositório
* `/legacy_local_ingestion`: Conjunto de scripts da versão inicial para execução local.
* `Aero_Clima_MVP.ipynb`: Notebook contendo o pipeline integral para ambiente Databricks.
* `.gitignore`: Configurações de segurança para exclusão de ambientes virtuais e credenciais.

## Roadmap de Otimização
- [ ] Modularização dos notebooks por domínios (Extração, Transformação e Carga).
- [ ] Otimização de I/O através da remoção de redundâncias de metadados (mergeSchema).
- [ ] Implementação de contratos de dados (Data Quality) para validação de schema na camada Gold.