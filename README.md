# Desafio de Data Eng

## Descrição
Este projeto é uma solução para um teste técnico focado em engenharia de dados, onde o objetivo é demonstrar habilidades em extração, transformação e carregamento (ETL) de dados para um banco de dados PostgreSQL. 
O script automatiza a ingestão de dados de arquivos JSON para o banco de dados, para proporcionar uma visualização dos dados através do Metabase (ferramenta de Data Visualization).

## Funcionalidades
Este script oferece várias funcionalidades chave para a manipulação eficiente de dados e automação de processos:

- **Leitura e Processamento de Dados de Arquivo JSON**: O script lê dados de arquivo JSON, aplicando processamento inicial para preparar os dados para inserção no banco de dados.

- **Remoção de Dados Duplicados**: Antes da inserção, o script verifica e remove quaisquer dados duplicados dentro dos arquivos JSON. Isso assegura que apenas dados únicos sejam inseridos no banco de dados.

- **Carga de Dados em Delta**: O script implementa uma lógica de carga em delta, o que significa que apenas os novos registros são inseridos no banco de dados. Isso reduz o volume de dados transferidos e processados, aumentando a eficiência.

- **Scheduler Automático**: Utiliza a biblioteca `schedule` para automatizar a execução do script em intervalos regulares (por exemplo, a cada minuto). Isso garante que os dados sejam atualizados regularmente sem intervenção manual.

- **Inserção de Dados no Banco de Dados PostgreSQL**: Após o processamento, os dados são inseridos em um banco de dados PostgreSQL, onde podem ser utilizados para análises e relatórios.

## Tecnologias Utilizadas
- Python 3.8
- PostgreSQL
- Bibliotecas Python: psycopg2, pandas, json, logging, schedule, time

### Pré-requisitos
- Python 3.8 ou superior
- PostgreSQL instalado e em execução
- Tabela tb_cliente, criada e estruturada especificamente para acomodar os dados provenientes do arquivo JSON utilizado neste projeto (json_for_case).
- Acesso à Internet para instalação de pacotes

## Configuração do Docker
Este projeto utiliza Docker para executar o PostgreSQL e o Metabase de forma isolada e configurável. Siga os passos abaixo para configurar e executar os contêineres:

1. **PostgreSQL**:
docker run --name postgres01 -e POSTGRES_PASSWORD=123 -p 5432:5432 --network minha-rede -d postgres

2. **Metabase**:
docker run -d -p 3000:3000 --name metabase --network minha-rede -d metabase/metabase


## Visualizações de Dados
   Aqui estão alguns exemplos das visualizações geradas pelo Metabase e uma visão dos dados diretamente do PostgreSQL:

### Dashboard Metabase
   ![Dashboard Metabase](https://github.com/steniosilveiraa/Case_Capim/blob/master/images/DashMetabase.png)

### Dados no PostgreSQL
   ![Dashboard Metabase](https://github.com/steniosilveiraa/Case_Capim/blob/master/images/Postgres.png)

<img width="680" alt="Dados PostgreSQL" src="https://github.com/steniosilveiraa/Case_Capim/blob/master/images/Postgres.png">   
