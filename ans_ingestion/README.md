# ANS Beneficiários Ingestion

Pipeline Python para ingestão automática de dados públicos da ANS na camada **raw** de um Data Lake em **Hadoop HDFS**.

O projeto lista os arquivos disponíveis em um diretório público da ANS, filtra os ZIPs válidos, baixa os arquivos, valida o conteúdo compactado, extrai os dados e publica os arquivos resultantes no HDFS usando WebHDFS.

## Contexto

Este projeto faz parte de um laboratório Big Data executado em um servidor de homelab com Hadoop, Spark, Hive, Apache Iceberg e JupyterHub/JupyterLab.

A ingestão representa a primeira etapa do fluxo de dados: carregar os dados crus no HDFS para posterior transformação, organização em camadas Medallion e modelagem dimensional com Apache Iceberg.

## O que o pipeline faz

1. Lê configurações por variáveis de ambiente.
2. Acessa automaticamente o diretório público da ANS.
3. Lista os arquivos disponíveis no website.
4. Filtra apenas arquivos `.zip` válidos.
5. Ignora arquivos consolidados identificados pelo token `XX`.
6. Baixa cada ZIP para uma área temporária local.
7. Valida os caminhos internos do ZIP para evitar path traversal.
8. Extrai os arquivos do pacote.
9. Envia os arquivos extraídos para uma área de staging no HDFS.
10. Publica os dados no destino final somente após a carga completa.
11. Remove arquivos temporários locais ao final do processamento.

## Arquitetura do fluxo

```text
Website ANS
   |
   | listagem + download dos ZIPs
   v
Área temporária local
   |
   | validação + extração
   v
HDFS staging
   |
   | publicação controlada
   v
HDFS raw zone
/dados/raw/ans/
```

## Boas práticas aplicadas

### Separação de responsabilidades

O projeto foi organizado em componentes independentes:

- `file_lister.py`: lista arquivos disponíveis no diretório HTTP.
- `filters.py`: centraliza as regras de filtro dos arquivos.
- `downloader.py`: realiza o download dos arquivos.
- `hdfs_client.py`: encapsula a comunicação com o HDFS via WebHDFS.
- `service.py`: orquestra o fluxo de ingestão.
- `config.py`: concentra as configurações do pipeline.
- `contracts.py`: define contratos/Protocol para baixo acoplamento entre componentes.
- `factory.py`: monta as dependências da aplicação.

Essa estrutura facilita testes, manutenção e substituição de implementações no futuro.

### Configuração externa

As principais configurações são definidas por variáveis de ambiente, evitando valores fixos no código e facilitando a execução em diferentes ambientes.

### Publicação com staging

Antes de publicar no destino final, os arquivos são enviados para um diretório temporário de staging no HDFS. A troca para o destino final acontece somente após a ingestão ser concluída com sucesso.

Esse desenho reduz o risco de deixar a camada raw parcialmente carregada em caso de erro durante download, extração ou upload.

### Backup e rollback simples

Quando já existe uma versão anterior no destino final, o pipeline move essa versão para backup temporário antes da publicação da nova carga. Se houver falha na troca, o processo tenta restaurar o backup.

### Segurança na extração dos ZIPs

Antes de extrair os arquivos, o pipeline valida os nomes internos do ZIP e bloqueia caminhos absolutos ou com `..`, reduzindo o risco de path traversal.

### Download em chunks

O download utiliza escrita em arquivo com tamanho de chunk configurável, evitando carregar arquivos grandes inteiros em memória.

### Limpeza de arquivos temporários

Após cada arquivo processado, o ZIP baixado e o diretório de extração local são removidos, reduzindo consumo desnecessário de disco.

## Requisitos

- Python 3.12+
- Hadoop HDFS em execução
- WebHDFS habilitado no NameNode
- Acesso de rede ao NameNode/WebHDFS
- Dependências Python listadas em `requirements.txt`

Instalação das dependências:

```bash
pip install -r requirements.txt
```

Atualmente, a dependência principal é:

```text
hdfs>=2.7.3
```

## Variáveis de ambiente

Exemplo de configuração:

```bash
export HDFS_BASE_URI=hdfs://localhost:9000
export HDFS_WEB_URL=http://localhost:9870
export HDFS_USER=edivan

export ANS_SOURCE_URL=https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios-024/202602/
export ANS_HDFS_DIR=hdfs://localhost:9000/dados/raw/ans/
export ANS_LOCAL_TMP_DIR=/tmp/ans
export ANS_REQUEST_TIMEOUT_SECONDS=60
export LOG_LEVEL=INFO
```

### Descrição das variáveis

| Variável | Descrição |
|---|---|
| `HDFS_BASE_URI` | URI base do HDFS, por exemplo `hdfs://localhost:9000`. |
| `HDFS_WEB_URL` | URL do WebHDFS/NameNode HTTP, por exemplo `http://localhost:9870`. |
| `HDFS_USER` | Usuário utilizado pelo cliente WebHDFS. |
| `ANS_SOURCE_URL` | URL pública de origem dos arquivos da ANS. |
| `ANS_HDFS_DIR` | Diretório final no HDFS onde os dados crus serão publicados. |
| `ANS_LOCAL_TMP_DIR` | Diretório temporário local usado para download e extração. |
| `ANS_REQUEST_TIMEOUT_SECONDS` | Timeout das requisições HTTP. |
| `LOG_LEVEL` | Nível de log da aplicação. |

Se `ANS_HDFS_DIR` não for definido, o destino padrão será:

```bash
${HDFS_BASE_URI}/dados/raw/ans/
```

Se `HDFS_WEB_URL` não for definido, o projeto tenta inferir automaticamente:

```bash
http://<host-do-HDFS_BASE_URI>:9870
```

## Execução

Dependendo da estrutura local do projeto, a execução pode ser feita por módulo:

```bash
python -m ans_ingestion.main
```

Ou diretamente pelo arquivo principal:

```bash
python main.py
```

Também existe o notebook `main.ipynb`, útil para demonstração e execução interativa no JupyterHub/JupyterLab.

Para execução operacional ou automatizada, prefira o entrypoint Python em vez do notebook.

## Estrutura sugerida do projeto

```text
ans_ingestion/
├── config.py
├── contracts.py
├── downloader.py
├── factory.py
├── file_lister.py
├── filters.py
├── hdfs_client.py
├── html_parser.py
├── main.py
└── service.py

requirements.txt
README.md
main.ipynb
```

## Exemplo de destino no HDFS

```text
/dados/raw/ans/
```

Essa pasta representa a camada de dados crus, que pode ser usada posteriormente como origem para transformações em camadas Bronze, Silver e Gold.

## Testes

Caso exista uma suíte de testes no projeto:

```bash
python -m unittest discover
```

## Observações operacionais

- O cliente HDFS utiliza a biblioteca Python `hdfs`, que se comunica com o NameNode via WebHDFS.
- O pipeline não depende diretamente do comando `hdfs dfs` para publicar os arquivos.
- O processo usa staging e backup temporário para evitar publicação parcial no diretório final.
- A carga atual é voltada para dados públicos da ANS, mas a estrutura permite reaproveitamento para outras fontes HTTP com poucos ajustes.

## Próximas evoluções

- Adicionar orquestração com Apache Airflow.
- Criar camadas Medallion a partir dos dados crus no HDFS.
- Transformar e persistir tabelas analíticas com Apache Iceberg.
- Modelar um Data Warehouse dimensional sobre os dados tratados.
- Adicionar logs estruturados e métricas operacionais.
- Criar testes automatizados para filtros, listagem, download e publicação no HDFS.
