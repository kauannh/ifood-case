# ifood-case

Solução para o case técnico de Data Architect do iFood: ingestão das corridas de táxi de NY (Janeiro a Maio de 2023), disponibilização dos dados para consumo via SQL e análises sobre eles.

Todo o projeto foi desenvolvido no [Databricks Community Edition](https://community.cloud.databricks.com/).

## Visão geral da solução

A ideia foi montar um pipeline que parte dos arquivos originais publicados pela TLC e entrega tabelas prontas para consumo, sem perder o dado bruto pelo caminho. Para isso usei uma arquitetura medalhão, separando o fluxo em três camadas bem definidas, cada uma com uma responsabilidade clara.

## Arquitetura

O dado percorre três camadas:

- **landing**: volume onde ficam os arquivos originais, exatamente como vieram da fonte, separados em pastas por ano e mês da disponibilização.
- **raw**: já em formato Delta, com todas as colunas convertidas para `string`. Isso resolve o problema de mudança de tipagem que existe do mes de Janeiro para os outros meses. As tabelas são particionadas por ano e mês, e cada carga sobrescreve apenas a partição correspondente.
- **trusted**: também em Delta, mas mantendo apenas as colunas que serão consumidas e já com a tipagem correta. É aqui que é feita a conversão de tipos e o dado está na camada de consumo, igualmente particionado por ano e mês.

## Decisões técnicas

- **Arquitetura medalhão**: separar landing, raw e trusted deixa claro de onde o dado veio e o que foi transformado em cada etapa e schema on read para camada de consumo trusted.
- **Schema on read ao processar trusted**: forçar todas as colunas para `string` na raw evita falhas de ingestão por mudança de tipo na origem. A tipagem correta fica concentrada na trusted, onde temos controle do schema final.
- **Particionamento por ano e mês**: como os arquivos são disponibilizados mensalmente, particionar por ano e mês acompanha o padrão da fonte e torna o reprocessamento por período simples e barato.

## Como executar

Importe o repositório no Databricks Community Edition e execute os notebooks na ordem abaixo.

### 1. Setup

Antes de tudo, execute `src/notebooks/0_setup.ipynb`. Ele cria o catálogo, os schemas e o volume usados no case (catálogo `ifood_case`, schemas `raw` e `trusted` e o volume `landing`).

### 2. Pipeline

Com o setup pronto, execute os notebooks na sequência:

- **`src/notebooks/1_landing.ipynb`** — faz a ingestão dos arquivos originais no volume da camada landing. Roda o extrator em `src/ingestion/ny_taxi_trip_extractor.py`.
- **`src/notebooks/2_raw.ipynb`** — processa os arquivos da landing para a camada raw em formato Delta. Roda o processador em `src/processing/landing_to_raw_processor.py`.
- **`src/notebooks/3_trusted.ipynb`** — processa a raw para a camada trusted, selecionando e convertendo as colunas. Roda o processador em `src/processing/raw_to_trusted_processor.py`.

### 3. Análises

Depois do pipeline executado, é possível rodar os notebooks de análise:

- **`analysis/respostas.ipynb`** — queries respondendo as perguntas do case.
- **`analysis/analises.ipynb`** — análise exploratória dos dados, para entender possíveis filtros e identificar outliers.

Na análise identifiquei algumas anomalias nos dados:

- corridas com horário de término anterior ao horário de início;
- corridas sem contagem de passageiros;
- corridas com `total_amount` negativo, não dá para afirmar se são créditos/estornos ou realmente erros, então na query de respostas filtrei apenas `total_amount > 0` para não distorcer as médias;
- corridas com duração maior que 24 horas.

Todos esses pontos estão documentados no notebook de análises e podem ser usados como filtros para refinar ainda mais as respostas.
