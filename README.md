# Relatório do Projeto ETL em OCaml

## Introdução

Este projeto implementa um sistema ETL (Extract, Transform, Load) em OCaml que processa dados de pedidos e itens de pedidos. O sistema é capaz de ler dados de arquivos CSV locais ou URLs remotas, aplicar transformações e filtros, e exportar os resultados em diferentes formatos.

## Estrutura do Projeto

O projeto está organizado da seguinte forma:

```
etl_project/
├── bin/                 # Código executável
│   ├── main.ml          # Ponto de entrada da aplicação
│   └── dune             # Configuração do executável
├── lib/                 # Bibliotecas e módulos principais
│   ├── types.ml         # Definições de tipos de dados
│   ├── parsers.ml       # Funções para parsing de dados
│   ├── pure.ml          # Funções puras de transformação
│   ├── ioHelper.ml      # Funções de entrada e saída
│   └── dune             # Configuração da biblioteca
├── test/                # Testes automatizados
│   ├── test_pure.ml     # Testes para funções puras
│   └── dune             # Configuração dos testes
├── data/                # Diretório para arquivos de dados
│   ├── order.csv        # Dados de pedidos
│   └── order_item.csv   # Dados de itens de pedidos
├── dune-project         # Configuração do projeto Dune
└── README.md            # Documentação do projeto
```
## Configuração do Ambiente

Para executar este projeto, você precisará:

1. OCaml (recomendado versão 4.14.0 ou superior)
2. Dune (sistema de build)
3. Dependências: csv, sqlite3, unix

Para instalar as dependências: 

```bash
opam install csv sqlite3 unix ounit2
```

## Como Executar

**Compilação**

```bash
dune build
```
**Execução**

```bash
# Execução básica (dados locais)
dune exec etl_project

# Com filtros
dune exec etl_project -- --status Complete --origin P

# Usando formato SQLite
dune exec etl_project -- --sqlite

# Gerando relatório mensal
dune exec etl_project -- --monthly

# Baixando dados de URLs
dune exec etl_project -- --order-url https://example.com/order.csv --order-item-url https://example.com/order_item.csv
```

**Executando Testes**

```bash
dune test
```

## Funcionalidades Implementadas

1. Extração de Dados:
    * Leitura de arquivos CSV locais
    * Download de arquivos CSV de URLs remotas

2. Transformação:
    * Parsing de dados para tipos estruturados
    * Filtragem por status de pedido (Pending, Complete, Cancelled)
    * Filtragem por origem de pedido (P, O)
    * Junção (inner join) entre pedidos e itens de pedido
    * Agregação de itens por pedido (cálculo de total e impostos)
    * Agregação de médias mensais de receita e impostos

3. Carregamento:
    * Saída em formato CSV
    * Saída em formato SQLite
    * Geração de relatório mensal opcional

## Responsabilidades de cada Módulo

### types.ml
- Define os tipos de dados fundamentais do sistema
- Contém definições para `order`, `orderItem`, `order_date`, `status` e `origin`
- Estabelece o modelo de dados que é utilizado por todos os outros módulos
- Separa a representação dos dados da lógica de negócio

### parsers.ml
- Responsável pela conversão de dados brutos para tipos estruturados
- Implementa funções de parsing para cada campo (IDs, datas, status, etc.)
- Utiliza o tipo `Result` para tratamento de erros de validação
- Separa a lógica de parsing do restante do sistema
- Contém funções para processar linhas CSV e converter em registros tipados

### pure.ml
- Contém funções puras (sem efeitos colaterais) de transformação de dados
- Implementa a lógica de agregação, filtragem e junção de pedidos e itens
- Responsável pelos cálculos de valores derivados (totais, impostos, médias)
- Mantém a lógica de negócio isolada das operações de E/S
- Fornece funções como `aggregate_order_items`, `filter_orders` e `join_orders_and_items`

### ioHelper.ml
- Gerencia todas as operações de entrada e saída
- Implementa leitura de arquivos CSV locais
- Fornece funcionalidade para download de dados de URLs remotas
- Responsável pela escrita dos resultados em formatos CSV e SQLite
- Encapsula todos os efeitos colaterais do sistema
- Contém funções como `process_order_csv`, `download_file` e `write_to_sqlite`

### main.ml
- Ponto de entrada da aplicação
- Define e processa os argumentos de linha de comando
- Orquestra o fluxo de trabalho ETL completo
- Conecta todos os módulos em um pipeline coeso
- Implementa o tratamento de erros de alto nível
- Gerencia o ciclo de vida dos recursos (arquivos temporários, etc.)

---

### Esta arquitetura modular permite:
1. Separação clara de responsabilidades
2. Testabilidade melhorada (especialmente das funções puras)
3. Manutenção simplificada
4. Extensibilidade para adicionar novas funcionalidades

A divisão entre operações puras (sem efeitos colaterais) e impuras (com E/S) segue os princípios da programação funcional, facilitando testes e raciocínio sobre o comportamento do sistema.

## Desenvolvimento

### Fase Incial

O Projeto foi construído em OCaml, utilizando o Dune como sistema de build. Apenas existia o arquivo `main.ml`. Primeiro procurei ler os arquivos CSV, utilizando a biblioteca `csv`, e converte-los para tipos estruturados. Já usava tipos especiais para representar os dados, como `order`, `orderItem`, `order_date`, `status` e `origin` e os parsers para converter os dados brutos. Havia funções de debug que printavam os dados, mas não havia lógica de transformação ou carregamento. Em seguida, implementei a lógica de transformação e carregamento, cumprindo com os requisitos **básicos** do projeto.

Vale lembrar que ainda não tinha criado um reposítório no GitHub e logo que acabei de cumprir os requisitos básicos, criei o repositório e fiz o primeiro commit.

### Evolução

Após a primeira versão, separei o código em módulos, moldando a arquitetura do projeto e fui implementando as funcionalidades adicionais. A primeira funcionalidade extra que implementei foi a de output em SQLite e assim por diante fui escolhendo as funcionalidades que era mais interessante implementar / mais faceis de implementar e de pouco em pouco o projeto foi evoluindo.

### Uso de AI Generativa

Eu usei o copilot do VS Code como autocomplete. 

Usei o o3 mini e Claude 3.7 para aprender mais sobre OCaml e o Dune. Pedia para me explicarem conceitos e para aprimorar algumas funções. 

Ex: Quando fiz o inner join meu codigo juntava as duas tabelas de orders, retornando uma terceira com todas as colunas e dai que fazia a filtragem. Ao pedir para as AIs elas sugeriram fazer um map filter, que fazia as duas operações ao mesmo tempo e que era mais eficiente.