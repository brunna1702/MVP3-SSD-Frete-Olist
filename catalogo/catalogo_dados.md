# Catálogo de Dados — camada gold (`workspace.frete_gold`)

Gerado automaticamente a partir de `scripts/mvp_frete.py` (`CATALOGO_GOLD`). A mesma definição gera o esquema das tabelas Delta, os comentários de tabela e coluna, as restrições (NOT NULL, CHECK, PRIMARY KEY, FOREIGN KEY) e a validação automática dos dados antes da carga.

## `dim_uf`

Unidades da Federação com região e população estimada 2018 (IBGE). Dimensão com dois papéis na fato: UF de origem (vendedor) e UF de destino (cliente).

Chave primária: `uf`

| Atributo | Tipo | Descrição | Domínio | Obrig. | Nulo significa | Linhagem |
|---|---|---|---|---|---|---|
| `uf` | string | Sigla da UF | AC, AL, AM, AP, BA, CE, DF, ES, GO, MA, MG, MS, MT, PA, PB, PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP, TO | sim | — | IBGE (código → sigla, R12) |
| `uf_nome` | string | Nome da UF | — | sim | — | IBGE SIDRA 6579, campo D1N |
| `codigo_ibge` | int | Código IBGE da UF | 11 a 53 | sim | — | IBGE SIDRA 6579, D1C |
| `regiao` | string | Grande região | Norte, Nordeste, Centro-Oeste, Sudeste, Sul | sim | — | Atribuída pela sigla (R12) |
| `populacao_2018` | bigint | População residente estimada em 2018 (pessoas) | 400000 a 50000000 | sim | — | IBGE SIDRA 6579, variável 9324, ano 2018 |
| `lat_centroide` | double | Latitude mediana dos CEPs da UF (graus) | -34 a 5.3 | não | UF sem CEP geolocalizado | Kaggle olist_geolocation (R04) |
| `lon_centroide` | double | Longitude mediana dos CEPs da UF (graus) | -74 a -34.7 | não | UF sem CEP geolocalizado | Kaggle olist_geolocation (R04) |

## `dim_tempo`

Calendário diário da janela de análise (jan/2017 a ago/2018).

Chave primária: `data_sk`

| Atributo | Tipo | Descrição | Domínio | Obrig. | Nulo significa | Linhagem |
|---|---|---|---|---|---|---|
| `data_sk` | int | Chave da data no formato AAAAMMDD | 20170101 a 20180831 | sim | — | Gerada (R14) |
| `data` | date | Data | 2017-01-01 a 2018-08-31 | sim | — | Gerada |
| `ano` | int | Ano | 2017 a 2018 | sim | — | Derivada de data |
| `mes` | int | Mês (1–12) | 1 a 12 | sim | — | Derivada de data |
| `ano_mes` | string | Ano e mês (AAAA-MM) | padrão `^20(17|18)-\d{2}$` | sim | — | Derivada de data |
| `nome_mes` | string | Mês abreviado | jan, fev, mar, abr, mai, jun, jul, ago, set, out, nov, dez | sim | — | Derivada de data |
| `trimestre` | int | Trimestre (1–4) | 1 a 4 | sim | — | Derivada de data |
| `dia_semana` | string | Dia da semana | segunda, terça, quarta, quinta, sexta, sábado, domingo | sim | — | Derivada de data |
| `fim_de_semana` | boolean | Sábado ou domingo | — | sim | — | Derivada de data |

## `dim_produto`

Produtos vendidos na janela, com categoria e medidas usadas no cálculo do frete.

Chave primária: `produto_sk`

| Atributo | Tipo | Descrição | Domínio | Obrig. | Nulo significa | Linhagem |
|---|---|---|---|---|---|---|
| `produto_sk` | int | Chave substituta do produto | ≥ 1 | sim | — | Gerada (R14) |
| `produto_id` | string | Identificador do produto na Olist (hash) | padrão `^[0-9a-f]{32}$` | sim | — | Kaggle olist_products_dataset.csv |
| `categoria` | string | Categoria do produto (nome original em português) | padrão `^[a-z_0-9]+$` | sim | — | products.product_category_name; nulo → 'sem_categoria' (R05) |
| `peso_g` | double | Peso informado (g) | 1 a 50000 | não | Peso não informado ou zero | products.product_weight_g (R06) |
| `comprimento_cm` | double | Comprimento da embalagem (cm) | 1 a 120 | não | Medida não informada | products.product_length_cm (R06) |
| `altura_cm` | double | Altura da embalagem (cm) | 1 a 120 | não | Medida não informada | products.product_height_cm (R06) |
| `largura_cm` | double | Largura da embalagem (cm) | 1 a 120 | não | Medida não informada | products.product_width_cm (R06) |
| `volume_cm3` | double | Volume da embalagem (cm³) | 1 a 1728000 | não | Alguma medida ausente | C × A × L (R06) |
| `peso_real_kg` | double | Peso real (kg) | 0.001 a 50 | não | Peso ausente | peso_g ÷ 1000 (R06) |
| `peso_cubado_kg` | double | Peso cubado (kg) = volume ÷ 6000 | 0 a 300 | não | Alguma medida ausente | volume_cm3 ÷ 6000 (R06) |
| `peso_taxado_kg` | double | Peso usado para cobrança: maior entre real e cubado (kg) | 0 a 300 | não | Peso e medidas ausentes | max(peso_real_kg, peso_cubado_kg) (R06) |
| `medidas_incompletas` | boolean | Peso ou alguma dimensão ausente | — | sim | — | Flag (R06) |

## `dim_vendedor`

Vendedores com vendas na janela, localização de origem e porte.

Chave primária: `vendedor_sk`

| Atributo | Tipo | Descrição | Domínio | Obrig. | Nulo significa | Linhagem |
|---|---|---|---|---|---|---|
| `vendedor_sk` | int | Chave substituta do vendedor | ≥ 1 | sim | — | Gerada (R14) |
| `vendedor_id` | string | Identificador do vendedor na Olist (hash) | padrão `^[0-9a-f]{32}$` | sim | — | Kaggle olist_sellers_dataset.csv |
| `uf` | string | UF do vendedor | AC, AL, AM, AP, BA, CE, DF, ES, GO, MA, MG, MS, MT, PA, PB, PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP, TO | sim | — | sellers.seller_state (R03) |
| `cidade` | string | Cidade do vendedor (minúsculas, sem acento) | — | sim | — | sellers.seller_city (R03) |
| `cep_prefixo` | string | Prefixo de CEP (5 dígitos) | padrão `^\d{5}$` | sim | — | sellers.seller_zip_code_prefix (R02) |
| `itens_vendidos_janela` | int | Itens vendidos na janela | 1 a 5000 | sim | — | Contagem em order_items (R11) |
| `porte` | string | Porte pelo volume de itens | micro (1–9 itens), pequeno (10–49), médio (50–199), grande (200+) | sim | — | Faixas de itens_vendidos_janela (R11) |

## `fato_item_frete`

Fato do frete. Grão: item vendido (pedido × sequência do item), compras de jan/2017 a ago/2018 que geraram envio.

Chave primária: `item_sk`
Chaves estrangeiras: `data_compra_sk` → `dim_tempo.data_sk`; `produto_sk` → `dim_produto.produto_sk`; `vendedor_sk` → `dim_vendedor.vendedor_sk`; `uf_origem` → `dim_uf.uf`; `uf_destino` → `dim_uf.uf`

| Atributo | Tipo | Descrição | Domínio | Obrig. | Nulo significa | Linhagem |
|---|---|---|---|---|---|---|
| `item_sk` | int | Chave substituta do item | ≥ 1 | sim | — | Gerada (R14) |
| `pedido_id` | string | Identificador do pedido (hash) | padrão `^[0-9a-f]{32}$` | sim | — | Kaggle olist_order_items_dataset.csv |
| `item_seq` | int | Sequência do item dentro do pedido | 1 a 25 | sim | — | order_items.order_item_id |
| `data_compra_sk` | int | Data da compra (FK dim_tempo) | 20170101 a 20180831 | sim | — | orders.order_purchase_timestamp (R14) |
| `produto_sk` | int | Produto (FK dim_produto) | ≥ 1 | sim | — | R14 |
| `vendedor_sk` | int | Vendedor (FK dim_vendedor) | ≥ 1 | sim | — | R14 |
| `uf_origem` | string | UF do vendedor — papel origem (FK dim_uf) | AC, AL, AM, AP, BA, CE, DF, ES, GO, MA, MG, MS, MT, PA, PB, PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP, TO | sim | — | sellers.seller_state (R03) |
| `uf_destino` | string | UF do cliente — papel destino (FK dim_uf) | AC, AL, AM, AP, BA, CE, DF, ES, GO, MA, MG, MS, MT, PA, PB, PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP, TO | sim | — | customers.customer_state (R03) |
| `status_pedido` | string | Status do pedido na extração | delivered, shipped, invoiced, processing, created, approved | sim | — | orders.order_status (R08) |
| `preco` | double | Preço do item (R$) | 0.01 a 10000 | sim | — | order_items.price |
| `frete` | double | Frete do item (R$); a Olist rateia o frete do pedido entre os itens | 0 a 1000 | sim | — | order_items.freight_value |
| `frete_relativo` | double | Frete ÷ preço do item (0,30 = frete equivale a 30% do preço) | 0 a 100 | sim | — | frete ÷ preco (R09) |
| `frete_gratis` | boolean | Frete igual a zero | — | sim | — | frete = 0 (R09) |
| `faixa_frete_relativo` | string | Faixa do frete relativo | 0 (grátis), até 10%, 10–20%, 20–30%, 30–50%, 50–100%, acima de 100% | sim | — | Faixas de frete_relativo (R09) |
| `distancia_km` | double | Distância em linha reta vendedor → cliente (km) | 0 a 4500 | não | CEP de origem ou destino sem geolocalização | Haversine entre centroides de CEP (R10) |
| `faixa_distancia` | string | Faixa de distância | até 100 km, 100–500 km, 500–1.000 km, 1.000–2.000 km, 2.000 km ou mais | não | Distância nula | Faixas de distancia_km (R10) |
| `mesma_uf` | boolean | Vendedor e cliente na mesma UF | — | sim | — | uf_origem = uf_destino |
| `itens_no_pedido` | int | Quantidade de itens no pedido | 1 a 25 | sim | — | Contagem por pedido (R14) |
| `nota_avaliacao` | int | Nota da avaliação do pedido (1–5) | 1 a 5 | não | Pedido sem avaliação | order_reviews.review_score (R07) |
