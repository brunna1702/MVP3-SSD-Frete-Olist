# Regras de transformação (R01–R16)

Cada regra está implementada em [`scripts/mvp_frete.py`](../scripts/mvp_frete.py) (`transformar_silver` e
`construir_estrela`). Toda execução registra as linhas afetadas e as linhas resultantes nas tabelas
`frete_silver.meta_log_transformacoes` e `frete_gold.meta_log_transformacoes`; a última execução está em
[`evidencias/tabelas/m2_log_transformacoes.csv`](../evidencias/tabelas/m2_log_transformacoes.csv).

| Regra | Camada | O que faz | Por quê | Linhas afetadas* |
|---|---|---|---|---:|
| **R01** | silver | Converte datas para timestamp, preço e frete para decimal (R$) e a sequência do item para inteiro. | A bronze é lida inteira como texto, sem conversão silenciosa; os tipos são definidos de forma explícita. | 212.091 |
| **R02** | silver | Completa o prefixo de CEP com zeros à esquerda (5 dígitos). | O CEP é chave de junção com a geolocalização; se fosse lido como número, perderia os zeros (CEPs de SP começam com 0). | 0 |
| **R03** | silver | Coloca a UF em maiúsculas e a valida contra as 27 UFs; deixa a cidade em minúsculas, sem acento. | Padroniza as chaves de UF usadas em `dim_uf` e evita grafias diferentes da mesma cidade. | 0 |
| **R04** | silver | Descarta coordenadas fora do território brasileiro e resume a geolocalização em um centroide (mediana) por prefixo de CEP. | A tabela tem 1 milhão de linhas, com repetições e pontos impossíveis; a mediana é robusta a esses erros. | 981.153 → 19.010 CEPs |
| **R05** | silver | Categoria ausente recebe `sem_categoria`. | Mantém o item na análise de frete; a categoria fica explícita na P3. | 610 |
| **R06** | silver | Converte peso de g para kg; calcula o peso cubado (C × A × L ÷ 6.000) e o peso taxado (o maior entre real e cubado); medidas nulas ou zero viram nulo + flag. | Transportadoras cobram pelo peso taxado; o fator 6.000 cm³/kg é o usual no mercado e nos Correios. Peso zero é fisicamente impossível. | 6 |
| **R07** | silver | Mantém uma avaliação por pedido, a respondida por último. | 547 pedidos têm mais de uma avaliação; a nota vale para o pedido inteiro. | 551 |
| **R08** | silver | Escopo: compras de jan/2017 a ago/2018, exceto pedidos cancelados ou indisponíveis e pedidos sem itens. | 2016 tem volume irrisório e set–out/2018 está truncado; pedidos cancelados não geraram envio. | 1.536 |
| **R09** | gold | Frete relativo = frete ÷ preço do item; frete zero sinalizado como `frete_gratis`; faixas de frete relativo. | É a medida central do problema. Frete maior que o preço é **mantido**: é o fenômeno estudado. | 383 (frete zero) |
| **R10** | gold | Distância vendedor → cliente pela fórmula de Haversine entre os centroides dos CEPs (km); faixas de distância. | Mede o fator geográfico da P2. A distância é nula quando um CEP não tem geolocalização; o item não é excluído. | 549 (distância nula) |
| **R11** | gold | Porte do vendedor pelo número de itens vendidos na janela: micro (1–9), pequeno (10–49), médio (50–199), grande (200+). | Dá contexto ao vendedor para análises futuras. | 3.029 |
| **R12** | silver | Conciliação Olist × IBGE: o código IBGE da UF vira sigla (a chave comum); a região é atribuída pela sigla. | A Olist identifica a UF pela sigla, e o IBGE pelo código numérico. | 0 (27 de 27 UFs conciliadas) |
| **R13** | silver | Minimização (LGPD): `customer_unique_id`, título e texto das avaliações não seguem para silver e gold. | Nenhuma pergunta precisa desses dados; reduz a exposição. | 198.665 → 198.114 |
| **R14** | gold | Chaves substitutas (`item_sk`, `produto_sk`, `vendedor_sk`), chave de data AAAAMMDD e contagem de itens no pedido. | Chaves inteiras e estáveis para o modelo estrela; `itens_no_pedido` indica onde o frete foi rateado. | 111.752 |
| **R15** | gold | Grão da fato = item vendido (pedido × sequência do item). | É o nível em que a Olist registra preço e frete. | — |
| **R16** | gold | Validação contra o Catálogo de Dados antes da carga; carga idempotente (DROP + CREATE); restrições NOT NULL, CHECK, PK e FK. | Garante que o que chega à gold respeita os domínios declarados, e que reexecutar não duplica dados. | 54 de 54 atributos OK |

\* Valores da execução de referência sobre a versão 2 do conjunto da Olist no Kaggle.

## Conversões de unidade

| Grandeza | Origem | Destino | Regra |
|---|---|---|---|
| Peso | gramas (`product_weight_g`) | quilogramas (`peso_real_kg`) | R06: ÷ 1.000 |
| Volume | cm × cm × cm | cm³ (`volume_cm3`) | R06 |
| Peso cubado | cm³ | kg (`peso_cubado_kg`) | R06: ÷ 6.000 |
| Distância | graus de latitude e longitude | km (`distancia_km`) | R10: Haversine, raio médio da Terra 6.371 km |
| Frete relativo | R$ / R$ | fração (0,30 = 30%) | R09 |
