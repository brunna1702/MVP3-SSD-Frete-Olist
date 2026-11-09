-- Modelo estrela do MVP de frete — gerado a partir do Catálogo de Dados (scripts/mvp_frete.py)
-- Destino: Databricks, catálogo workspace, schema frete_gold (tabelas Delta)

CREATE SCHEMA IF NOT EXISTS workspace.frete_gold;

CREATE TABLE IF NOT EXISTS workspace.frete_gold.dim_uf (
  uf STRING NOT NULL COMMENT 'Sigla da UF',
  uf_nome STRING NOT NULL COMMENT 'Nome da UF',
  codigo_ibge INT NOT NULL COMMENT 'Código IBGE da UF',
  regiao STRING NOT NULL COMMENT 'Grande região',
  populacao_2018 BIGINT NOT NULL COMMENT 'População residente estimada em 2018 (pessoas)',
  lat_centroide DOUBLE COMMENT 'Latitude mediana dos CEPs da UF (graus)',
  lon_centroide DOUBLE COMMENT 'Longitude mediana dos CEPs da UF (graus)',
  CONSTRAINT pk_dim_uf PRIMARY KEY (uf)
) USING DELTA COMMENT 'Unidades da Federação com região e população estimada 2018 (IBGE). Dimensão com dois papéis na fato: UF de origem (vendedor) e UF de destino (cliente).';

CREATE TABLE IF NOT EXISTS workspace.frete_gold.dim_tempo (
  data_sk INT NOT NULL COMMENT 'Chave da data no formato AAAAMMDD',
  data DATE NOT NULL COMMENT 'Data',
  ano INT NOT NULL COMMENT 'Ano',
  mes INT NOT NULL COMMENT 'Mês (1–12)',
  ano_mes STRING NOT NULL COMMENT 'Ano e mês (AAAA-MM)',
  nome_mes STRING NOT NULL COMMENT 'Mês abreviado',
  trimestre INT NOT NULL COMMENT 'Trimestre (1–4)',
  dia_semana STRING NOT NULL COMMENT 'Dia da semana',
  fim_de_semana BOOLEAN NOT NULL COMMENT 'Sábado ou domingo',
  CONSTRAINT pk_dim_tempo PRIMARY KEY (data_sk)
) USING DELTA COMMENT 'Calendário diário da janela de análise (jan/2017 a ago/2018).';

CREATE TABLE IF NOT EXISTS workspace.frete_gold.dim_produto (
  produto_sk INT NOT NULL COMMENT 'Chave substituta do produto',
  produto_id STRING NOT NULL COMMENT 'Identificador do produto na Olist (hash)',
  categoria STRING NOT NULL COMMENT 'Categoria do produto (nome original em português)',
  peso_g DOUBLE COMMENT 'Peso informado (g)',
  comprimento_cm DOUBLE COMMENT 'Comprimento da embalagem (cm)',
  altura_cm DOUBLE COMMENT 'Altura da embalagem (cm)',
  largura_cm DOUBLE COMMENT 'Largura da embalagem (cm)',
  volume_cm3 DOUBLE COMMENT 'Volume da embalagem (cm³)',
  peso_real_kg DOUBLE COMMENT 'Peso real (kg)',
  peso_cubado_kg DOUBLE COMMENT 'Peso cubado (kg) = volume ÷ 6000',
  peso_taxado_kg DOUBLE COMMENT 'Peso usado para cobrança: maior entre real e cubado (kg)',
  medidas_incompletas BOOLEAN NOT NULL COMMENT 'Peso ou alguma dimensão ausente',
  CONSTRAINT pk_dim_produto PRIMARY KEY (produto_sk)
) USING DELTA COMMENT 'Produtos vendidos na janela, com categoria e medidas usadas no cálculo do frete.';

CREATE TABLE IF NOT EXISTS workspace.frete_gold.dim_vendedor (
  vendedor_sk INT NOT NULL COMMENT 'Chave substituta do vendedor',
  vendedor_id STRING NOT NULL COMMENT 'Identificador do vendedor na Olist (hash)',
  uf STRING NOT NULL COMMENT 'UF do vendedor',
  cidade STRING NOT NULL COMMENT 'Cidade do vendedor (minúsculas, sem acento)',
  cep_prefixo STRING NOT NULL COMMENT 'Prefixo de CEP (5 dígitos)',
  itens_vendidos_janela INT NOT NULL COMMENT 'Itens vendidos na janela',
  porte STRING NOT NULL COMMENT 'Porte pelo volume de itens',
  CONSTRAINT pk_dim_vendedor PRIMARY KEY (vendedor_sk)
) USING DELTA COMMENT 'Vendedores com vendas na janela, localização de origem e porte.';

CREATE TABLE IF NOT EXISTS workspace.frete_gold.fato_item_frete (
  item_sk INT NOT NULL COMMENT 'Chave substituta do item',
  pedido_id STRING NOT NULL COMMENT 'Identificador do pedido (hash)',
  item_seq INT NOT NULL COMMENT 'Sequência do item dentro do pedido',
  data_compra_sk INT NOT NULL COMMENT 'Data da compra (FK dim_tempo)',
  produto_sk INT NOT NULL COMMENT 'Produto (FK dim_produto)',
  vendedor_sk INT NOT NULL COMMENT 'Vendedor (FK dim_vendedor)',
  uf_origem STRING NOT NULL COMMENT 'UF do vendedor — papel origem (FK dim_uf)',
  uf_destino STRING NOT NULL COMMENT 'UF do cliente — papel destino (FK dim_uf)',
  status_pedido STRING NOT NULL COMMENT 'Status do pedido na extração',
  preco DOUBLE NOT NULL COMMENT 'Preço do item (R$)',
  frete DOUBLE NOT NULL COMMENT 'Frete do item (R$); a Olist rateia o frete do pedido entre os itens',
  frete_relativo DOUBLE NOT NULL COMMENT 'Frete ÷ preço do item (0,30 = frete equivale a 30% do preço)',
  frete_gratis BOOLEAN NOT NULL COMMENT 'Frete igual a zero',
  faixa_frete_relativo STRING NOT NULL COMMENT 'Faixa do frete relativo',
  distancia_km DOUBLE COMMENT 'Distância em linha reta vendedor → cliente (km)',
  faixa_distancia STRING COMMENT 'Faixa de distância',
  mesma_uf BOOLEAN NOT NULL COMMENT 'Vendedor e cliente na mesma UF',
  itens_no_pedido INT NOT NULL COMMENT 'Quantidade de itens no pedido',
  nota_avaliacao INT COMMENT 'Nota da avaliação do pedido (1–5)',
  CONSTRAINT pk_fato_item_frete PRIMARY KEY (item_sk),
  CONSTRAINT fk_fato_data_compra_sk FOREIGN KEY (data_compra_sk) REFERENCES workspace.frete_gold.dim_tempo (data_sk),
  CONSTRAINT fk_fato_produto_sk FOREIGN KEY (produto_sk) REFERENCES workspace.frete_gold.dim_produto (produto_sk),
  CONSTRAINT fk_fato_vendedor_sk FOREIGN KEY (vendedor_sk) REFERENCES workspace.frete_gold.dim_vendedor (vendedor_sk),
  CONSTRAINT fk_fato_uf_origem FOREIGN KEY (uf_origem) REFERENCES workspace.frete_gold.dim_uf (uf),
  CONSTRAINT fk_fato_uf_destino FOREIGN KEY (uf_destino) REFERENCES workspace.frete_gold.dim_uf (uf)
) USING DELTA COMMENT 'Fato do frete. Grão: item vendido (pedido × sequência do item), compras de jan/2017 a ago/2018 que geraram envio.';
