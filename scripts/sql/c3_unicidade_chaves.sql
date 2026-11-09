SELECT 'dim_uf' AS tabela, COUNT(*) AS linhas, COUNT(DISTINCT uf) AS chaves_distintas FROM dim_uf
UNION ALL SELECT 'dim_tempo', COUNT(*), COUNT(DISTINCT data_sk) FROM dim_tempo
UNION ALL SELECT 'dim_produto', COUNT(*), COUNT(DISTINCT produto_sk) FROM dim_produto
UNION ALL SELECT 'dim_vendedor', COUNT(*), COUNT(DISTINCT vendedor_sk) FROM dim_vendedor
UNION ALL SELECT 'fato_item_frete', COUNT(*), COUNT(DISTINCT item_sk) FROM fato_item_frete
