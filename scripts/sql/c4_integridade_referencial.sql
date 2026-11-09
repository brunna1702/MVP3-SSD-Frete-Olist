SELECT 'data_compra_sk -> dim_tempo' AS chave_estrangeira, COUNT(*) AS orfaos
  FROM fato_item_frete f LEFT JOIN dim_tempo t ON f.data_compra_sk = t.data_sk WHERE t.data_sk IS NULL
UNION ALL SELECT 'produto_sk -> dim_produto', COUNT(*)
  FROM fato_item_frete f LEFT JOIN dim_produto p ON f.produto_sk = p.produto_sk WHERE p.produto_sk IS NULL
UNION ALL SELECT 'vendedor_sk -> dim_vendedor', COUNT(*)
  FROM fato_item_frete f LEFT JOIN dim_vendedor v ON f.vendedor_sk = v.vendedor_sk WHERE v.vendedor_sk IS NULL
UNION ALL SELECT 'uf_origem -> dim_uf', COUNT(*)
  FROM fato_item_frete f LEFT JOIN dim_uf u ON f.uf_origem = u.uf WHERE u.uf IS NULL
UNION ALL SELECT 'uf_destino -> dim_uf', COUNT(*)
  FROM fato_item_frete f LEFT JOIN dim_uf u ON f.uf_destino = u.uf WHERE u.uf IS NULL
