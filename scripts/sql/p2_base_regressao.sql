SELECT f.frete, f.preco, f.distancia_km, p.peso_taxado_kg, f.uf_destino, u.regiao
FROM fato_item_frete f
JOIN dim_produto p ON f.produto_sk = p.produto_sk
JOIN dim_uf u      ON f.uf_destino = u.uf
WHERE f.frete > 0 AND f.distancia_km IS NOT NULL AND p.peso_taxado_kg > 0
