SELECT f.uf_destino AS uf, u.regiao,
       COUNT(*)                      AS itens,
       MEDIAN(f.frete_relativo)      AS mediana_frete_relativo,
       SUM(f.frete) / SUM(f.preco)   AS frete_sobre_receita,
       AVG(f.frete)                  AS frete_medio,
       MEDIAN(f.preco)               AS preco_mediano,
       AVG(f.distancia_km)           AS distancia_media_km,
       AVG(p.peso_taxado_kg)         AS peso_taxado_medio_kg,
       AVG(CASE WHEN f.frete_relativo > 0.30 THEN 1.0 ELSE 0.0 END) AS pct_itens_frete_acima_30
FROM fato_item_frete f
JOIN dim_uf u      ON f.uf_destino = u.uf
JOIN dim_produto p ON f.produto_sk = p.produto_sk
GROUP BY f.uf_destino, u.regiao
ORDER BY mediana_frete_relativo DESC
