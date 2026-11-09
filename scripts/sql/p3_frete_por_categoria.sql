SELECT p.categoria,
       COUNT(*)                     AS itens,
       MEDIAN(f.frete_relativo)     AS mediana_frete_relativo,
       MEDIAN(f.preco)              AS preco_mediano,
       MEDIAN(f.frete)              AS frete_mediano,
       MEDIAN(p.peso_taxado_kg)     AS peso_taxado_mediano_kg,
       AVG(CASE WHEN p.peso_cubado_kg > p.peso_real_kg THEN 1.0 ELSE 0.0 END) AS pct_cubado_maior_que_real,
       AVG(CASE WHEN f.frete_relativo > 0.30 THEN 1.0 ELSE 0.0 END) AS pct_itens_frete_acima_30,
       SUM(f.frete)                 AS frete_total
FROM fato_item_frete f
JOIN dim_produto p ON f.produto_sk = p.produto_sk
GROUP BY p.categoria
HAVING COUNT(*) >= 500
ORDER BY mediana_frete_relativo DESC
