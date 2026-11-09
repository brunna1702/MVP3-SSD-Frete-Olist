WITH nacional AS (SELECT MEDIAN(frete_relativo) AS m FROM fato_item_frete),
     por_uf   AS (SELECT uf_destino, MEDIAN(frete_relativo) AS m FROM fato_item_frete GROUP BY uf_destino)
SELECT f.uf_destino AS uf, u.regiao,
       COUNT(*)                                                                  AS itens,
       SUM(f.preco)                                                              AS receita_produtos,
       SUM(f.frete)                                                              AS frete_total,
       SUM(CASE WHEN f.frete > 0.3 * f.preco THEN 1 ELSE 0 END)             AS itens_beneficiados,
       SUM(CASE WHEN f.frete > 0.3 * f.preco THEN f.frete - 0.3 * f.preco ELSE 0 END) AS custo_subsidio
FROM fato_item_frete f
JOIN por_uf pu ON f.uf_destino = pu.uf_destino
JOIN dim_uf u  ON f.uf_destino = u.uf
CROSS JOIN nacional n
WHERE pu.m > n.m
GROUP BY f.uf_destino, u.regiao
ORDER BY custo_subsidio DESC
