SELECT t.ano_mes,
       COUNT(*)                                   AS itens,
       SUM(f.preco)                               AS receita_produtos,
       SUM(f.frete)                               AS frete_total,
       SUM(f.frete) / SUM(f.preco)                AS frete_sobre_receita,
       MEDIAN(f.frete_relativo)                   AS mediana_frete_relativo,
       AVG(CASE WHEN f.frete_relativo > 0.30 THEN 1.0 ELSE 0.0 END) AS pct_itens_frete_acima_30
FROM fato_item_frete f
JOIN dim_tempo t ON f.data_compra_sk = t.data_sk
GROUP BY t.ano_mes
ORDER BY t.ano_mes
