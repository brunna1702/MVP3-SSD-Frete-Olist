SELECT COUNT(*) AS itens, COUNT(DISTINCT pedido_id) AS pedidos,
       SUM(preco) AS receita_produtos, SUM(frete) AS frete_total,
       SUM(frete) / SUM(preco) AS frete_sobre_receita,
       MEDIAN(frete_relativo) AS mediana_frete_relativo,
       AVG(CASE WHEN frete_relativo > 0.30 THEN 1.0 ELSE 0.0 END) AS pct_itens_frete_acima_30,
       AVG(CASE WHEN frete_relativo > 1.00 THEN 1.0 ELSE 0.0 END) AS pct_itens_frete_acima_preco,
       AVG(CASE WHEN frete_gratis THEN 1.0 ELSE 0.0 END) AS pct_frete_gratis
FROM fato_item_frete
