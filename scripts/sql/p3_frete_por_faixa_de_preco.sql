SELECT CASE WHEN f.preco <= 30  THEN '1. até R$ 30'
            WHEN f.preco <= 50  THEN '2. R$ 30–50'
            WHEN f.preco <= 100 THEN '3. R$ 50–100'
            WHEN f.preco <= 200 THEN '4. R$ 100–200'
            ELSE                     '5. acima de R$ 200' END AS faixa_preco,
       COUNT(*)                 AS itens,
       MEDIAN(f.frete)          AS frete_mediano,
       MEDIAN(f.frete_relativo) AS mediana_frete_relativo,
       AVG(CASE WHEN f.frete_relativo > 0.30 THEN 1.0 ELSE 0.0 END) AS pct_itens_frete_acima_30
FROM fato_item_frete f
GROUP BY 1
ORDER BY 1
