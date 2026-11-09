SELECT 'silver.itens' AS origem, COUNT(*) AS itens, ROUND(SUM(preco), 2) AS receita, ROUND(SUM(frete), 2) AS frete
  FROM silver__itens
UNION ALL
SELECT 'gold.fato_item_frete', COUNT(*), ROUND(SUM(preco), 2), ROUND(SUM(frete), 2) FROM fato_item_frete
