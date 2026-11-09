SELECT u.uf, u.regiao, u.populacao_2018,
       COUNT(DISTINCT f.pedido_id)                                AS pedidos,
       COUNT(DISTINCT f.pedido_id) * 100000.0 / u.populacao_2018  AS pedidos_por_100mil_hab,
       MEDIAN(f.frete_relativo)                                   AS mediana_frete_relativo,
       AVG(f.distancia_km)                                        AS distancia_media_km
FROM dim_uf u
LEFT JOIN fato_item_frete f ON f.uf_destino = u.uf
GROUP BY u.uf, u.regiao, u.populacao_2018
ORDER BY pedidos_por_100mil_hab DESC
