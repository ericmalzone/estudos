 WITH cte (venda_valor, vendedor_nome, id) AS 
(  
    SELECT vda.venda_valor, 
           vdd.vendedor_nome,
           ROW_NUMBER() OVER(PARTITION BY vda.vendedor_id ORDER BY vda.venda_valor DESC) AS id
      FROM venda vda
INNER JOIN vendedor vdd
        ON vda.vendedor_id = vdd.vendedor_id
     WHERE YEAR(vda.venda_data) = '2016'
)
SELECT venda_valor, vendedor_nome
  FROM cte
 WHERE id = 1
