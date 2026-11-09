# Evidências

## Geradas automaticamente pelos notebooks

* `graficos/` — gráficos das perguntas P1–P5 e do volume mensal (q3).
* `tabelas/` — resultados da qualidade (q1–q3), da validação do catálogo e do log de linhagem (m1–m2), da carga e das
  conferências SQL (c1–c5), das perguntas (p1–p5), das hipóteses (d1) e do armazenamento (f1–f2).
* `metadados/_metadados_coleta.json` — data da coleta, forma de acesso, volume e SHA-256 de cada arquivo.

## Capturas de tela da execução no Databricks

| Arquivo | O que mostrar | Onde |
|---|---|---|
| `01_volume_bronze.png` | Pasta `coleta=AAAA-MM-DD` com os 10 CSVs e o `_metadados_coleta.json` | Catalog → workspace → frete_bronze → Volumes → arquivos |
| `02_silver_tabelas.png` | As 9 tabelas da silver | Catalog → workspace → frete_silver |
| `03_gold_tabelas.png` | As 5 tabelas do modelo + 3 tabelas `meta_*` | Catalog → workspace → frete_gold |
| `04_gold_colunas_fato.png` | Aba **Columns** da `fato_item_frete`, com tipos e comentários vindos do catálogo | Catalog → frete_gold → fato_item_frete |
| `05_gold_diagrama_er.png` | Diagrama de relacionamentos gerado pelas PK/FK | Catalog → fato_item_frete → *View relationships* / *Entity relationship diagram* |
| `06_notebook_03_conferencias.png` | Saída das conferências SQL (unicidade, órfãos = 0, reconciliação) | Notebook 03, seção 6 |
| `07_notebook_04_hipoteses.png` | Tabela de avaliação das hipóteses | Notebook 04, final |
| `08_sql_editor_consulta.png` (opcional) | Uma consulta da pasta `scripts/sql/` executada no **SQL Editor** sobre a gold | SQL Editor |
