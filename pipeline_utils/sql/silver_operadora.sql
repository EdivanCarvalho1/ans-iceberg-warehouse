INSERT OVERWRITE spark_catalog.silver.operadora
WITH ranked_operadoras AS (
    SELECT
        cd_operadora,
        nm_razao_social,
        nr_cnpj,
        modalidade_operadora,
        ROW_NUMBER() OVER (
            PARTITION BY cd_operadora
            ORDER BY dt_carga DESC NULLS LAST, business_row_hash DESC
        ) AS dedup_rank
    FROM vw_silver_validated
    WHERE invalid_record = FALSE
      AND cd_operadora IS NOT NULL
)
SELECT
    cd_operadora,
    nm_razao_social,
    nr_cnpj,
    modalidade_operadora
FROM ranked_operadoras
WHERE dedup_rank = 1;
