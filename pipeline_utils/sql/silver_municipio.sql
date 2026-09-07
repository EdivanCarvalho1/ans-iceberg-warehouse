INSERT OVERWRITE spark_catalog.silver.municipio
WITH ranked_municipios AS (
    SELECT
        cd_municipio,
        nm_municipio,
        sg_uf,
        ROW_NUMBER() OVER (
            PARTITION BY cd_municipio
            ORDER BY dt_carga DESC NULLS LAST, business_row_hash DESC
        ) AS dedup_rank
    FROM vw_silver_validated
    WHERE invalid_record = FALSE
      AND cd_municipio IS NOT NULL
)
SELECT
    cd_municipio,
    nm_municipio,
    sg_uf
FROM ranked_municipios
WHERE dedup_rank = 1;
