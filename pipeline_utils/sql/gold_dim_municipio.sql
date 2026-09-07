INSERT OVERWRITE spark_catalog.gold.dim_municipio
WITH ranked_municipios AS (
    SELECT
        SHA2(CONCAT_WS('||', 'dim_municipio', COALESCE(TRIM(CAST(cd_municipio AS STRING)), '__NULL__')), 256) AS sk_municipio,
        cd_municipio,
        nm_municipio,
        sg_uf,
        ROW_NUMBER() OVER (
            PARTITION BY cd_municipio
            ORDER BY SHA2(CONCAT_WS('||',
                COALESCE(nm_municipio, '__NULL__'),
                COALESCE(sg_uf, '__NULL__')
            ), 256) DESC
        ) AS dedup_rank
    FROM spark_catalog.silver.municipio
    WHERE cd_municipio IS NOT NULL
)
SELECT
    sk_municipio,
    cd_municipio,
    nm_municipio,
    sg_uf
FROM ranked_municipios
WHERE dedup_rank = 1;
