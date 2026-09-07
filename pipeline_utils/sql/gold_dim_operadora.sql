INSERT OVERWRITE spark_catalog.gold.dim_operadora
WITH ranked_operadoras AS (
    SELECT
        XXHASH64(CONCAT_WS('||', 'dim_operadora', COALESCE(TRIM(CAST(cd_operadora AS STRING)), '__NULL__'))) AS sk_operadora,
        cd_operadora,
        nm_razao_social,
        nr_cnpj,
        modalidade_operadora,
        ROW_NUMBER() OVER (
            PARTITION BY cd_operadora
            ORDER BY SHA2(CONCAT_WS('||',
                COALESCE(nm_razao_social, '__NULL__'),
                COALESCE(nr_cnpj, '__NULL__'),
                COALESCE(modalidade_operadora, '__NULL__')
            ), 256) DESC
        ) AS dedup_rank
    FROM spark_catalog.silver.operadora
    WHERE cd_operadora IS NOT NULL
)
SELECT
    sk_operadora,
    cd_operadora,
    nm_razao_social,
    nr_cnpj,
    modalidade_operadora
FROM ranked_operadoras
WHERE dedup_rank = 1;
