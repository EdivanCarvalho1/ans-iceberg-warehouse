INSERT OVERWRITE spark_catalog.gold.dim_perfil_beneficiario
WITH ranked_perfis AS (
    SELECT
        XXHASH64(CONCAT_WS('||',
            'dim_perfil_beneficiario',
            COALESCE(TRIM(CAST(tp_sexo AS STRING)), '__NULL__'),
            COALESCE(TRIM(CAST(de_faixa_etaria AS STRING)), '__NULL__'),
            COALESCE(TRIM(CAST(de_faixa_etaria_reaj AS STRING)), '__NULL__'),
            COALESCE(TRIM(CAST(tipo_vinculo AS STRING)), '__NULL__')
        )) AS sk_perfil_beneficiario,
        tp_sexo,
        de_faixa_etaria,
        de_faixa_etaria_reaj,
        tipo_vinculo,
        ROW_NUMBER() OVER (
            PARTITION BY tp_sexo, de_faixa_etaria, de_faixa_etaria_reaj, tipo_vinculo
            ORDER BY SHA2(CONCAT_WS('||',
                COALESCE(tp_sexo, '__NULL__'),
                COALESCE(de_faixa_etaria, '__NULL__'),
                COALESCE(de_faixa_etaria_reaj, '__NULL__'),
                COALESCE(tipo_vinculo, '__NULL__')
            ), 256) DESC
        ) AS dedup_rank
    FROM spark_catalog.silver.beneficiario_movimento
    WHERE tp_sexo IS NOT NULL
      AND de_faixa_etaria IS NOT NULL
      AND de_faixa_etaria_reaj IS NOT NULL
      AND tipo_vinculo IS NOT NULL
)
SELECT
    sk_perfil_beneficiario,
    tp_sexo,
    de_faixa_etaria,
    de_faixa_etaria_reaj,
    tipo_vinculo
FROM ranked_perfis
WHERE dedup_rank = 1;
