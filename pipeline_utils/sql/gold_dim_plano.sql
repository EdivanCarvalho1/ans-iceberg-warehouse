INSERT OVERWRITE spark_catalog.gold.dim_plano
WITH ranked_planos AS (
    SELECT
        SHA2(CONCAT_WS('||',
            'dim_plano',
            COALESCE(TRIM(CAST(cd_operadora AS STRING)), '__NULL__'),
            COALESCE(TRIM(CAST(cd_plano AS STRING)), '__NULL__')
        ), 256) AS sk_plano,
        cd_operadora,
        cd_plano,
        tp_vigencia_plano,
        de_contratacao_plano,
        de_segmentacao_plano,
        de_abrg_geografica_plano,
        cobertura_assist_plan,
        ROW_NUMBER() OVER (
            PARTITION BY cd_operadora, cd_plano
            ORDER BY SHA2(CONCAT_WS('||',
                COALESCE(tp_vigencia_plano, '__NULL__'),
                COALESCE(de_contratacao_plano, '__NULL__'),
                COALESCE(de_segmentacao_plano, '__NULL__'),
                COALESCE(de_abrg_geografica_plano, '__NULL__'),
                COALESCE(cobertura_assist_plan, '__NULL__')
            ), 256) DESC
        ) AS dedup_rank
    FROM spark_catalog.silver.plano
    WHERE cd_operadora IS NOT NULL
      AND cd_plano IS NOT NULL
)
SELECT
    sk_plano,
    cd_operadora,
    cd_plano,
    tp_vigencia_plano,
    de_contratacao_plano,
    de_segmentacao_plano,
    de_abrg_geografica_plano,
    cobertura_assist_plan
FROM ranked_planos
WHERE dedup_rank = 1;
