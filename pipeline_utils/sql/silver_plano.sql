INSERT OVERWRITE spark_catalog.silver.plano
WITH ranked_planos AS (
    SELECT
        cd_operadora,
        cd_plano,
        tp_vigencia_plano,
        de_contratacao_plano,
        de_segmentacao_plano,
        de_abrg_geografica_plano,
        cobertura_assist_plan,
        ROW_NUMBER() OVER (
            PARTITION BY
                cd_operadora,
                cd_plano
            ORDER BY dt_carga DESC NULLS LAST, business_row_hash DESC
        ) AS dedup_rank
    FROM vw_silver_validated
    WHERE invalid_record = FALSE
      AND cd_operadora IS NOT NULL
      AND cd_plano IS NOT NULL
)
SELECT
    cd_operadora,
    cd_plano,
    tp_vigencia_plano,
    de_contratacao_plano,
    de_segmentacao_plano,
    de_abrg_geografica_plano,
    cobertura_assist_plan
FROM ranked_planos
WHERE dedup_rank = 1;
