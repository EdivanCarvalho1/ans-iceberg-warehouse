INSERT OVERWRITE spark_catalog.silver.beneficiario_movimento
WITH ranked_movimentos AS (
    SELECT
        id_cmpt_movel,
        cd_operadora,
        cd_municipio,
        cd_plano,
        tp_sexo,
        de_faixa_etaria,
        de_faixa_etaria_reaj,
        tipo_vinculo,
        qt_beneficiario_ativo,
        qt_beneficiario_aderido,
        qt_beneficiario_cancelado,
        dt_carga,
        ROW_NUMBER() OVER (
            PARTITION BY
                id_cmpt_movel,
                cd_operadora,
                cd_municipio,
                cd_plano,
                tp_sexo,
                de_faixa_etaria,
                de_faixa_etaria_reaj,
                tipo_vinculo
            ORDER BY dt_carga DESC NULLS LAST, business_row_hash DESC
        ) AS dedup_rank
    FROM vw_silver_validated
    WHERE invalid_record = FALSE
)
SELECT
    id_cmpt_movel,
    cd_operadora,
    cd_municipio,
    cd_plano,
    tp_sexo,
    de_faixa_etaria,
    de_faixa_etaria_reaj,
    tipo_vinculo,
    qt_beneficiario_ativo,
    qt_beneficiario_aderido,
    qt_beneficiario_cancelado,
    dt_carga
FROM ranked_movimentos
WHERE dedup_rank = 1;
