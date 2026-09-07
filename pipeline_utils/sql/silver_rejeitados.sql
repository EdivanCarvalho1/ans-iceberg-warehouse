INSERT OVERWRITE spark_catalog.silver.beneficiario_rejeitado
SELECT
    id_cmpt_movel,
    cd_operadora,
    cd_municipio,
    cd_plano,
    tp_sexo,
    de_faixa_etaria,
    de_faixa_etaria_reaj,
    tipo_vinculo
FROM vw_silver_validated
WHERE invalid_record = TRUE;
