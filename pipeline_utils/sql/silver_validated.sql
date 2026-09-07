CREATE OR REPLACE TEMPORARY VIEW vw_silver_validated AS
WITH source_rows AS (
    SELECT
        CAST(id_cmpt_movel AS STRING) AS id_cmpt_movel_raw,
        CAST(cd_operadora AS STRING) AS cd_operadora_raw,
        CAST(nm_razao_social AS STRING) AS nm_razao_social_raw,
        CAST(nr_cnpj AS STRING) AS nr_cnpj_raw,
        CAST(modalidade_operadora AS STRING) AS modalidade_operadora_raw,
        CAST(sg_uf AS STRING) AS sg_uf_raw,
        CAST(cd_municipio AS STRING) AS cd_municipio_raw,
        CAST(nm_municipio AS STRING) AS nm_municipio_raw,
        CAST(tp_sexo AS STRING) AS tp_sexo_raw,
        CAST(de_faixa_etaria AS STRING) AS de_faixa_etaria_raw,
        CAST(de_faixa_etaria_reaj AS STRING) AS de_faixa_etaria_reaj_raw,
        CAST(cd_plano AS STRING) AS cd_plano_raw,
        CAST(tp_vigencia_plano AS STRING) AS tp_vigencia_plano_raw,
        CAST(de_contratacao_plano AS STRING) AS de_contratacao_plano_raw,
        CAST(de_segmentacao_plano AS STRING) AS de_segmentacao_plano_raw,
        CAST(de_abrg_geografica_plano AS STRING) AS de_abrg_geografica_plano_raw,
        CAST(cobertura_assist_plan AS STRING) AS cobertura_assist_plan_raw,
        CAST(tipo_vinculo AS STRING) AS tipo_vinculo_raw,
        CAST(qt_beneficiario_ativo AS STRING) AS qt_beneficiario_ativo_raw,
        CAST(qt_beneficiario_aderido AS STRING) AS qt_beneficiario_aderido_raw,
        CAST(qt_beneficiario_cancelado AS STRING) AS qt_beneficiario_cancelado_raw,
        CAST(dt_carga AS STRING) AS dt_carga_raw
    FROM spark_catalog.bronze.beneficiarios
), cleaned_strings AS (
    SELECT
        CASE WHEN UPPER(TRIM(id_cmpt_movel_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(id_cmpt_movel_raw) END AS id_cmpt_movel_string,
        CASE WHEN UPPER(TRIM(cd_operadora_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(cd_operadora_raw) END AS cd_operadora_string,
        CASE WHEN UPPER(TRIM(nm_razao_social_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(nm_razao_social_raw) END AS nm_razao_social_string,
        CASE WHEN UPPER(TRIM(nr_cnpj_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(nr_cnpj_raw) END AS nr_cnpj_string,
        CASE WHEN UPPER(TRIM(modalidade_operadora_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(modalidade_operadora_raw) END AS modalidade_operadora_string,
        CASE WHEN UPPER(TRIM(sg_uf_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(sg_uf_raw) END AS sg_uf_string,
        CASE WHEN UPPER(TRIM(cd_municipio_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(cd_municipio_raw) END AS cd_municipio_string,
        CASE WHEN UPPER(TRIM(nm_municipio_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(nm_municipio_raw) END AS nm_municipio_string,
        CASE WHEN UPPER(TRIM(tp_sexo_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(tp_sexo_raw) END AS tp_sexo_string,
        CASE WHEN UPPER(TRIM(de_faixa_etaria_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(de_faixa_etaria_raw) END AS de_faixa_etaria_string,
        CASE WHEN UPPER(TRIM(de_faixa_etaria_reaj_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(de_faixa_etaria_reaj_raw) END AS de_faixa_etaria_reaj_string,
        CASE WHEN UPPER(TRIM(cd_plano_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(cd_plano_raw) END AS cd_plano_string,
        CASE WHEN UPPER(TRIM(tp_vigencia_plano_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(tp_vigencia_plano_raw) END AS tp_vigencia_plano_string,
        CASE WHEN UPPER(TRIM(de_contratacao_plano_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(de_contratacao_plano_raw) END AS de_contratacao_plano_string,
        CASE WHEN UPPER(TRIM(de_segmentacao_plano_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(de_segmentacao_plano_raw) END AS de_segmentacao_plano_string,
        CASE WHEN UPPER(TRIM(de_abrg_geografica_plano_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(de_abrg_geografica_plano_raw) END AS de_abrg_geografica_plano_string,
        CASE WHEN UPPER(TRIM(cobertura_assist_plan_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(cobertura_assist_plan_raw) END AS cobertura_assist_plan_string,
        CASE WHEN UPPER(TRIM(tipo_vinculo_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(tipo_vinculo_raw) END AS tipo_vinculo_string,
        CASE WHEN UPPER(TRIM(qt_beneficiario_ativo_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(qt_beneficiario_ativo_raw) END AS qt_beneficiario_ativo_string,
        CASE WHEN UPPER(TRIM(qt_beneficiario_aderido_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(qt_beneficiario_aderido_raw) END AS qt_beneficiario_aderido_string,
        CASE WHEN UPPER(TRIM(qt_beneficiario_cancelado_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(qt_beneficiario_cancelado_raw) END AS qt_beneficiario_cancelado_string,
        CASE WHEN UPPER(TRIM(dt_carga_raw)) IN ('', 'NULL', 'N/A', 'NA', 'NAN', '-', 'NONE') THEN NULL ELSE TRIM(dt_carga_raw) END AS dt_carga_string
    FROM source_rows
), typed_rows AS (
    SELECT
        CASE WHEN NULLIF(REGEXP_REPLACE(id_cmpt_movel_string, '[^0-9]', ''), '') IS NULL THEN NULL ELSE REGEXP_REPLACE(id_cmpt_movel_string, '[^0-9]', '') END AS id_cmpt_movel,
        CASE WHEN NULLIF(REGEXP_REPLACE(cd_operadora_string, '[^0-9]', ''), '') IS NULL THEN NULL ELSE REGEXP_REPLACE(cd_operadora_string, '[^0-9]', '') END AS cd_operadora,
        UPPER(nm_razao_social_string) AS nm_razao_social,
        CASE WHEN NULLIF(REGEXP_REPLACE(nr_cnpj_string, '[^0-9]', ''), '') IS NULL THEN NULL ELSE REGEXP_REPLACE(nr_cnpj_string, '[^0-9]', '') END AS nr_cnpj,
        UPPER(modalidade_operadora_string) AS modalidade_operadora,
        CASE WHEN UPPER(sg_uf_string) IN ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO') THEN UPPER(sg_uf_string) ELSE NULL END AS sg_uf,
        CASE WHEN NULLIF(REGEXP_REPLACE(cd_municipio_string, '[^0-9]', ''), '') IS NULL THEN NULL ELSE REGEXP_REPLACE(cd_municipio_string, '[^0-9]', '') END AS cd_municipio,
        UPPER(nm_municipio_string) AS nm_municipio,
        UPPER(tp_sexo_string) AS tp_sexo,
        de_faixa_etaria_string AS de_faixa_etaria,
        de_faixa_etaria_reaj_string AS de_faixa_etaria_reaj,
        CASE WHEN NULLIF(REGEXP_REPLACE(cd_plano_string, '[^0-9]', ''), '') IS NULL THEN NULL ELSE REGEXP_REPLACE(cd_plano_string, '[^0-9]', '') END AS cd_plano,
        UPPER(tp_vigencia_plano_string) AS tp_vigencia_plano,
        UPPER(de_contratacao_plano_string) AS de_contratacao_plano,
        UPPER(de_segmentacao_plano_string) AS de_segmentacao_plano,
        UPPER(de_abrg_geografica_plano_string) AS de_abrg_geografica_plano,
        UPPER(cobertura_assist_plan_string) AS cobertura_assist_plan,
        UPPER(tipo_vinculo_string) AS tipo_vinculo,
        CASE WHEN REGEXP_REPLACE(qt_beneficiario_ativo_string, '[^0-9-]', '') RLIKE '^-?[0-9]+$' THEN CAST(REGEXP_REPLACE(qt_beneficiario_ativo_string, '[^0-9-]', '') AS BIGINT) ELSE NULL END AS qt_beneficiario_ativo,
        CASE WHEN REGEXP_REPLACE(qt_beneficiario_aderido_string, '[^0-9-]', '') RLIKE '^-?[0-9]+$' THEN CAST(REGEXP_REPLACE(qt_beneficiario_aderido_string, '[^0-9-]', '') AS BIGINT) ELSE NULL END AS qt_beneficiario_aderido,
        CASE WHEN REGEXP_REPLACE(qt_beneficiario_cancelado_string, '[^0-9-]', '') RLIKE '^-?[0-9]+$' THEN CAST(REGEXP_REPLACE(qt_beneficiario_cancelado_string, '[^0-9-]', '') AS BIGINT) ELSE NULL END AS qt_beneficiario_cancelado,
        COALESCE(TO_DATE(dt_carga_string, 'yyyy-MM-dd'), TO_DATE(dt_carga_string, 'dd/MM/yyyy'), TO_DATE(dt_carga_string, 'yyyyMMdd')) AS dt_carga
    FROM cleaned_strings
), validated_rows AS (
    SELECT
        id_cmpt_movel,
        cd_operadora,
        nm_razao_social,
        nr_cnpj,
        modalidade_operadora,
        sg_uf,
        cd_municipio,
        nm_municipio,
        tp_sexo,
        de_faixa_etaria,
        de_faixa_etaria_reaj,
        cd_plano,
        tp_vigencia_plano,
        de_contratacao_plano,
        de_segmentacao_plano,
        de_abrg_geografica_plano,
        cobertura_assist_plan,
        tipo_vinculo,
        qt_beneficiario_ativo,
        qt_beneficiario_aderido,
        qt_beneficiario_cancelado,
        dt_carga,
        CASE
            WHEN id_cmpt_movel IS NULL
                OR cd_operadora IS NULL
                OR cd_municipio IS NULL
                OR cd_plano IS NULL
                OR tp_sexo IS NULL
                OR de_faixa_etaria IS NULL
                OR de_faixa_etaria_reaj IS NULL
                OR tipo_vinculo IS NULL
                OR (id_cmpt_movel IS NOT NULL AND NOT (id_cmpt_movel RLIKE '^[0-9]{6}$'))
                OR (id_cmpt_movel RLIKE '^[0-9]{6}$' AND NOT (CAST(SUBSTRING(id_cmpt_movel, 5, 2) AS INT) BETWEEN 1 AND 12))
                OR (cd_operadora IS NOT NULL AND NOT (cd_operadora RLIKE '^[0-9]+$'))
                OR (cd_municipio IS NOT NULL AND NOT (cd_municipio RLIKE '^[0-9]{6,7}$'))
                OR sg_uf IS NULL
                OR (tp_sexo IS NOT NULL AND tp_sexo NOT IN ('F', 'M', 'I'))
                OR (nr_cnpj IS NOT NULL AND LENGTH(nr_cnpj) <> 14)
                OR (qt_beneficiario_ativo IS NOT NULL AND qt_beneficiario_ativo < 0)
                OR (qt_beneficiario_aderido IS NOT NULL AND qt_beneficiario_aderido < 0)
                OR (qt_beneficiario_cancelado IS NOT NULL AND qt_beneficiario_cancelado < 0)
                OR dt_carga IS NULL
            THEN TRUE ELSE FALSE
        END AS invalid_record,
        SHA2(CONCAT_WS('||',
            COALESCE(CAST(id_cmpt_movel AS STRING), '__NULL__'),
            COALESCE(CAST(cd_operadora AS STRING), '__NULL__'),
            COALESCE(nm_razao_social, '__NULL__'),
            COALESCE(nr_cnpj, '__NULL__'),
            COALESCE(modalidade_operadora, '__NULL__'),
            COALESCE(sg_uf, '__NULL__'),
            COALESCE(cd_municipio, '__NULL__'),
            COALESCE(nm_municipio, '__NULL__'),
            COALESCE(tp_sexo, '__NULL__'),
            COALESCE(de_faixa_etaria, '__NULL__'),
            COALESCE(de_faixa_etaria_reaj, '__NULL__'),
            COALESCE(cd_plano, '__NULL__'),
            COALESCE(tp_vigencia_plano, '__NULL__'),
            COALESCE(de_contratacao_plano, '__NULL__'),
            COALESCE(de_segmentacao_plano, '__NULL__'),
            COALESCE(de_abrg_geografica_plano, '__NULL__'),
            COALESCE(cobertura_assist_plan, '__NULL__'),
            COALESCE(tipo_vinculo, '__NULL__'),
            COALESCE(CAST(qt_beneficiario_ativo AS STRING), '__NULL__'),
            COALESCE(CAST(qt_beneficiario_aderido AS STRING), '__NULL__'),
            COALESCE(CAST(qt_beneficiario_cancelado AS STRING), '__NULL__'),
            COALESCE(CAST(dt_carga AS STRING), '__NULL__')
        ), 256) AS business_row_hash
    FROM typed_rows
)
SELECT
    id_cmpt_movel,
    cd_operadora,
    nm_razao_social,
    nr_cnpj,
    modalidade_operadora,
    sg_uf,
    cd_municipio,
    nm_municipio,
    tp_sexo,
    de_faixa_etaria,
    de_faixa_etaria_reaj,
    cd_plano,
    tp_vigencia_plano,
    de_contratacao_plano,
    de_segmentacao_plano,
    de_abrg_geografica_plano,
    cobertura_assist_plan,
    tipo_vinculo,
    qt_beneficiario_ativo,
    qt_beneficiario_aderido,
    qt_beneficiario_cancelado,
    dt_carga,
    invalid_record,
    business_row_hash
FROM validated_rows;
