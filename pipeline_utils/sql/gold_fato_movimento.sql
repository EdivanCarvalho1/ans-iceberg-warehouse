INSERT OVERWRITE spark_catalog.gold.fato_beneficiario_movimento
WITH joined_dimensions AS (
    SELECT
        m.id_cmpt_movel,
        CAST(SUBSTRING(m.id_cmpt_movel, 1, 4) AS INT) AS ano_competencia,
        CAST(SUBSTRING(m.id_cmpt_movel, 5, 2) AS INT) AS mes_competencia,
        TO_DATE(CONCAT(m.id_cmpt_movel, '01'), 'yyyyMMdd') AS dt_competencia,
        DATE_FORMAT(TO_DATE(CONCAT(m.id_cmpt_movel, '01'), 'yyyyMMdd'), 'yyyy-MM') AS ds_competencia,
        o.sk_operadora,
        mu.sk_municipio,
        pl.sk_plano,
        pe.sk_perfil_beneficiario,
        m.qt_beneficiario_ativo,
        m.qt_beneficiario_aderido,
        m.qt_beneficiario_cancelado,
        m.dt_carga,
        SHA2(CONCAT_WS('||',
            COALESCE(m.id_cmpt_movel, '__NULL__'),
            COALESCE(CAST(o.sk_operadora AS STRING), '__NULL__'),
            COALESCE(CAST(mu.sk_municipio AS STRING), '__NULL__'),
            COALESCE(CAST(pl.sk_plano AS STRING), '__NULL__'),
            COALESCE(CAST(pe.sk_perfil_beneficiario AS STRING), '__NULL__'),
            COALESCE(CAST(m.qt_beneficiario_ativo AS STRING), '__NULL__'),
            COALESCE(CAST(m.qt_beneficiario_aderido AS STRING), '__NULL__'),
            COALESCE(CAST(m.qt_beneficiario_cancelado AS STRING), '__NULL__')
        ), 256) AS fact_row_hash
    FROM spark_catalog.silver.beneficiario_movimento m
    INNER JOIN spark_catalog.gold.dim_operadora o
        ON m.cd_operadora = o.cd_operadora
    INNER JOIN spark_catalog.gold.dim_municipio mu
        ON m.cd_municipio = mu.cd_municipio
    INNER JOIN spark_catalog.gold.dim_plano pl
        ON m.cd_operadora = pl.cd_operadora
       AND m.cd_plano = pl.cd_plano
    INNER JOIN spark_catalog.gold.dim_perfil_beneficiario pe
        ON m.tp_sexo = pe.tp_sexo
       AND m.de_faixa_etaria = pe.de_faixa_etaria
       AND m.de_faixa_etaria_reaj = pe.de_faixa_etaria_reaj
       AND m.tipo_vinculo = pe.tipo_vinculo
), ranked_facts AS (
    SELECT
        id_cmpt_movel,
        ano_competencia,
        mes_competencia,
        dt_competencia,
        ds_competencia,
        sk_operadora,
        sk_municipio,
        sk_plano,
        sk_perfil_beneficiario,
        qt_beneficiario_ativo,
        qt_beneficiario_aderido,
        qt_beneficiario_cancelado,
        ROW_NUMBER() OVER (
            PARTITION BY
                id_cmpt_movel,
                sk_operadora,
                sk_municipio,
                sk_plano,
                sk_perfil_beneficiario
            ORDER BY dt_carga DESC NULLS LAST, fact_row_hash DESC
        ) AS dedup_rank
    FROM joined_dimensions
)
SELECT
    id_cmpt_movel,
    ano_competencia,
    mes_competencia,
    dt_competencia,
    ds_competencia,
    sk_operadora,
    sk_municipio,
    sk_plano,
    sk_perfil_beneficiario,
    qt_beneficiario_ativo,
    qt_beneficiario_aderido,
    qt_beneficiario_cancelado
FROM ranked_facts
WHERE dedup_rank = 1;
