with pst_networkrail_movements AS (
    SELECT * FROM {{ ref('pst_networkrail_fact_movements') }}
)

, final AS (

    SELECT
            event_type,
            actual_timestamp_utc,
            event_source,
            train_id,
            variation_status,
            toc_id,
            company_name
    FROM pst_networkrail_movements
)


SELECT * FROM final