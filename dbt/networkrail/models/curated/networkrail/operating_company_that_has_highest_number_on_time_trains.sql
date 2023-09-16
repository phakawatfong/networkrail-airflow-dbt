with source AS (

        SELECT * FROM {{ ref('crt_networkrail_fact_movements') }}
)

, sum_train_ontime AS (


    SELECT 
        toc_id,
        sum(case when variation_status = 'ON TIME' then 1 else 0 end) as train_ontime
    FROM source
    where actual_timestamp_utc >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    group by toc_id
)


, rank_sum_train_ontime_number AS (
    SELECT
        toc_id,
        train_ontime,
        rank() over(order by train_ontime desc) AS rank_late
    FROM sum_train_ontime
)


select toc_id, train_ontime from rank_sum_train_ontime_number where rank_late = 1 