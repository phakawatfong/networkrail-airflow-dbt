with source AS (

        SELECT * FROM {{ ref('crt_networkrail_fact_movements') }}
)

, sum_train_late AS (


    SELECT 
        toc_id,
        sum(case when variation_status = 'LATE' then 1 else 0 end) as train_late
    FROM source
    where actual_timestamp_utc >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    group by toc_id
)


, rank_sum_train_late_number AS (
    SELECT
        toc_id,
        train_late,
        rank() over(order by train_late desc) AS rank_late
    FROM sum_train_late
)


select toc_id, train_late from rank_sum_train_late_number where rank_late = 1 