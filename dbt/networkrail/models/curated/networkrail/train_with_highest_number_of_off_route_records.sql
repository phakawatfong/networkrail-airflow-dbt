with source AS (

        SELECT * FROM {{ ref('crt_networkrail_fact_movements') }}
)

, sum_train_off_route_number AS (


    SELECT 
        train_id,
        sum(case when variation_status = 'OFF ROUTE' then 1 else 0 end) as train_off_route_number
    FROM source
    group by train_id
)


, rank_sum_train_off_route_number AS (
    SELECT
        train_id,
        train_off_route_number,
        rank() over(order by train_off_route_number desc) AS rank_off_route
    FROM sum_train_off_route_number
)


select train_id, train_off_route_number from rank_sum_train_off_route_number where rank_off_route = 1 