with source AS (

    SELECT * FROM {{ ref('crt_networkrail_fact_movements') }}

)


, aggregate_train_status AS (


    SELECT 
                train_id,
                array_agg(variation_status) as array_status
            FROM source
            group by train_id
)

select train_id
from aggregate_train_status
where 'LATE' not in unnest(array_status) AND 'OFF ROUTE' not in unnest(array_status)
