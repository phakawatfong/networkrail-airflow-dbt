-- Which hour of each day has the highest number of late trains? 

WITH source AS (
    
    SELECT * FROM {{ ref('crt_networkrail_fact_movements') }}

)



, sum_late_train_each_hour as (

      SELECT 
            extract(HOUR from actual_timestamp_utc) as hour_each_day,
            sum(case when variation_status = 'LATE' then 1 else 0 end) as number_of_late_train
      FROM source
      group by hour_each_day

)

, final  AS (

            select 
                    *, 
                    row_number() over(order by number_of_late_train desc) as rn 
            from sum_late_train_each_hour
)

select hour_each_day, number_of_late_train from final
where rn = 1
