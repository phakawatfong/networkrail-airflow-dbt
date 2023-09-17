with

source as (

    select * from {{ source('networkrail', 'movements') }}

)

, renamed_recasted as (

    select 
        event_type,
        DATETIME(gbtt_timestamp, "Europe/London") as gbtt_timestamp_utc,
        original_loc_stanox,
        DATETIME(planned_timestamp, "Europe/London") as planned_timestamp_utc,
        timetable_variation,
        DATETIME(original_loc_timestamp, "Europe/London") as  original_loc_timestamp_utc,
        current_train_id,
        delay_monitoring_point,
        next_report_run_time,
        reporting_stanox,
        DATETIME(actual_timestamp, "Europe/London") as actual_timestamp_utc,
        correction_ind,
        event_source,
        train_file_address,
        platform,
        division_code,
        train_terminated,
        train_id,
        offroute_ind,
        variation_status,
        train_service_code,
        toc_id,
        loc_stanox,
        auto_expected,
        direction_ind,
        route,
        planned_event_type,
        next_report_stanox,
        line_ind
    from source

)

, final as (


    select
        event_type,
        gbtt_timestamp_utc,
        original_loc_stanox,
        planned_timestamp_utc,
        timetable_variation,
        original_loc_timestamp_utc,
        current_train_id,
        delay_monitoring_point,
        next_report_run_time,
        reporting_stanox,
        actual_timestamp_utc,
        correction_ind,
        event_source,
        train_file_address,
        platform,
        division_code,
        train_terminated,
        train_id,
        offroute_ind,
        variation_status,
        train_service_code,
        toc_id,
        loc_stanox,
        auto_expected,
        direction_ind,
        route,
        planned_event_type,
        next_report_stanox,
        line_ind
    from renamed_recasted

)

select * from final