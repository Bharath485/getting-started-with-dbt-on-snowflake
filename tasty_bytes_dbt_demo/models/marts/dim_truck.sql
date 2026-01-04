
{{ config(
    materialized = 'table',
    tags = ['mart','tasty_bytes','pos','dimension']
) }}

-- Surrogate key + conformed attributes
with s as (
    select * from {{ ref('stg_pos_truck') }}
),

dim as (
    select
        {{ dbt_utils.generate_surrogate_key(['truck_bk']) }} as truck_key,   -- surrogate
        truck_bk                                            as truck_bk,    -- business key

        -- hierarchical / conformed attributes
        franchise_bk,
        location_bk,

        make,
        model,
        truck_year,

        primary_city,
        primary_country,

        latitude,
        longitude,

        -- current-state attributes
        status,
        is_active,

        -- audit
        {{ dbt_current_timestamp() }} as dt_created,
        {{ dbt_current_timestamp() }} as dt_modified
    from s
)

select * from dim
