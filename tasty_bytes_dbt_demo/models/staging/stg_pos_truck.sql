
{{ config(
    materialized = 'view',
    tags = ['staging','tasty_bytes','pos']
) }}

-- Raw source: tb_101.TRUCK
-- Normalize raw columns -> snake_case, cast types, and derive helpful fields

with src as (
    select
        -- Business keys
        TRUCK_ID,
        FRANCHISE_ID,
        LOCATION_ID,

        -- Attributes
        MAKE,
        MODEL,
        YEAR,
        STATUS,              -- e.g., 'ACTIVE', 'INACTIVE', 'MAINTENANCE'
        PRIMARY_CITY,
        PRIMARY_COUNTRY,

        -- Geo
        LATITUDE,
        LONGITUDE,

        -- Timestamps (optional; adjust to your schema)
        CREATED_AT,
        UPDATED_AT

    from {{ source('tb_101', 'TRUCK') }}
),

typed as (
    select
        cast(TRUCK_ID     as number)      as truck_bk,
        cast(FRANCHISE_ID as number)      as franchise_bk,
        cast(LOCATION_ID  as number)      as location_bk,

        nullif(trim(MAKE), '')            as make,
        nullif(trim(MODEL), '')           as model,
        try_to_number(YEAR)               as truck_year,
        upper(nullif(trim(STATUS), ''))   as status,

        nullif(trim(PRIMARY_CITY), '')    as primary_city,
        nullif(trim(PRIMARY_COUNTRY), '') as primary_country,

        try_to_decimal(LATITUDE, 12, 6)   as latitude,
        try_to_decimal(LONGITUDE,12, 6)   as longitude,

        -- Use Snowflake-safe conversions; change if your warehouse differs
        try_to_timestamp_ntz(CREATED_AT)  as created_at,
        try_to_timestamp_ntz(UPDATED_AT)  as updated_at
    from src
),

derived as (
    select
        *,
        -- Status flags
        case when status = 'ACTIVE' then 1 else 0 end as is_active,

        -- Date keys (YYYYMMDD) for audit/slowly changing contexts
        to_number(to_char(coalesce(updated_at, created_at), 'YYYYMMDD')) as last_event_date_key
    from typed
)

select
    truck_bk,
    franchise_bk,
    location_bk,
    make,
    model,
    truck_year,
    status,
    is_active,
    primary_city,
    primary_country,
    latitude,
    longitude,
    created_at,
    updated_at,
    last_event_date_key
from derived
