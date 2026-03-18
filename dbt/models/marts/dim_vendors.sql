/*
    Mart model: dim_vendors (Vendor Dimension)

    The vendor dimension contains one row per unique vendor with
    their standardized contact information and aggregate stats.

    This is a Type 1 SCD (Slowly Changing Dimension) — vendor info
    is overwritten with the latest data rather than preserving history.
    For a portfolio project, Type 1 is appropriate. In production,
    you might use Type 2 to track vendor address changes over time.

    Grain: One row per unique vendor
    Primary key: vendor_key (surrogate), vendor_id (business key)
*/

with vendors as (

    select * from {{ ref('int_normalized_vendors') }}

),

final as (

    select
        -- Surrogate key for joins from fact tables
        row_number() over (order by vendor_id) as vendor_key,

        -- Business key (deterministic hash)
        vendor_id,

        -- Vendor details
        vendor_name,
        vendor_address,
        vendor_city,
        vendor_state,
        vendor_zip,
        vendor_phone,
        vendor_email,

        -- Aggregate stats
        first_seen_date,
        last_seen_date,
        document_count,

        -- Audit columns
        current_timestamp() as created_at,
        current_timestamp() as updated_at

    from vendors

)

select * from final
