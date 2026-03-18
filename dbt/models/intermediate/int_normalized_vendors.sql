/*
    Intermediate model: normalized vendors

    Purpose: Create a deduplicated vendor list from all parsed invoices.
    The same vendor may appear across multiple invoices — this model
    groups them into a single row using a deterministic hash ID.

    Deduplication strategy:
      - Hash vendor name + address to create a stable vendor_id
      - Use the most recent invoice's data for contact info
      - Track first/last seen dates and document count

    Grain: One row per unique vendor
*/

with invoice_vendors as (

    select
        vendor_id,
        vendor_name,
        vendor_address,
        vendor_city,
        vendor_state,
        vendor_zip,
        vendor_phone,
        vendor_email,
        invoice_date,
        parse_timestamp,

        -- Rank to pick the most recent record per vendor
        row_number() over (
            partition by vendor_id
            order by parse_timestamp desc
        ) as recency_rank

    from {{ ref('int_cleaned_invoices') }}

),

-- Get the most recent record for each vendor
latest_per_vendor as (

    select * from invoice_vendors
    where recency_rank = 1

),

-- Aggregate stats across all invoices per vendor
vendor_stats as (

    select
        vendor_id,
        min(invoice_date) as first_seen_date,
        max(invoice_date) as last_seen_date,
        count(*)          as document_count

    from {{ ref('int_cleaned_invoices') }}
    group by vendor_id

),

normalized as (

    select
        lv.vendor_id,
        lv.vendor_name,
        lv.vendor_address,
        lv.vendor_city,
        lv.vendor_state,
        lv.vendor_zip,
        lv.vendor_phone,
        lv.vendor_email,
        vs.first_seen_date,
        vs.last_seen_date,
        vs.document_count

    from latest_per_vendor lv
    left join vendor_stats vs
        on lv.vendor_id = vs.vendor_id

)

select * from normalized
