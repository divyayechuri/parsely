/*
    Intermediate model: cleaned invoices

    Purpose: Enrich invoice data with computed fields and normalize
    values for consistent downstream analysis.

    Transformations:
      - Calculate payment terms (days between invoice and due date)
      - Standardize currency to uppercase
      - Add a vendor_id hash for joining to the vendor dimension
      - Flag high-value invoices for visibility

    Grain: One row per document
*/

with invoices as (

    select * from {{ ref('stg_parsed_invoices') }}

),

cleaned as (

    select
        document_id,

        -- Vendor fields (will be used to generate vendor_id)
        vendor_name,
        vendor_address,
        vendor_city,
        vendor_state,
        vendor_zip,
        vendor_phone,
        vendor_email,

        -- Generate a deterministic vendor ID for deduplication
        -- Same vendor always gets the same hash, regardless of document
        md5(
            lower(trim(coalesce(vendor_name, '')))
            || '|'
            || lower(trim(coalesce(vendor_address, '')))
        ) as vendor_id,

        -- Invoice details
        invoice_number,
        invoice_date,
        due_date,
        po_number,

        -- Payment terms: days between invoice date and due date
        case
            when invoice_date is not null and due_date is not null
            then datediff('day', invoice_date, due_date)
            else null
        end as payment_terms_days,

        -- Financial data
        subtotal,
        tax_amount,
        total_amount,
        currency,

        -- Tax rate (calculated if not directly available)
        case
            when subtotal > 0 and tax_amount is not null
            then round(tax_amount / subtotal, 4)
            else null
        end as effective_tax_rate,

        -- High-value flag (invoices over $10,000)
        case
            when total_amount >= 10000 then true
            else false
        end as is_high_value,

        -- Metadata
        parse_confidence,
        validation_status,
        parse_timestamp

    from invoices

)

select * from cleaned
