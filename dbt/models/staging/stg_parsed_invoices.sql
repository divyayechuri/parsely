/*
    Staging model: parsed invoices

    Purpose: Clean interface over the Silver layer parsed_invoices table.
    This model:
      - Renames columns to a consistent convention
      - Casts date strings to proper DATE types
      - Trims whitespace from text fields
      - Filters out failed validations (only passed + review_needed proceed)

    Grain: One row per document (invoice)
    Source: silver.parsed_invoices (loaded by Python pipeline)
*/

with source as (

    select * from {{ source('silver', 'parsed_invoices') }}

),

staged as (

    select
        -- Primary key
        document_id,

        -- Vendor information
        trim(vendor_name)       as vendor_name,
        trim(vendor_address)    as vendor_address,
        trim(vendor_city)       as vendor_city,
        upper(trim(vendor_state)) as vendor_state,
        trim(vendor_zip)        as vendor_zip,
        trim(vendor_phone)      as vendor_phone,
        lower(trim(vendor_email)) as vendor_email,

        -- Invoice details
        trim(invoice_number)    as invoice_number,
        invoice_date::date      as invoice_date,
        due_date::date          as due_date,
        trim(po_number)         as po_number,

        -- Financial data
        subtotal,
        tax_amount,
        total_amount,
        upper(trim(currency))   as currency,

        -- Metadata
        parse_confidence,
        validation_status,
        parse_timestamp

    from source

    -- Only forward data that passed validation or needs review
    where validation_status in ('passed', 'review_needed')

)

select * from staged
