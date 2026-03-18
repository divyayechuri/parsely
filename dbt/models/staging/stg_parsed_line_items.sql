/*
    Staging model: parsed line items

    Purpose: Clean interface over the Silver layer parsed_line_items table.
    This model:
      - Renames columns for consistency
      - Ensures numeric types are correct
      - Joins to invoices to filter out items from failed documents

    Grain: One row per line item per document
    Source: silver.parsed_line_items (loaded by Python pipeline)
*/

with source as (

    select * from {{ source('silver', 'parsed_line_items') }}

),

-- Only include line items from invoices that passed staging filter
valid_invoices as (

    select document_id
    from {{ ref('stg_parsed_invoices') }}

),

staged as (

    select
        li.line_item_id,
        li.document_id,
        li.line_number,
        trim(li.description)        as description,
        li.quantity::number(10,2)    as quantity,
        li.unit_price::number(12,2)  as unit_price,
        li.line_amount::number(12,2) as line_amount,
        li.extraction_confidence

    from source li
    inner join valid_invoices vi
        on li.document_id = vi.document_id

)

select * from staged
