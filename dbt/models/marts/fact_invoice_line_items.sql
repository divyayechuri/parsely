/*
    Mart model: fact_invoice_line_items

    Fact table at the line-item grain — the most detailed level of
    invoice data. Each row represents one item on one invoice.

    Joins to both dimensions so you can analyze:
      - What items does each vendor sell?
      - What's the average unit price by item description?
      - Which line items drive the most spend?

    Grain: One row per line item per invoice
    Primary key: line_item_key (surrogate)
    Foreign keys: document_key → dim_documents, vendor_key → dim_vendors
*/

with line_items as (

    select * from {{ ref('stg_parsed_line_items') }}

),

documents as (

    select document_key, document_id, vendor_key
    from {{ ref('dim_documents') }}

),

final as (

    select
        -- Surrogate key
        row_number() over (order by li.line_item_id) as line_item_key,

        -- Foreign keys
        d.document_key,
        d.vendor_key,

        -- Line item details
        li.line_number,
        li.description,
        li.quantity,
        li.unit_price,
        li.line_amount,

        -- Audit
        current_timestamp() as created_at

    from line_items li
    inner join documents d
        on li.document_id = d.document_id

)

select * from final
