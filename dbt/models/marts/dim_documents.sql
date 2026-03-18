/*
    Mart model: dim_documents (Document Dimension)

    Contains one row per parsed invoice with header-level information.
    Joins to dim_vendors via vendor_key so fact tables can reference
    both document and vendor dimensions.

    Grain: One row per document (invoice)
    Primary key: document_key (surrogate), document_id (business key)
*/

with invoices as (

    select * from {{ ref('int_cleaned_invoices') }}

),

vendors as (

    select vendor_key, vendor_id
    from {{ ref('dim_vendors') }}

),

final as (

    select
        -- Surrogate key
        row_number() over (order by i.document_id) as document_key,

        -- Business key
        i.document_id,

        -- Foreign key to vendor dimension
        v.vendor_key,

        -- Document metadata
        'invoice' as document_type,

        -- Invoice details
        i.invoice_number,
        i.invoice_date,
        i.due_date,
        i.po_number,
        i.payment_terms_days,

        -- Financial data
        i.subtotal,
        i.tax_amount,
        i.total_amount,
        i.effective_tax_rate,
        i.currency,
        i.is_high_value,

        -- Quality metadata
        i.parse_confidence,
        i.validation_status,
        i.parse_timestamp as upload_timestamp,

        -- Audit
        current_timestamp() as created_at

    from invoices i
    left join vendors v
        on i.vendor_id = v.vendor_id

)

select * from final
