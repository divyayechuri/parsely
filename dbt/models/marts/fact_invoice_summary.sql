/*
    Mart model: fact_invoice_summary

    Aggregated fact table at the invoice grain. Provides pre-computed
    metrics per invoice so dashboards and the Streamlit app don't
    need to aggregate line items on every query.

    Metrics:
      - Total line item count
      - Financial totals (subtotal, tax, total)
      - Average and max line item amounts
      - Days until payment due

    Grain: One row per invoice
    Primary key: summary_key (surrogate)
    Foreign keys: document_key → dim_documents, vendor_key → dim_vendors
*/

with documents as (

    select * from {{ ref('dim_documents') }}

),

line_items as (

    select
        document_key,
        count(*)                    as total_line_items,
        sum(line_amount)            as sum_line_amounts,
        avg(line_amount)            as avg_line_item_amount,
        max(line_amount)            as max_line_item_amount,
        min(line_amount)            as min_line_item_amount

    from {{ ref('fact_invoice_line_items') }}
    group by document_key

),

final as (

    select
        -- Surrogate key
        row_number() over (order by d.document_key) as summary_key,

        -- Foreign keys
        d.document_key,
        d.vendor_key,

        -- Line item aggregates
        coalesce(li.total_line_items, 0) as total_line_items,

        -- Financial summary
        d.subtotal,
        d.tax_amount,
        d.total_amount,
        round(coalesce(li.avg_line_item_amount, 0), 2) as avg_line_item_amount,
        coalesce(li.max_line_item_amount, 0) as max_line_item_amount,
        coalesce(li.min_line_item_amount, 0) as min_line_item_amount,

        -- Date fields for time-based analysis
        d.invoice_date,
        d.due_date,
        d.payment_terms_days,

        -- Quality
        d.parse_confidence,
        d.is_high_value,

        -- Audit
        current_timestamp() as created_at

    from documents d
    left join line_items li
        on d.document_key = li.document_key

)

select * from final
