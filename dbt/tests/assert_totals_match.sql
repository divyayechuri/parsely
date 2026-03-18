/*
    Custom data test: Invoice totals must add up.

    Verifies that total_amount = subtotal + tax_amount for every invoice.
    This is the most critical data quality check — if the math doesn't
    work, either the extraction was wrong or the document has errors.

    A tolerance of $0.02 is allowed for floating-point rounding.

    If this test returns any rows, those invoices have mismatched totals.
    dbt treats any rows returned by a test as failures.
*/

with invoices as (

    select
        document_key,
        invoice_number,
        subtotal,
        tax_amount,
        total_amount,
        round(coalesce(subtotal, 0) + coalesce(tax_amount, 0), 2) as expected_total

    from {{ ref('dim_documents') }}

    -- Only test rows where we have all three values
    where subtotal is not null
      and tax_amount is not null
      and total_amount is not null

)

select
    document_key,
    invoice_number,
    subtotal,
    tax_amount,
    total_amount,
    expected_total,
    abs(total_amount - expected_total) as difference

from invoices
where abs(total_amount - expected_total) > 0.02
