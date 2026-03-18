/*
    Macro: parse_currency

    Converts a currency string (e.g., "$1,234.56", "1234.56", "$1234")
    into a clean numeric value. Handles:
      - Dollar signs and other currency symbols
      - Commas as thousands separators
      - Missing decimal places

    Usage in a model:
        select {{ parse_currency('total_amount_str') }} as total_amount

    This macro is reusable across any model that needs to clean
    currency values extracted from documents.
*/

{% macro parse_currency(column_name) %}

    try_cast(
        regexp_replace(
            regexp_replace(
                {{ column_name }},
                '[\\$€£¥,]',  -- Remove currency symbols and commas
                ''
            ),
            '^\\s+|\\s+$',  -- Trim whitespace
            ''
        )
        as number(12, 2)
    )

{% endmacro %}
