-- =============================================================
-- Parsely: Create Tables
-- =============================================================

USE DATABASE parsely;

-- ─── BRONZE LAYER ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS bronze.raw_documents (
    document_id         VARCHAR(36)     NOT NULL,   -- UUID assigned at ingestion
    file_name           VARCHAR(255)    NOT NULL,   -- Original uploaded file name
    file_type           VARCHAR(10)     NOT NULL,   -- pdf, docx, etc.
    file_size_bytes     BIGINT,                     -- File size in bytes
    raw_text            TEXT,                        -- Full extracted text from document
    raw_tables_json     TEXT,                        -- Extracted tables as JSON array
    metadata_json       TEXT,                        -- Document metadata (author, dates, etc.)
    ingestion_timestamp TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    source_path         VARCHAR(500),               -- Upload source path
    PRIMARY KEY (document_id)
);

-- ─── SILVER LAYER ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS silver.parsed_invoices (
    document_id         VARCHAR(36)     NOT NULL,   -- FK to bronze.raw_documents
    vendor_name         VARCHAR(255),
    vendor_address      VARCHAR(500),
    vendor_city         VARCHAR(100),
    vendor_state        VARCHAR(50),
    vendor_zip          VARCHAR(20),
    vendor_phone        VARCHAR(30),
    vendor_email        VARCHAR(255),
    invoice_number      VARCHAR(50),
    invoice_date        DATE,
    due_date            DATE,
    po_number           VARCHAR(50),
    subtotal            NUMBER(12,2),
    tax_amount          NUMBER(12,2),
    total_amount        NUMBER(12,2),
    currency            VARCHAR(3)      DEFAULT 'USD',
    parse_confidence    FLOAT,                      -- 0.0 to 1.0
    validation_status   VARCHAR(20),                -- passed, failed, review_needed
    parse_timestamp     TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (document_id)
);

CREATE TABLE IF NOT EXISTS silver.parsed_line_items (
    line_item_id        VARCHAR(36)     NOT NULL,   -- UUID
    document_id         VARCHAR(36)     NOT NULL,   -- FK to parsed_invoices
    line_number         INT,                        -- Order within invoice
    description         VARCHAR(500),
    quantity            NUMBER(10,2),
    unit_price          NUMBER(12,2),
    line_amount         NUMBER(12,2),               -- quantity * unit_price
    extraction_confidence FLOAT,                    -- 0.0 to 1.0
    PRIMARY KEY (line_item_id)
);

-- ─── GOLD LAYER ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.dim_vendors (
    vendor_key          INT AUTOINCREMENT NOT NULL,
    vendor_id           VARCHAR(64)     NOT NULL,   -- Business key (hash of name + address)
    vendor_name         VARCHAR(255)    NOT NULL,
    vendor_address      VARCHAR(500),
    vendor_city         VARCHAR(100),
    vendor_state        VARCHAR(50),
    vendor_zip          VARCHAR(20),
    vendor_phone        VARCHAR(30),
    vendor_email        VARCHAR(255),
    first_seen_date     DATE,
    document_count      INT             DEFAULT 0,
    created_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (vendor_key),
    UNIQUE (vendor_id)
);

CREATE TABLE IF NOT EXISTS gold.dim_documents (
    document_key        INT AUTOINCREMENT NOT NULL,
    document_id         VARCHAR(36)     NOT NULL,   -- Business key (UUID)
    vendor_key          INT,                        -- FK to dim_vendors
    document_type       VARCHAR(20),                -- invoice, purchase_order
    file_name           VARCHAR(255),
    invoice_number      VARCHAR(50),
    invoice_date        DATE,
    due_date            DATE,
    po_number           VARCHAR(50),
    subtotal            NUMBER(12,2),
    tax_amount          NUMBER(12,2),
    total_amount        NUMBER(12,2),
    currency            VARCHAR(3),
    parse_confidence    FLOAT,
    upload_timestamp    TIMESTAMP_NTZ,
    created_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (document_key),
    UNIQUE (document_id)
);

CREATE TABLE IF NOT EXISTS gold.fact_invoice_line_items (
    line_item_key       INT AUTOINCREMENT NOT NULL,
    document_key        INT             NOT NULL,   -- FK to dim_documents
    vendor_key          INT             NOT NULL,   -- FK to dim_vendors
    line_number         INT,
    description         VARCHAR(500),
    quantity            NUMBER(10,2),
    unit_price          NUMBER(12,2),
    line_amount         NUMBER(12,2),
    created_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (line_item_key)
);

CREATE TABLE IF NOT EXISTS gold.fact_invoice_summary (
    summary_key         INT AUTOINCREMENT NOT NULL,
    document_key        INT             NOT NULL,   -- FK to dim_documents
    vendor_key          INT             NOT NULL,   -- FK to dim_vendors
    total_line_items    INT,
    subtotal            NUMBER(12,2),
    tax_amount          NUMBER(12,2),
    total_amount        NUMBER(12,2),
    avg_line_item_amount NUMBER(12,2),
    max_line_item_amount NUMBER(12,2),
    invoice_date        DATE,
    created_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (summary_key)
);
