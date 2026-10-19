-- Coldstone warehouse, Kenya: Redshift DDL for the coldstone_web database (workgroup eatngo-analytics).
-- Run once in DBeaver on the coldstone_web connection. Generated from 01_tables.sql (the template).
--
-- Two schemas for Kenya:
--   coldstone_web_ke_staging   - recreated and loaded by COPY on every run (one business date's Parquet files from S3)
--   coldstone_web_ke_analytics - the permanent tables; staging rows are MERGEd in by external_id
--
-- Optimisation choices:
--   * Order, activity and customer tables are SORTed by created_at, so date-range queries skip old blocks
--     (Redshift's equivalent of partitioning).
--   * orders and its child tables share DISTKEY on the order id, so order joins happen on one node
--     without shuffling. activity_logs / activity_initiators do the same on the activity id.
--   * Small reference tables (branches, menu, staff, roles, discounts, wallets) use DISTSTYLE ALL:
--     a full copy on every node, so joins to them never move data.
--   * PRIMARY KEY / FOREIGN KEY are informational only (Redshift does not enforce them); the optimiser
--     uses them, and the MERGE step is what actually prevents duplicates.
--
-- Conventions: ids are the API's 24-character ids; money is BIGINT cents; timestamps are UTC.
-- messages / message_recipients are not created: the API has no source for them.

CREATE SCHEMA IF NOT EXISTS coldstone_web_ke_staging;
CREATE SCHEMA IF NOT EXISTS coldstone_web_ke_analytics;


-- ===================== reference tables (full snapshot every run) =====================

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.branches (
    external_id           VARCHAR(24)   NOT NULL,
    branch_code           VARCHAR(16),
    name                  VARCHAR(256),
    address               VARCHAR(1024),
    latitude              DOUBLE PRECISION,
    longitude             DOUBLE PRECISION,
    is_active             BOOLEAN,
    is_headquarters       BOOLEAN,
    is_private            BOOLEAN,
    longest_distance      INTEGER,
    customer_external_id  VARCHAR(24),
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    loaded_at             TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTSTYLE ALL SORTKEY (external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.roles (
    external_id  VARCHAR(24)   NOT NULL,
    name         VARCHAR(256),
    description  VARCHAR(1024),
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP,
    loaded_at    TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTSTYLE ALL SORTKEY (external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics."permissions" (
    external_id  VARCHAR(24)   NOT NULL,
    name         VARCHAR(256),
    description  VARCHAR(1024),
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP,
    loaded_at    TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTSTYLE ALL SORTKEY (external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.sub_permissions (
    external_id             VARCHAR(24)   NOT NULL,
    name                    VARCHAR(256),
    description             VARCHAR(1024),
    permission_external_id  VARCHAR(24)   REFERENCES coldstone_web_ke_analytics."permissions" (external_id),
    is_enabled              BOOLEAN,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP,
    loaded_at               TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTSTYLE ALL SORTKEY (permission_external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.members (
    external_id           VARCHAR(24)   NOT NULL,
    email                 VARCHAR(256),
    username              VARCHAR(256),
    first_name            VARCHAR(256),
    last_name             VARCHAR(256),
    role_name             VARCHAR(256),
    is_deactivated        BOOLEAN,
    verified              BOOLEAN,
    phone_number          VARCHAR(32),
    customer_external_id  VARCHAR(24),
    branch_external_id    VARCHAR(24)   REFERENCES coldstone_web_ke_analytics.branches (external_id),
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    loaded_at             TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTSTYLE ALL SORTKEY (external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.branch_members (
    branch_external_id  VARCHAR(24)  NOT NULL REFERENCES coldstone_web_ke_analytics.branches (external_id),
    member_external_id  VARCHAR(24)  NOT NULL REFERENCES coldstone_web_ke_analytics.members (external_id),
    loaded_at           TIMESTAMP    DEFAULT GETDATE(),
    PRIMARY KEY (branch_external_id, member_external_id)
) DISTSTYLE ALL SORTKEY (member_external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.member_additional_branches (
    member_external_id  VARCHAR(24)  NOT NULL REFERENCES coldstone_web_ke_analytics.members (external_id),
    branch_external_id  VARCHAR(24)  NOT NULL REFERENCES coldstone_web_ke_analytics.branches (external_id),
    loaded_at           TIMESTAMP    DEFAULT GETDATE(),
    PRIMARY KEY (member_external_id, branch_external_id)
) DISTSTYLE ALL SORTKEY (member_external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.member_roles (
    member_external_id  VARCHAR(24)  NOT NULL REFERENCES coldstone_web_ke_analytics.members (external_id),
    role_external_id    VARCHAR(24)  NOT NULL REFERENCES coldstone_web_ke_analytics.roles (external_id),
    loaded_at           TIMESTAMP    DEFAULT GETDATE(),
    PRIMARY KEY (member_external_id, role_external_id)
) DISTSTYLE ALL SORTKEY (member_external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.menu_categories (
    external_id  VARCHAR(24)   NOT NULL,
    name         VARCHAR(256),
    loaded_at    TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTSTYLE ALL SORTKEY (external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.menu_items (
    external_id           VARCHAR(24)   NOT NULL,
    slug                  VARCHAR(512),
    name                  VARCHAR(512),
    description           VARCHAR(4096),
    price                 BIGINT,
    original_price        BIGINT,
    lagos_price           BIGINT,
    regional_price        BIGINT,
    quantity              INTEGER,
    track_quantity        BOOLEAN,
    is_cup_size           BOOLEAN,
    cup_size              VARCHAR(64),
    has_sku               BOOLEAN,
    sku                   VARCHAR(128),
    low_stock_threshold   INTEGER,
    image                 VARCHAR(2048),
    channel               VARCHAR(32),
    active                BOOLEAN,
    out_of_stock          BOOLEAN,
    customer_external_id  VARCHAR(24),
    branch_external_id    VARCHAR(24)   REFERENCES coldstone_web_ke_analytics.branches (external_id),
    category_external_id  VARCHAR(24)   REFERENCES coldstone_web_ke_analytics.menu_categories (external_id),
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    loaded_at             TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTSTYLE ALL SORTKEY (external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.menu_modifiers (
    external_id            VARCHAR(24)   NOT NULL,
    menu_item_external_id  VARCHAR(24)   REFERENCES coldstone_web_ke_analytics.menu_items (external_id),
    title                  VARCHAR(512),
    position               INTEGER,
    quantity               INTEGER,
    unit                   VARCHAR(32),
    modifier_type          VARCHAR(32),
    channel                VARCHAR(32),
    loaded_at              TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTSTYLE ALL SORTKEY (menu_item_external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.modifier_items (
    modifier_external_id   VARCHAR(24)  NOT NULL REFERENCES coldstone_web_ke_analytics.menu_modifiers (external_id),
    menu_item_external_id  VARCHAR(24)  NOT NULL REFERENCES coldstone_web_ke_analytics.menu_items (external_id),
    loaded_at              TIMESTAMP    DEFAULT GETDATE(),
    PRIMARY KEY (modifier_external_id, menu_item_external_id)
) DISTSTYLE ALL SORTKEY (modifier_external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.discounts (
    external_id              VARCHAR(24)   NOT NULL,
    code                     VARCHAR(64),
    type                     VARCHAR(64),
    target                   VARCHAR(64),
    channel                  VARCHAR(32),
    category                 VARCHAR(64),
    value                    BIGINT,
    start_date               TIMESTAMP,
    end_date                 TIMESTAMP,
    is_first_time_user_only  BOOLEAN,
    minimum_order_amount     BIGINT,
    min_quantity_per_item    INTEGER,
    partial_combo_threshold  DECIMAL(5, 2),
    usage_limit              INTEGER,
    usage_count              INTEGER,
    active                   BOOLEAN,
    partial_combo_allowed    BOOLEAN,
    customer_external_id     VARCHAR(24),
    branch_external_id       VARCHAR(24)   REFERENCES coldstone_web_ke_analytics.branches (external_id),
    created_at               TIMESTAMP,
    updated_at               TIMESTAMP,
    loaded_at                TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTSTYLE ALL SORTKEY (external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.discount_applicable_items (
    discount_external_id  VARCHAR(24)  NOT NULL REFERENCES coldstone_web_ke_analytics.discounts (external_id),
    item_external_id      VARCHAR(24)  NOT NULL,
    loaded_at             TIMESTAMP    DEFAULT GETDATE(),
    PRIMARY KEY (discount_external_id, item_external_id)
) DISTSTYLE ALL SORTKEY (discount_external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.discount_combo_items (
    discount_external_id  VARCHAR(24)  NOT NULL REFERENCES coldstone_web_ke_analytics.discounts (external_id),
    item_external_id      VARCHAR(24)  NOT NULL,
    loaded_at             TIMESTAMP    DEFAULT GETDATE(),
    PRIMARY KEY (discount_external_id, item_external_id)
) DISTSTYLE ALL SORTKEY (discount_external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.wallets (
    external_id           VARCHAR(24)   NOT NULL,
    reference             VARCHAR(64),
    customer_external_id  VARCHAR(24),
    account_external_id   VARCHAR(24),
    branch_external_id    VARCHAR(24)   REFERENCES coldstone_web_ke_analytics.branches (external_id),
    balance               BIGINT,
    current_balance       BIGINT,
    temp_balance          BIGINT,
    active                BOOLEAN,
    account_name          VARCHAR(256),
    account_number        VARCHAR(32),
    bank_name             VARCHAR(128),
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    loaded_at             TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTSTYLE ALL SORTKEY (branch_external_id);


-- ===================== transaction tables (incremental) =====================

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.customers (
    external_id           VARCHAR(24)   NOT NULL,
    first_name            VARCHAR(256),
    last_name             VARCHAR(256),
    email                 VARCHAR(256),
    phone_number          VARCHAR(32),
    customer_external_id  VARCHAR(24),
    branch_external_id    VARCHAR(24),
    active                BOOLEAN,
    unsubscribed          BOOLEAN,
    channel               VARCHAR(32),
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    loaded_at             TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTSTYLE AUTO SORTKEY (created_at);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.orders (
    external_id           VARCHAR(24)   NOT NULL,
    reference             VARCHAR(64),
    payment_method        VARCHAR(64),
    payment_status        VARCHAR(32),
    status                VARCHAR(32),
    channel               VARCHAR(32),
    subtotal              BIGINT,
    delivery_fee          BIGINT,
    total_price           BIGINT,
    phone_number          VARCHAR(32),
    branch_external_id    VARCHAR(24)   REFERENCES coldstone_web_ke_analytics.branches (external_id),
    branch_name           VARCHAR(256),
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    customer_external_id  VARCHAR(24),
    customer_name         VARCHAR(256),
    delivery_type         VARCHAR(32),
    loaded_at             TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTKEY (external_id) COMPOUND SORTKEY (created_at, branch_external_id);

-- Child rows have no id of their own: the load deletes an order's rows and re-inserts them,
-- so line_no (position within the order) is what keeps each row unique.
CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.order_items (
    order_external_id    VARCHAR(24)   NOT NULL REFERENCES coldstone_web_ke_analytics.orders (external_id),
    line_no              SMALLINT      NOT NULL,
    product_external_id  VARCHAR(24),
    product_name         VARCHAR(512),
    price                BIGINT,
    quantity             INTEGER,
    loaded_at            TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (order_external_id, line_no)
) DISTKEY (order_external_id) SORTKEY (order_external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.order_item_modifiers (
    order_external_id     VARCHAR(24)   NOT NULL REFERENCES coldstone_web_ke_analytics.orders (external_id),
    line_no               SMALLINT      NOT NULL,
    modifier_no           SMALLINT      NOT NULL,
    product_external_id   VARCHAR(24),
    modifier_external_id  VARCHAR(24),
    modifier_name         VARCHAR(512),
    price                 BIGINT,
    loaded_at             TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (order_external_id, line_no, modifier_no)
) DISTKEY (order_external_id) SORTKEY (order_external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.order_timeline (
    order_external_id  VARCHAR(24)   NOT NULL REFERENCES coldstone_web_ke_analytics.orders (external_id),
    step_no            SMALLINT      NOT NULL,
    title              VARCHAR(64),
    description        VARCHAR(1024),
    status             BOOLEAN,
    created_at         TIMESTAMP,
    updated_at         TIMESTAMP,
    loaded_at          TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (order_external_id, step_no)
) DISTKEY (order_external_id) SORTKEY (order_external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.activity_logs (
    external_id           VARCHAR(24)    NOT NULL,
    description           VARCHAR(4096),
    customer_external_id  VARCHAR(24),
    branch_external_id    VARCHAR(24),
    initiator_type        VARCHAR(32),
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    loaded_at             TIMESTAMP      DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTKEY (external_id) SORTKEY (created_at);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.activity_initiators (
    activity_external_id   VARCHAR(24)   NOT NULL REFERENCES coldstone_web_ke_analytics.activity_logs (external_id),
    initiator_external_id  VARCHAR(24)   NOT NULL,
    first_name             VARCHAR(256),
    last_name              VARCHAR(256),
    username               VARCHAR(256),
    email                  VARCHAR(256),
    role_name              VARCHAR(256),
    initiator_type         VARCHAR(32),
    loaded_at              TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (activity_external_id, initiator_external_id)
) DISTKEY (activity_external_id) SORTKEY (activity_external_id);

CREATE TABLE IF NOT EXISTS coldstone_web_ke_analytics.wallet_transactions (
    external_id           VARCHAR(24)    NOT NULL,
    reference             VARCHAR(64),
    amount                BIGINT,
    type                  VARCHAR(32),
    status                VARCHAR(32),
    customer_external_id  VARCHAR(24),
    branch_external_id    VARCHAR(24)    REFERENCES coldstone_web_ke_analytics.branches (external_id),
    description           VARCHAR(1024),
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    loaded_at             TIMESTAMP      DEFAULT GETDATE(),
    PRIMARY KEY (external_id)
) DISTSTYLE AUTO COMPOUND SORTKEY (created_at, branch_external_id);


-- ===================== staging =====================
-- No tables are created in coldstone_web_ke_staging. load_redshift.py recreates each staging table on every run with the
-- analytics columns minus loaded_at, because COPY from Parquet matches columns by position and the
-- files have no loaded_at. The MERGE sets loaded_at in coldstone_web_ke_analytics.
