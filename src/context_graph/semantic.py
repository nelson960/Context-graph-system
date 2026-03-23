from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import Engine

from context_graph.config import ALLOWED_METRICS, APPROVED_VIEWS, BUSINESS_GLOSSARY, ENTITY_CONFIGS


def _execute_script(engine: Engine, sql_script: str) -> None:
    connection = engine.raw_connection()
    try:
        connection.executescript(sql_script)
        connection.commit()
    finally:
        connection.close()


def create_sql_indexes(engine: Engine) -> None:
    index_statements = """
    CREATE INDEX IF NOT EXISTS idx_sales_orders_sales_order ON sales_orders (salesOrder);
    CREATE INDEX IF NOT EXISTS idx_sales_order_items_order_item ON sales_order_items (salesOrder, salesOrderItem);
    CREATE INDEX IF NOT EXISTS idx_delivery_items_delivery_item ON delivery_items (deliveryDocument, deliveryDocumentItem);
    CREATE INDEX IF NOT EXISTS idx_delivery_items_reference ON delivery_items (referenceSdDocument, referenceSdDocumentItem);
    CREATE INDEX IF NOT EXISTS idx_billing_items_billing_item ON billing_items (billingDocument, billingDocumentItem);
    CREATE INDEX IF NOT EXISTS idx_billing_items_reference ON billing_items (referenceSdDocument, referenceSdDocumentItem);
    CREATE INDEX IF NOT EXISTS idx_billing_documents_billing_document ON billing_documents (billingDocument);
    CREATE INDEX IF NOT EXISTS idx_journal_entries_reference ON journal_entries_ar (referenceDocument);
    CREATE INDEX IF NOT EXISTS idx_journal_entries_accounting ON journal_entries_ar (companyCode, fiscalYear, accountingDocument, accountingDocumentItem);
    CREATE INDEX IF NOT EXISTS idx_payments_accounting ON payments_ar (companyCode, fiscalYear, accountingDocument, accountingDocumentItem);
    CREATE INDEX IF NOT EXISTS idx_otd_sales_order_item ON order_to_delivery_bridge (sales_order, sales_order_item);
    CREATE INDEX IF NOT EXISTS idx_dtb_delivery_item ON delivery_to_billing_bridge (delivery_document, delivery_document_item);
    CREATE INDEX IF NOT EXISTS idx_btj_billing_document ON billing_to_journal_bridge (billing_document);
    CREATE INDEX IF NOT EXISTS idx_jtp_accounting ON journal_to_payment_bridge (company_code, fiscal_year, accounting_document, accounting_document_item);
    """
    _execute_script(engine, index_statements)


def create_sql_views(engine: Engine) -> None:
    view_sql = """
    DROP VIEW IF EXISTS v_sales_order_flow;
    CREATE VIEW v_sales_order_flow AS
    WITH delivery_agg AS (
        SELECT
            sales_order,
            sales_order_item,
            COUNT(DISTINCT delivery_document) AS delivery_count,
            GROUP_CONCAT(DISTINCT delivery_document) AS delivery_documents,
            SUM(actual_delivery_quantity) AS delivered_quantity
        FROM order_to_delivery_bridge
        WHERE link_status = 'direct'
        GROUP BY sales_order, sales_order_item
    ),
    billing_agg AS (
        SELECT
            odb.sales_order,
            odb.sales_order_item,
            COUNT(DISTINCT dtb.billing_document) AS billing_count,
            GROUP_CONCAT(DISTINCT dtb.billing_document) AS billing_documents,
            SUM(dtb.billing_quantity) AS billed_quantity,
            SUM(dtb.billing_net_amount) AS billed_net_amount
        FROM order_to_delivery_bridge AS odb
        JOIN delivery_to_billing_bridge AS dtb
          ON odb.delivery_document = dtb.delivery_document
         AND odb.delivery_document_item = dtb.delivery_document_item
         AND dtb.link_status = 'direct'
        WHERE odb.link_status = 'direct'
        GROUP BY odb.sales_order, odb.sales_order_item
    )
    SELECT
        so.salesOrder AS sales_order,
        soi.salesOrderItem AS sales_order_item,
        so.creationDate AS sales_order_created_at,
        so.requestedDeliveryDate AS requested_delivery_date,
        so.soldToParty AS customer_id,
        c.businessPartnerName AS customer_name,
        so.salesOrderType AS sales_order_type,
        so.overallDeliveryStatus AS overall_delivery_status,
        so.overallOrdReltdBillgStatus AS overall_billing_status,
        soi.material AS product_id,
        pd.productDescription AS product_description,
        soi.requestedQuantity AS requested_quantity,
        soi.requestedQuantityUnit AS requested_quantity_unit,
        soi.netAmount AS item_net_amount,
        soi.transactionCurrency AS currency,
        soi.productionPlant AS production_plant,
        delivery_agg.delivery_count,
        delivery_agg.delivery_documents,
        delivery_agg.delivered_quantity,
        billing_agg.billing_count,
        billing_agg.billing_documents,
        billing_agg.billed_quantity,
        billing_agg.billed_net_amount
    FROM sales_order_items AS soi
    JOIN sales_orders AS so
      ON so.salesOrder = soi.salesOrder
    LEFT JOIN customers AS c
      ON c.businessPartner = so.soldToParty
    LEFT JOIN product_descriptions AS pd
      ON pd.product = soi.material
     AND pd.language = 'EN'
    LEFT JOIN delivery_agg
      ON delivery_agg.sales_order = soi.salesOrder
     AND delivery_agg.sales_order_item = soi.salesOrderItem
    LEFT JOIN billing_agg
      ON billing_agg.sales_order = soi.salesOrder
     AND billing_agg.sales_order_item = soi.salesOrderItem;

    DROP VIEW IF EXISTS v_billing_trace;
    DROP VIEW IF EXISTS v_delivery_flow;
    CREATE VIEW v_delivery_flow AS
    SELECT
        d.deliveryDocument AS delivery_document,
        di.deliveryDocumentItem AS delivery_document_item,
        d.creationDate AS delivery_created_at,
        d.actualGoodsMovementDate AS actual_goods_movement_date,
        d.shippingPoint AS shipping_point,
        d.overallGoodsMovementStatus AS overall_goods_movement_status,
        d.overallPickingStatus AS overall_picking_status,
        odb.sales_order,
        odb.sales_order_item,
        so.soldToParty AS customer_id,
        c.businessPartnerName AS customer_name,
        di.plant AS plant,
        di.storageLocation AS storage_location,
        soi.material AS product_id,
        pd.productDescription AS product_description,
        dtb.billing_document,
        dtb.billing_document_item,
        dtb.billing_net_amount,
        dtb.billing_currency
    FROM deliveries AS d
    JOIN delivery_items AS di
      ON di.deliveryDocument = d.deliveryDocument
    LEFT JOIN order_to_delivery_bridge AS odb
      ON odb.delivery_document = di.deliveryDocument
     AND odb.delivery_document_item = di.deliveryDocumentItem
     AND odb.link_status = 'direct'
    LEFT JOIN sales_orders AS so
      ON so.salesOrder = odb.sales_order
    LEFT JOIN sales_order_items AS soi
      ON soi.salesOrder = odb.sales_order
     AND soi.salesOrderItem = odb.sales_order_item
    LEFT JOIN customers AS c
      ON c.businessPartner = so.soldToParty
    LEFT JOIN product_descriptions AS pd
      ON pd.product = soi.material
     AND pd.language = 'EN'
    LEFT JOIN delivery_to_billing_bridge AS dtb
      ON dtb.delivery_document = di.deliveryDocument
     AND dtb.delivery_document_item = di.deliveryDocumentItem
     AND dtb.link_status = 'direct';

    DROP VIEW IF EXISTS v_billing_flow;
    CREATE VIEW v_billing_flow AS
    SELECT
        bd.billingDocument AS billing_document,
        bi.billingDocumentItem AS billing_document_item,
        bd.billingDocumentDate AS billing_document_date,
        bd.billingDocumentType AS billing_document_type,
        bd.billingDocumentIsCancelled AS billing_document_is_cancelled,
        bd.totalNetAmount AS billing_total_amount,
        bd.transactionCurrency AS billing_currency,
        dtb.delivery_document,
        dtb.delivery_document_item,
        odb.sales_order,
        odb.sales_order_item,
        bd.soldToParty AS customer_id,
        c.businessPartnerName AS customer_name,
        bi.material AS product_id,
        pd.productDescription AS product_description,
        btj.journal_accounting_document,
        btj.journal_accounting_document_item,
        btj.journal_posting_date,
        btj.journal_amount,
        jtp.clearing_accounting_document,
        jtp.clearing_date,
        jtp.payment_amount
    FROM billing_documents AS bd
    JOIN billing_items AS bi
      ON bi.billingDocument = bd.billingDocument
    LEFT JOIN delivery_to_billing_bridge AS dtb
      ON dtb.billing_document = bi.billingDocument
     AND dtb.billing_document_item = bi.billingDocumentItem
     AND dtb.link_status = 'direct'
    LEFT JOIN order_to_delivery_bridge AS odb
      ON odb.delivery_document = dtb.delivery_document
     AND odb.delivery_document_item = dtb.delivery_document_item
     AND odb.link_status = 'direct'
    LEFT JOIN customers AS c
      ON c.businessPartner = bd.soldToParty
    LEFT JOIN product_descriptions AS pd
      ON pd.product = bi.material
     AND pd.language = 'EN'
    LEFT JOIN billing_to_journal_bridge AS btj
      ON btj.billing_document = bd.billingDocument
     AND btj.link_status = 'direct'
    LEFT JOIN journal_to_payment_bridge AS jtp
      ON jtp.company_code = btj.journal_company_code
     AND jtp.fiscal_year = btj.journal_fiscal_year
     AND jtp.accounting_document = btj.journal_accounting_document
     AND jtp.accounting_document_item = btj.journal_accounting_document_item
     AND jtp.link_status = 'direct';

    DROP VIEW IF EXISTS v_billing_trace;
    CREATE VIEW v_billing_trace AS
    SELECT
        bd.billingDocument AS billing_document,
        bi.billingDocumentItem AS billing_document_item,
        bd.billingDocumentDate AS billing_document_date,
        bd.totalNetAmount AS billing_total_amount,
        bd.transactionCurrency AS billing_currency,
        bi.material AS product_id,
        pd.productDescription AS product_description,
        dtb.delivery_document,
        dtb.delivery_document_item,
        odb.sales_order,
        odb.sales_order_item,
        so.soldToParty AS customer_id,
        c.businessPartnerName AS customer_name,
        btj.journal_company_code,
        btj.journal_fiscal_year,
        btj.journal_accounting_document,
        btj.journal_accounting_document_item,
        btj.journal_posting_date,
        btj.journal_amount,
        btj.journal_currency,
        jtp.clearing_accounting_document,
        jtp.clearing_date,
        jtp.payment_amount,
        jtp.payment_currency
    FROM billing_items AS bi
    JOIN billing_documents AS bd
      ON bd.billingDocument = bi.billingDocument
    LEFT JOIN delivery_to_billing_bridge AS dtb
      ON dtb.billing_document = bi.billingDocument
     AND dtb.billing_document_item = bi.billingDocumentItem
     AND dtb.link_status = 'direct'
    LEFT JOIN order_to_delivery_bridge AS odb
      ON odb.delivery_document = dtb.delivery_document
     AND odb.delivery_document_item = dtb.delivery_document_item
     AND odb.link_status = 'direct'
    LEFT JOIN sales_orders AS so
      ON so.salesOrder = odb.sales_order
    LEFT JOIN customers AS c
      ON c.businessPartner = so.soldToParty
    LEFT JOIN product_descriptions AS pd
      ON pd.product = bi.material
     AND pd.language = 'EN'
    LEFT JOIN billing_to_journal_bridge AS btj
      ON btj.billing_document = bd.billingDocument
     AND btj.link_status = 'direct'
    LEFT JOIN journal_to_payment_bridge AS jtp
      ON jtp.company_code = btj.journal_company_code
     AND jtp.fiscal_year = btj.journal_fiscal_year
     AND jtp.accounting_document = btj.journal_accounting_document
     AND jtp.accounting_document_item = btj.journal_accounting_document_item
     AND jtp.link_status = 'direct';

    DROP VIEW IF EXISTS v_financial_flow;
    CREATE VIEW v_financial_flow AS
    SELECT
        bd.billingDocument AS billing_document,
        bd.billingDocumentDate AS billing_document_date,
        bd.soldToParty AS customer_id,
        c.businessPartnerName AS customer_name,
        bd.totalNetAmount AS billing_total_amount,
        bd.transactionCurrency AS billing_currency,
        btj.journal_company_code,
        btj.journal_fiscal_year,
        btj.journal_accounting_document,
        btj.journal_accounting_document_item,
        btj.journal_posting_date,
        btj.journal_amount,
        btj.journal_currency,
        jtp.clearing_accounting_document,
        jtp.clearing_date,
        jtp.payment_amount,
        jtp.payment_currency,
        CASE WHEN btj.journal_accounting_document IS NOT NULL THEN 1 ELSE 0 END AS has_journal_entry,
        CASE WHEN jtp.payment_customer IS NOT NULL THEN 1 ELSE 0 END AS has_payment
    FROM billing_documents AS bd
    LEFT JOIN customers AS c
      ON c.businessPartner = bd.soldToParty
    LEFT JOIN billing_to_journal_bridge AS btj
      ON btj.billing_document = bd.billingDocument
     AND btj.link_status = 'direct'
    LEFT JOIN journal_to_payment_bridge AS jtp
      ON jtp.company_code = btj.journal_company_code
     AND jtp.fiscal_year = btj.journal_fiscal_year
     AND jtp.accounting_document = btj.journal_accounting_document
     AND jtp.accounting_document_item = btj.journal_accounting_document_item
     AND jtp.link_status = 'direct';

    DROP VIEW IF EXISTS v_product_billing_summary;
    CREATE VIEW v_product_billing_summary AS
    SELECT
        bi.material AS product_id,
        pd.productDescription AS product_description,
        COUNT(DISTINCT bi.billingDocument) AS distinct_billing_documents,
        COUNT(*) AS billing_item_count,
        SUM(bi.billingQuantity) AS total_billed_quantity,
        SUM(bi.netAmount) AS total_billed_amount,
        MIN(bd.billingDocumentDate) AS first_billing_date,
        MAX(bd.billingDocumentDate) AS last_billing_date
    FROM billing_items AS bi
    LEFT JOIN billing_documents AS bd
      ON bd.billingDocument = bi.billingDocument
    LEFT JOIN product_descriptions AS pd
      ON pd.product = bi.material
     AND pd.language = 'EN'
    GROUP BY bi.material, pd.productDescription;

    DROP VIEW IF EXISTS v_customer_360;
    CREATE VIEW v_customer_360 AS
    WITH order_agg AS (
        SELECT
            soldToParty AS customer_id,
            COUNT(DISTINCT salesOrder) AS sales_order_count,
            SUM(totalNetAmount) AS total_sales_order_amount
        FROM sales_orders
        GROUP BY soldToParty
    ),
    delivery_agg AS (
        SELECT
            so.soldToParty AS customer_id,
            COUNT(DISTINCT odb.delivery_document) AS delivery_count
        FROM order_to_delivery_bridge AS odb
        JOIN sales_orders AS so
          ON so.salesOrder = odb.sales_order
        WHERE odb.link_status = 'direct'
        GROUP BY so.soldToParty
    ),
    billing_agg AS (
        SELECT
            soldToParty AS customer_id,
            COUNT(DISTINCT billingDocument) AS billing_document_count,
            SUM(totalNetAmount) AS total_billed_amount
        FROM billing_documents
        GROUP BY soldToParty
    ),
    journal_agg AS (
        SELECT
            customer AS customer_id,
            COUNT(DISTINCT accountingDocument) AS journal_entry_count,
            SUM(amountInTransactionCurrency) AS total_journal_amount
        FROM journal_entries_ar
        GROUP BY customer
    ),
    payment_agg AS (
        SELECT
            customer AS customer_id,
            COUNT(DISTINCT accountingDocument) AS payment_count,
            SUM(amountInTransactionCurrency) AS total_payment_amount
        FROM payments_ar
        GROUP BY customer
    )
    SELECT
        c.businessPartner AS customer_id,
        c.customer AS customer_number,
        c.businessPartnerName AS customer_name,
        c.businessPartnerCategory AS customer_category,
        a.cityName AS city_name,
        a.country AS country,
        order_agg.sales_order_count,
        order_agg.total_sales_order_amount,
        delivery_agg.delivery_count,
        billing_agg.billing_document_count,
        billing_agg.total_billed_amount,
        journal_agg.journal_entry_count,
        journal_agg.total_journal_amount,
        payment_agg.payment_count,
        payment_agg.total_payment_amount
    FROM customers AS c
    LEFT JOIN addresses AS a
      ON a.businessPartner = c.businessPartner
    LEFT JOIN order_agg
      ON order_agg.customer_id = c.businessPartner
    LEFT JOIN delivery_agg
      ON delivery_agg.customer_id = c.businessPartner
    LEFT JOIN billing_agg
      ON billing_agg.customer_id = c.businessPartner
    LEFT JOIN journal_agg
      ON journal_agg.customer_id = c.businessPartner
    LEFT JOIN payment_agg
      ON payment_agg.customer_id = c.businessPartner;

    DROP VIEW IF EXISTS v_incomplete_order_flows;
    CREATE VIEW v_incomplete_order_flows AS
    WITH delivery_agg AS (
        SELECT
            sales_order,
            sales_order_item,
            COUNT(DISTINCT delivery_document) AS delivery_count,
            SUM(actual_delivery_quantity) AS delivered_quantity
        FROM order_to_delivery_bridge
        WHERE link_status = 'direct'
        GROUP BY sales_order, sales_order_item
    ),
    billing_agg AS (
        SELECT
            odb.sales_order,
            odb.sales_order_item,
            COUNT(DISTINCT dtb.billing_document) AS billing_count,
            SUM(dtb.billing_quantity) AS billed_quantity
        FROM order_to_delivery_bridge AS odb
        JOIN delivery_to_billing_bridge AS dtb
          ON dtb.delivery_document = odb.delivery_document
         AND dtb.delivery_document_item = odb.delivery_document_item
         AND dtb.link_status = 'direct'
        WHERE odb.link_status = 'direct'
        GROUP BY odb.sales_order, odb.sales_order_item
    ),
    journal_agg AS (
        SELECT
            odb.sales_order,
            odb.sales_order_item,
            COUNT(DISTINCT btj.journal_accounting_document) AS journal_count
        FROM order_to_delivery_bridge AS odb
        JOIN delivery_to_billing_bridge AS dtb
          ON dtb.delivery_document = odb.delivery_document
         AND dtb.delivery_document_item = odb.delivery_document_item
         AND dtb.link_status = 'direct'
        JOIN billing_to_journal_bridge AS btj
          ON btj.billing_document = dtb.billing_document
         AND btj.link_status = 'direct'
        WHERE odb.link_status = 'direct'
        GROUP BY odb.sales_order, odb.sales_order_item
    ),
    payment_agg AS (
        SELECT
            odb.sales_order,
            odb.sales_order_item,
            COUNT(DISTINCT jtp.accounting_document) AS payment_count
        FROM order_to_delivery_bridge AS odb
        JOIN delivery_to_billing_bridge AS dtb
          ON dtb.delivery_document = odb.delivery_document
         AND dtb.delivery_document_item = odb.delivery_document_item
         AND dtb.link_status = 'direct'
        JOIN billing_to_journal_bridge AS btj
          ON btj.billing_document = dtb.billing_document
         AND btj.link_status = 'direct'
        JOIN journal_to_payment_bridge AS jtp
          ON jtp.company_code = btj.journal_company_code
         AND jtp.fiscal_year = btj.journal_fiscal_year
         AND jtp.accounting_document = btj.journal_accounting_document
         AND jtp.accounting_document_item = btj.journal_accounting_document_item
         AND jtp.link_status = 'direct'
        WHERE odb.link_status = 'direct'
        GROUP BY odb.sales_order, odb.sales_order_item
    )
    SELECT
        so.salesOrder AS sales_order,
        soi.salesOrderItem AS sales_order_item,
        so.soldToParty AS customer_id,
        soi.material AS product_id,
        soi.requestedQuantity AS requested_quantity,
        delivery_agg.delivered_quantity,
        billing_agg.billed_quantity,
        COALESCE(delivery_agg.delivery_count, 0) AS delivery_count,
        COALESCE(billing_agg.billing_count, 0) AS billing_count,
        COALESCE(journal_agg.journal_count, 0) AS journal_count,
        COALESCE(payment_agg.payment_count, 0) AS payment_count,
        CASE WHEN COALESCE(delivery_agg.delivery_count, 0) > 0 THEN 1 ELSE 0 END AS has_delivery,
        CASE WHEN COALESCE(billing_agg.billing_count, 0) > 0 THEN 1 ELSE 0 END AS has_billing,
        CASE WHEN COALESCE(journal_agg.journal_count, 0) > 0 THEN 1 ELSE 0 END AS has_journal,
        CASE WHEN COALESCE(payment_agg.payment_count, 0) > 0 THEN 1 ELSE 0 END AS has_payment,
        CASE
            WHEN COALESCE(delivery_agg.delivery_count, 0) > 0 AND COALESCE(billing_agg.billing_count, 0) = 0 THEN 'delivered_not_billed'
            WHEN COALESCE(billing_agg.billing_count, 0) > 0 AND COALESCE(journal_agg.journal_count, 0) = 0 THEN 'billed_not_posted'
            WHEN COALESCE(journal_agg.journal_count, 0) > 0 AND COALESCE(payment_agg.payment_count, 0) = 0 THEN 'posted_not_paid'
            WHEN COALESCE(delivery_agg.delivered_quantity, 0) <> COALESCE(soi.requestedQuantity, 0) THEN 'quantity_mismatch'
            ELSE NULL
        END AS primary_anomaly
    FROM sales_order_items AS soi
    JOIN sales_orders AS so
      ON so.salesOrder = soi.salesOrder
    LEFT JOIN delivery_agg
      ON delivery_agg.sales_order = soi.salesOrder
     AND delivery_agg.sales_order_item = soi.salesOrderItem
    LEFT JOIN billing_agg
      ON billing_agg.sales_order = soi.salesOrder
     AND billing_agg.sales_order_item = soi.salesOrderItem
    LEFT JOIN journal_agg
      ON journal_agg.sales_order = soi.salesOrder
     AND journal_agg.sales_order_item = soi.salesOrderItem
    LEFT JOIN payment_agg
      ON payment_agg.sales_order = soi.salesOrder
     AND payment_agg.sales_order_item = soi.salesOrderItem;
    """
    _execute_script(engine, view_sql)


def build_semantic_catalog() -> dict[str, Any]:
    return {
        "glossary": BUSINESS_GLOSSARY,
        "entities": {
            config.canonical_name: {
                "raw_name": config.raw_name,
                "primary_key": list(config.primary_key),
                "id_columns": sorted(config.id_columns),
                "numeric_columns": sorted(config.numeric_columns),
                "datetime_columns": sorted(config.datetime_columns),
                "time_columns": sorted(config.time_columns),
                "bool_columns": sorted(config.bool_columns),
            }
            for config in ENTITY_CONFIGS.values()
        },
        "approved_views": APPROVED_VIEWS,
        "allowed_metrics": list(ALLOWED_METRICS),
    }


def run_sql_query(engine: Engine, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, engine)


def write_semantic_catalog(output_path: str | Path) -> None:
    catalog = build_semantic_catalog()
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(catalog, indent=2, sort_keys=True), encoding="utf-8")
