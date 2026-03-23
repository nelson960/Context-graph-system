from __future__ import annotations

from typing import Any

import pandas as pd


def _add_bridge_metadata(frame: pd.DataFrame, link_status: pd.Series, derivation_rule: str, provenance_columns: str) -> pd.DataFrame:
    enriched = frame.copy()
    enriched["link_status"] = link_status
    enriched["derivation_rule"] = derivation_rule
    enriched["provenance_columns"] = provenance_columns
    return enriched


def build_order_to_delivery_bridge(canonical_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    order_items = canonical_frames["sales_order_items"][
        ["salesOrder", "salesOrderItem", "material", "requestedQuantity", "requestedQuantityUnit"]
    ].copy()
    delivery_items = canonical_frames["delivery_items"][
        [
            "deliveryDocument",
            "deliveryDocumentItem",
            "referenceSdDocument",
            "referenceSdDocumentItem",
            "actualDeliveryQuantity",
            "deliveryQuantityUnit",
            "plant",
            "storageLocation",
        ]
    ].copy()
    merged = order_items.merge(
        delivery_items,
        left_on=["salesOrder", "salesOrderItem"],
        right_on=["referenceSdDocument", "referenceSdDocumentItem"],
        how="left",
    )
    bridge = merged.rename(
        columns={
            "salesOrder": "sales_order",
            "salesOrderItem": "sales_order_item",
            "deliveryDocument": "delivery_document",
            "deliveryDocumentItem": "delivery_document_item",
            "material": "product",
            "requestedQuantity": "requested_quantity",
            "requestedQuantityUnit": "requested_quantity_unit",
            "actualDeliveryQuantity": "actual_delivery_quantity",
            "deliveryQuantityUnit": "delivery_quantity_unit",
            "plant": "plant",
            "storageLocation": "storage_location",
        }
    )
    link_status = bridge["delivery_document"].notna().map({True: "direct", False: "unresolved"})
    return _add_bridge_metadata(
        bridge,
        link_status=link_status,
        derivation_rule="delivery_items.referenceSdDocument/referenceSdDocumentItem -> sales_order_items.salesOrder/salesOrderItem",
        provenance_columns="referenceSdDocument,referenceSdDocumentItem,salesOrder,salesOrderItem",
    )


def build_delivery_to_billing_bridge(canonical_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    delivery_items = canonical_frames["delivery_items"][
        [
            "deliveryDocument",
            "deliveryDocumentItem",
            "referenceSdDocument",
            "referenceSdDocumentItem",
            "actualDeliveryQuantity",
            "deliveryQuantityUnit",
        ]
    ].copy()
    billing_items = canonical_frames["billing_items"][
        [
            "billingDocument",
            "billingDocumentItem",
            "material",
            "billingQuantity",
            "billingQuantityUnit",
            "netAmount",
            "transactionCurrency",
            "referenceSdDocument",
            "referenceSdDocumentItem",
        ]
    ].copy()
    merged = delivery_items.merge(
        billing_items,
        left_on=["deliveryDocument", "deliveryDocumentItem"],
        right_on=["referenceSdDocument", "referenceSdDocumentItem"],
        how="left",
        suffixes=("_delivery", "_billing"),
    )
    bridge = merged.rename(
        columns={
            "deliveryDocument": "delivery_document",
            "deliveryDocumentItem": "delivery_document_item",
            "referenceSdDocument_delivery": "sales_order",
            "referenceSdDocumentItem_delivery": "sales_order_item",
            "billingDocument": "billing_document",
            "billingDocumentItem": "billing_document_item",
            "material": "product",
            "billingQuantity": "billing_quantity",
            "billingQuantityUnit": "billing_quantity_unit",
            "netAmount": "billing_net_amount",
            "transactionCurrency": "billing_currency",
            "actualDeliveryQuantity": "actual_delivery_quantity",
            "deliveryQuantityUnit": "delivery_quantity_unit",
        }
    )
    link_status = bridge["billing_document"].notna().map({True: "direct", False: "unresolved"})
    return _add_bridge_metadata(
        bridge,
        link_status=link_status,
        derivation_rule="billing_items.referenceSdDocument/referenceSdDocumentItem -> delivery_items.deliveryDocument/deliveryDocumentItem",
        provenance_columns="referenceSdDocument,referenceSdDocumentItem,deliveryDocument,deliveryDocumentItem",
    )


def build_billing_to_journal_bridge(canonical_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    billings = canonical_frames["billing_documents"][
        ["billingDocument", "companyCode", "fiscalYear", "accountingDocument", "soldToParty"]
    ].copy()
    journals = canonical_frames["journal_entries_ar"][
        [
            "companyCode",
            "fiscalYear",
            "accountingDocument",
            "accountingDocumentItem",
            "referenceDocument",
            "customer",
            "postingDate",
            "documentDate",
            "amountInTransactionCurrency",
            "transactionCurrency",
            "clearingAccountingDocument",
            "clearingDate",
        ]
    ].copy()
    merged = billings.merge(
        journals,
        left_on="billingDocument",
        right_on="referenceDocument",
        how="left",
        suffixes=("_billing", "_journal"),
    )
    bridge = merged.rename(
        columns={
            "billingDocument": "billing_document",
            "companyCode_billing": "billing_company_code",
            "fiscalYear_billing": "billing_fiscal_year",
            "accountingDocument_billing": "billing_accounting_document",
            "soldToParty": "sold_to_party",
            "companyCode_journal": "journal_company_code",
            "fiscalYear_journal": "journal_fiscal_year",
            "accountingDocument_journal": "journal_accounting_document",
            "accountingDocumentItem": "journal_accounting_document_item",
            "referenceDocument": "journal_reference_document",
            "customer": "journal_customer",
            "postingDate": "journal_posting_date",
            "documentDate": "journal_document_date",
            "amountInTransactionCurrency": "journal_amount",
            "transactionCurrency": "journal_currency",
            "clearingAccountingDocument": "journal_clearing_accounting_document",
            "clearingDate": "journal_clearing_date",
        }
    )
    link_status = bridge["journal_accounting_document"].notna().map({True: "direct", False: "unresolved"})
    return _add_bridge_metadata(
        bridge,
        link_status=link_status,
        derivation_rule="journal_entries_ar.referenceDocument -> billing_documents.billingDocument",
        provenance_columns="referenceDocument,billingDocument",
    )


def build_journal_to_payment_bridge(canonical_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    journals = canonical_frames["journal_entries_ar"][
        [
            "companyCode",
            "fiscalYear",
            "accountingDocument",
            "accountingDocumentItem",
            "referenceDocument",
            "customer",
            "amountInTransactionCurrency",
            "transactionCurrency",
        ]
    ].copy()
    payments = canonical_frames["payments_ar"][
        [
            "companyCode",
            "fiscalYear",
            "accountingDocument",
            "accountingDocumentItem",
            "clearingAccountingDocument",
            "clearingDate",
            "customer",
            "amountInTransactionCurrency",
            "transactionCurrency",
        ]
    ].copy()
    merged = journals.merge(
        payments,
        on=["companyCode", "fiscalYear", "accountingDocument", "accountingDocumentItem"],
        how="left",
        suffixes=("_journal", "_payment"),
    )
    bridge = merged.rename(
        columns={
            "companyCode": "company_code",
            "fiscalYear": "fiscal_year",
            "accountingDocument": "accounting_document",
            "accountingDocumentItem": "accounting_document_item",
            "referenceDocument": "billing_document",
            "customer_journal": "journal_customer",
            "amountInTransactionCurrency_journal": "journal_amount",
            "transactionCurrency_journal": "journal_currency",
            "customer_payment": "payment_customer",
            "clearingAccountingDocument": "clearing_accounting_document",
            "clearingDate": "clearing_date",
            "amountInTransactionCurrency_payment": "payment_amount",
            "transactionCurrency_payment": "payment_currency",
        }
    )
    link_status = bridge["payment_customer"].notna().map({True: "direct", False: "unresolved"})
    return _add_bridge_metadata(
        bridge,
        link_status=link_status,
        derivation_rule="payments_ar.companyCode/fiscalYear/accountingDocument/accountingDocumentItem -> journal_entries_ar.companyCode/fiscalYear/accountingDocument/accountingDocumentItem",
        provenance_columns="companyCode,fiscalYear,accountingDocument,accountingDocumentItem",
    )


def build_all_bridges(canonical_frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    return {
        "order_to_delivery_bridge": build_order_to_delivery_bridge(canonical_frames),
        "delivery_to_billing_bridge": build_delivery_to_billing_bridge(canonical_frames),
        "billing_to_journal_bridge": build_billing_to_journal_bridge(canonical_frames),
        "journal_to_payment_bridge": build_journal_to_payment_bridge(canonical_frames),
    }


def build_bridge_coverage_report(bridges: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for bridge_name, frame in bridges.items():
        status_counts = frame["link_status"].value_counts(dropna=False).to_dict()
        rows.append(
            {
                "bridge_name": bridge_name,
                "row_count": len(frame),
                "direct_count": int(status_counts.get("direct", 0)),
                "inferred_count": int(status_counts.get("inferred", 0)),
                "unresolved_count": int(status_counts.get("unresolved", 0)),
                "coverage_ratio": round(
                    float(status_counts.get("direct", 0) + status_counts.get("inferred", 0)) / float(len(frame)),
                    6,
                )
                if len(frame)
                else 0.0,
            }
        )
    return pd.DataFrame(rows).sort_values("bridge_name").reset_index(drop=True)
