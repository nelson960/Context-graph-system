from __future__ import annotations

import json
from collections import deque
from typing import Any, Iterable

import networkx as nx
import pandas as pd
import plotly.graph_objects as go


ENTITY_STYLE = {
    "SalesOrder": {"color": "#1f77b4", "symbol": "square", "family": "commercial", "order": 1},
    "SalesOrderItem": {"color": "#6baed6", "symbol": "square-open", "family": "commercial", "order": 2},
    "ScheduleLine": {"color": "#9ecae1", "symbol": "square-dot", "family": "commercial", "order": 3},
    "Delivery": {"color": "#ff7f0e", "symbol": "square", "family": "commercial", "order": 4},
    "DeliveryItem": {"color": "#ffbb78", "symbol": "square-open", "family": "commercial", "order": 5},
    "BillingDocument": {"color": "#d62728", "symbol": "square", "family": "commercial", "order": 6},
    "BillingItem": {"color": "#ff9896", "symbol": "square-open", "family": "commercial", "order": 7},
    "JournalEntry": {"color": "#9467bd", "symbol": "diamond", "family": "finance", "order": 8},
    "Payment": {"color": "#8c564b", "symbol": "diamond", "family": "finance", "order": 9},
    "Customer": {"color": "#2ca02c", "symbol": "circle", "family": "master", "order": 0},
    "Address": {"color": "#98df8a", "symbol": "circle-open", "family": "master", "order": -1},
    "Product": {"color": "#17becf", "symbol": "hexagon", "family": "master", "order": 2},
    "Plant": {"color": "#bcbd22", "symbol": "triangle-up", "family": "master", "order": 3},
    "StorageLocation": {"color": "#dbdb8d", "symbol": "triangle-up-open", "family": "master", "order": 4},
    "CompanyCode": {"color": "#7f7f7f", "symbol": "diamond-wide", "family": "finance", "order": 7},
    "SalesArea": {"color": "#c7c7c7", "symbol": "hexagon2", "family": "master", "order": 1},
}

HIDDEN_BY_DEFAULT = {"SalesOrderItem", "ScheduleLine", "DeliveryItem", "BillingItem", "StorageLocation"}


def _clean_metadata(row: pd.Series) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for key, value in row.items():
        if key == "raw_payload":
            continue
        if pd.isna(value):
            continue
        if isinstance(value, pd.Timestamp):
            metadata[key] = value.isoformat()
        else:
            metadata[key] = value
    return metadata


def _node_record(
    node_id: str,
    node_type: str,
    business_key: str,
    display_label: str,
    subtitle: str | None,
    document_date: Any,
    status: str | None,
    amount: Any,
    currency: str | None,
    source_tables: list[str],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "node_type": node_type,
        "business_key": business_key,
        "display_label": display_label,
        "subtitle": subtitle,
        "document_date": document_date,
        "status": status,
        "amount": amount,
        "currency": currency,
        "source_tables": ",".join(source_tables),
        "default_visible": 0 if node_type in HIDDEN_BY_DEFAULT else 1,
        "metadata_json": json.dumps(metadata, sort_keys=True),
    }


def _edge_record(
    edge_id: str,
    edge_type: str,
    source_id: str,
    target_id: str,
    link_status: str,
    derivation_rule: str,
    provenance_columns: str,
    quantity: Any = None,
    edge_date: Any = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "edge_id": edge_id,
        "edge_type": edge_type,
        "source_id": source_id,
        "target_id": target_id,
        "link_status": link_status,
        "derivation_rule": derivation_rule,
        "provenance_columns": provenance_columns,
        "quantity": quantity,
        "edge_date": edge_date,
        "metadata_json": json.dumps(metadata or {}, sort_keys=True),
    }


def _composite_business_key(parts: Iterable[Any]) -> str:
    return ":".join(str(part) for part in parts if part is not None)


def build_graph_tables(
    canonical_frames: dict[str, pd.DataFrame],
    bridges: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []

    for _, row in canonical_frames["sales_orders"].iterrows():
        nodes.append(
            _node_record(
                node_id=f"sales_order:{row.salesOrder}",
                node_type="SalesOrder",
                business_key=row.salesOrder,
                display_label=row.salesOrder,
                subtitle=row.salesOrderType,
                document_date=row.creationDate,
                status=f"delivery={row.overallDeliveryStatus or 'NA'} | billing={row.overallOrdReltdBillgStatus or 'NA'}",
                amount=row.totalNetAmount,
                currency=row.transactionCurrency,
                source_tables=["sales_orders"],
                metadata=_clean_metadata(row),
            )
        )
    for _, row in canonical_frames["sales_order_items"].iterrows():
        node_id = f"sales_order_item:{row.salesOrder}:{row.salesOrderItem}"
        nodes.append(
            _node_record(
                node_id=node_id,
                node_type="SalesOrderItem",
                business_key=_composite_business_key([row.salesOrder, row.salesOrderItem]),
                display_label=f"{row.salesOrder}/{row.salesOrderItem}",
                subtitle=row.material,
                document_date=None,
                status=row.salesDocumentRjcnReason,
                amount=row.netAmount,
                currency=row.transactionCurrency,
                source_tables=["sales_order_items"],
                metadata=_clean_metadata(row),
            )
        )
        edges.append(
            _edge_record(
                edge_id=f"has_item:{row.salesOrder}:{row.salesOrderItem}",
                edge_type="HAS_ITEM",
                source_id=f"sales_order:{row.salesOrder}",
                target_id=node_id,
                link_status="direct",
                derivation_rule="sales_order_items.salesOrder -> sales_orders.salesOrder",
                provenance_columns="salesOrder,salesOrderItem",
                metadata={"sales_order": row.salesOrder, "sales_order_item": row.salesOrderItem},
            )
        )
        if row.material:
            edges.append(
                _edge_record(
                    edge_id=f"refers_to_product:{row.salesOrder}:{row.salesOrderItem}:{row.material}",
                    edge_type="REFERS_TO_PRODUCT",
                    source_id=node_id,
                    target_id=f"product:{row.material}",
                    link_status="direct",
                    derivation_rule="sales_order_items.material -> products.product",
                    provenance_columns="material,product",
                )
            )
        if row.productionPlant:
            edges.append(
                _edge_record(
                    edge_id=f"shipped_from_order_item:{row.salesOrder}:{row.salesOrderItem}:{row.productionPlant}",
                    edge_type="SHIPPED_FROM",
                    source_id=node_id,
                    target_id=f"plant:{row.productionPlant}",
                    link_status="direct",
                    derivation_rule="sales_order_items.productionPlant -> plants.plant",
                    provenance_columns="productionPlant,plant",
                )
            )

    for _, row in canonical_frames["schedule_lines"].iterrows():
        node_id = f"schedule_line:{row.salesOrder}:{row.salesOrderItem}:{row.scheduleLine}"
        nodes.append(
            _node_record(
                node_id=node_id,
                node_type="ScheduleLine",
                business_key=_composite_business_key([row.salesOrder, row.salesOrderItem, row.scheduleLine]),
                display_label=f"{row.salesOrder}/{row.salesOrderItem}/{row.scheduleLine}",
                subtitle="schedule",
                document_date=row.confirmedDeliveryDate,
                status=None,
                amount=row.confdOrderQtyByMatlAvailCheck,
                currency=None,
                source_tables=["schedule_lines"],
                metadata=_clean_metadata(row),
            )
        )
        edges.append(
            _edge_record(
                edge_id=f"has_schedule_line:{row.salesOrder}:{row.salesOrderItem}:{row.scheduleLine}",
                edge_type="HAS_SCHEDULE_LINE",
                source_id=f"sales_order_item:{row.salesOrder}:{row.salesOrderItem}",
                target_id=node_id,
                link_status="direct",
                derivation_rule="schedule_lines.salesOrder/salesOrderItem -> sales_order_items.salesOrder/salesOrderItem",
                provenance_columns="salesOrder,salesOrderItem,scheduleLine",
            )
        )

    for _, row in canonical_frames["deliveries"].iterrows():
        nodes.append(
            _node_record(
                node_id=f"delivery:{row.deliveryDocument}",
                node_type="Delivery",
                business_key=row.deliveryDocument,
                display_label=row.deliveryDocument,
                subtitle=row.shippingPoint,
                document_date=row.creationDate,
                status=f"goods_movement={row.overallGoodsMovementStatus or 'NA'} | picking={row.overallPickingStatus or 'NA'}",
                amount=None,
                currency=None,
                source_tables=["deliveries"],
                metadata=_clean_metadata(row),
            )
        )
    for _, row in canonical_frames["delivery_items"].iterrows():
        node_id = f"delivery_item:{row.deliveryDocument}:{row.deliveryDocumentItem}"
        nodes.append(
            _node_record(
                node_id=node_id,
                node_type="DeliveryItem",
                business_key=_composite_business_key([row.deliveryDocument, row.deliveryDocumentItem]),
                display_label=f"{row.deliveryDocument}/{row.deliveryDocumentItem}",
                subtitle=row.referenceSdDocument,
                document_date=row.lastChangeDate,
                status=row.itemBillingBlockReason,
                amount=row.actualDeliveryQuantity,
                currency=None,
                source_tables=["delivery_items"],
                metadata=_clean_metadata(row),
            )
        )
        edges.append(
            _edge_record(
                edge_id=f"part_of_delivery:{row.deliveryDocument}:{row.deliveryDocumentItem}",
                edge_type="PART_OF_DELIVERY",
                source_id=node_id,
                target_id=f"delivery:{row.deliveryDocument}",
                link_status="direct",
                derivation_rule="delivery_items.deliveryDocument -> deliveries.deliveryDocument",
                provenance_columns="deliveryDocument,deliveryDocumentItem",
                quantity=row.actualDeliveryQuantity,
            )
        )

    for _, row in canonical_frames["billing_documents"].iterrows():
        nodes.append(
            _node_record(
                node_id=f"billing_document:{row.billingDocument}",
                node_type="BillingDocument",
                business_key=row.billingDocument,
                display_label=row.billingDocument,
                subtitle=row.billingDocumentType,
                document_date=row.billingDocumentDate,
                status="cancelled" if row.billingDocumentIsCancelled else "active",
                amount=row.totalNetAmount,
                currency=row.transactionCurrency,
                source_tables=["billing_documents"],
                metadata=_clean_metadata(row),
            )
        )
    for _, row in canonical_frames["billing_items"].iterrows():
        node_id = f"billing_item:{row.billingDocument}:{row.billingDocumentItem}"
        nodes.append(
            _node_record(
                node_id=node_id,
                node_type="BillingItem",
                business_key=_composite_business_key([row.billingDocument, row.billingDocumentItem]),
                display_label=f"{row.billingDocument}/{row.billingDocumentItem}",
                subtitle=row.material,
                document_date=None,
                status=None,
                amount=row.netAmount,
                currency=row.transactionCurrency,
                source_tables=["billing_items"],
                metadata=_clean_metadata(row),
            )
        )
        edges.append(
            _edge_record(
                edge_id=f"part_of_billing:{row.billingDocument}:{row.billingDocumentItem}",
                edge_type="PART_OF_BILLING",
                source_id=node_id,
                target_id=f"billing_document:{row.billingDocument}",
                link_status="direct",
                derivation_rule="billing_items.billingDocument -> billing_documents.billingDocument",
                provenance_columns="billingDocument,billingDocumentItem",
                quantity=row.billingQuantity,
            )
        )

    for _, row in canonical_frames["journal_entries_ar"].iterrows():
        node_id = (
            f"journal_entry:{row.companyCode}:{row.fiscalYear}:{row.accountingDocument}:{row.accountingDocumentItem}"
        )
        nodes.append(
            _node_record(
                node_id=node_id,
                node_type="JournalEntry",
                business_key=_composite_business_key(
                    [row.companyCode, row.fiscalYear, row.accountingDocument, row.accountingDocumentItem]
                ),
                display_label=row.accountingDocument,
                subtitle=row.referenceDocument,
                document_date=row.postingDate,
                status=row.accountingDocumentType,
                amount=row.amountInTransactionCurrency,
                currency=row.transactionCurrency,
                source_tables=["journal_entries_ar"],
                metadata=_clean_metadata(row),
            )
        )
    for _, row in canonical_frames["payments_ar"].iterrows():
        node_id = f"payment:{row.companyCode}:{row.fiscalYear}:{row.accountingDocument}:{row.accountingDocumentItem}"
        nodes.append(
            _node_record(
                node_id=node_id,
                node_type="Payment",
                business_key=_composite_business_key(
                    [row.companyCode, row.fiscalYear, row.accountingDocument, row.accountingDocumentItem]
                ),
                display_label=row.accountingDocument,
                subtitle=row.clearingAccountingDocument,
                document_date=row.clearingDate,
                status="cleared" if row.clearingAccountingDocument else "open",
                amount=row.amountInTransactionCurrency,
                currency=row.transactionCurrency,
                source_tables=["payments_ar"],
                metadata=_clean_metadata(row),
            )
        )

    for _, row in canonical_frames["customers"].iterrows():
        nodes.append(
            _node_record(
                node_id=f"customer:{row.businessPartner}",
                node_type="Customer",
                business_key=row.businessPartner,
                display_label=row.businessPartnerName or row.businessPartnerFullName or row.businessPartner,
                subtitle=row["customer"],
                document_date=row.creationDate,
                status="blocked" if row.businessPartnerIsBlocked else "active",
                amount=None,
                currency=None,
                source_tables=["customers"],
                metadata=_clean_metadata(row),
            )
        )
    for _, row in canonical_frames["addresses"].iterrows():
        nodes.append(
            _node_record(
                node_id=f"address:{row.businessPartner}:{row.addressId}",
                node_type="Address",
                business_key=_composite_business_key([row.businessPartner, row.addressId]),
                display_label=row.cityName or row.addressId,
                subtitle=row["country"],
                document_date=row.validityStartDate,
                status=None,
                amount=None,
                currency=None,
                source_tables=["addresses"],
                metadata=_clean_metadata(row),
            )
        )
        edges.append(
            _edge_record(
                edge_id=f"has_address:{row.businessPartner}:{row.addressId}",
                edge_type="HAS_ADDRESS",
                source_id=f"customer:{row.businessPartner}",
                target_id=f"address:{row.businessPartner}:{row.addressId}",
                link_status="direct",
                derivation_rule="addresses.businessPartner -> customers.businessPartner",
                provenance_columns="businessPartner,addressId",
            )
        )

    for _, row in canonical_frames["products"].iterrows():
        nodes.append(
            _node_record(
                node_id=f"product:{row['product']}",
                node_type="Product",
                business_key=row["product"],
                display_label=row["product"],
                subtitle=row["productType"],
                document_date=row["creationDate"],
                status="deleted" if row["isMarkedForDeletion"] else "active",
                amount=row["netWeight"],
                currency=None,
                source_tables=["products"],
                metadata=_clean_metadata(row),
            )
        )

    for _, row in canonical_frames["plants"].iterrows():
        nodes.append(
            _node_record(
                node_id=f"plant:{row['plant']}",
                node_type="Plant",
                business_key=row["plant"],
                display_label=row["plant"],
                subtitle=row["plantName"],
                document_date=None,
                status="archived" if row["isMarkedForArchiving"] else "active",
                amount=None,
                currency=None,
                source_tables=["plants"],
                metadata=_clean_metadata(row),
            )
        )

    for _, row in canonical_frames["storage_locations"].iterrows():
        node_id = f"storage_location:{row['product']}:{row['plant']}:{row['storageLocation']}"
        nodes.append(
            _node_record(
                node_id=node_id,
                node_type="StorageLocation",
                business_key=_composite_business_key([row["product"], row["plant"], row["storageLocation"]]),
                display_label=row["storageLocation"],
                subtitle=row["plant"],
                document_date=None,
                status=None,
                amount=None,
                currency=None,
                source_tables=["storage_locations"],
                metadata=_clean_metadata(row),
            )
        )
        edges.append(
            _edge_record(
                edge_id=f"stored_at:{row['product']}:{row['plant']}:{row['storageLocation']}",
                edge_type="STORED_AT",
                source_id=f"product:{row['product']}",
                target_id=node_id,
                link_status="direct",
                derivation_rule="storage_locations.product/plant/storageLocation",
                provenance_columns="product,plant,storageLocation",
            )
        )

    product_plant_edges = canonical_frames["product_plant_assignments"].drop_duplicates(subset=["product", "plant"])
    for _, row in product_plant_edges.iterrows():
        edges.append(
            _edge_record(
                edge_id=f"available_at:{row['product']}:{row['plant']}",
                edge_type="AVAILABLE_AT",
                source_id=f"product:{row['product']}",
                target_id=f"plant:{row['plant']}",
                link_status="direct",
                derivation_rule="product_plant_assignments.product/plant",
                provenance_columns="product,plant",
            )
        )

    company_codes = pd.concat(
        [
            canonical_frames["customer_company_assignments"][["companyCode"]].rename(columns={"companyCode": "company_code"}),
            canonical_frames["billing_documents"][["companyCode"]].rename(columns={"companyCode": "company_code"}),
            canonical_frames["journal_entries_ar"][["companyCode"]].rename(columns={"companyCode": "company_code"}),
        ],
        ignore_index=True,
    ).dropna().drop_duplicates()
    for _, row in company_codes.iterrows():
        nodes.append(
            _node_record(
                node_id=f"company_code:{row.company_code}",
                node_type="CompanyCode",
                business_key=row.company_code,
                display_label=row.company_code,
                subtitle="company code",
                document_date=None,
                status=None,
                amount=None,
                currency=None,
                source_tables=["customer_company_assignments", "billing_documents", "journal_entries_ar"],
                metadata={"companyCode": row.company_code},
            )
        )

    sales_areas = canonical_frames["customer_sales_area_assignments"][
        ["salesOrganization", "distributionChannel", "division"]
    ].drop_duplicates()
    for _, row in sales_areas.iterrows():
        business_key = _composite_business_key([row.salesOrganization, row.distributionChannel, row.division])
        nodes.append(
            _node_record(
                node_id=f"sales_area:{business_key}",
                node_type="SalesArea",
                business_key=business_key,
                display_label=business_key,
                subtitle="sales area",
                document_date=None,
                status=None,
                amount=None,
                currency=None,
                source_tables=["customer_sales_area_assignments"],
                metadata=_clean_metadata(row),
            )
        )

    for _, row in canonical_frames["sales_orders"].iterrows():
        edges.append(
            _edge_record(
                edge_id=f"ordered_by:{row.salesOrder}:{row.soldToParty}",
                edge_type="ORDERED_BY",
                source_id=f"sales_order:{row.salesOrder}",
                target_id=f"customer:{row.soldToParty}",
                link_status="direct",
                derivation_rule="sales_orders.soldToParty -> customers.businessPartner",
                provenance_columns="soldToParty,businessPartner",
            )
        )

    for _, row in canonical_frames["customer_company_assignments"].iterrows():
        edges.append(
            _edge_record(
                edge_id=f"assigned_to_company:{row['customer']}:{row['companyCode']}",
                edge_type="ASSIGNED_TO_COMPANY",
                source_id=f"customer:{row['customer']}",
                target_id=f"company_code:{row['companyCode']}",
                link_status="direct",
                derivation_rule="customer_company_assignments.customer/companyCode",
                provenance_columns="customer,companyCode",
            )
        )

    for _, row in canonical_frames["customer_sales_area_assignments"].iterrows():
        sales_area_key = _composite_business_key([row["salesOrganization"], row["distributionChannel"], row["division"]])
        edges.append(
            _edge_record(
                edge_id=f"assigned_to_sales_area:{row['customer']}:{sales_area_key}",
                edge_type="ASSIGNED_TO_SALES_AREA",
                source_id=f"customer:{row['customer']}",
                target_id=f"sales_area:{sales_area_key}",
                link_status="direct",
                derivation_rule="customer_sales_area_assignments.customer/salesOrganization/distributionChannel/division",
                provenance_columns="customer,salesOrganization,distributionChannel,division",
            )
        )

    customer_delivery_links = (
        bridges["order_to_delivery_bridge"]
        .loc[lambda frame: frame["link_status"] == "direct", ["sales_order", "delivery_document"]]
        .merge(
            canonical_frames["sales_orders"][["salesOrder", "soldToParty"]],
            left_on="sales_order",
            right_on="salesOrder",
            how="left",
        )
        .dropna(subset=["soldToParty", "delivery_document"])
        .drop_duplicates(subset=["soldToParty", "delivery_document"])
    )
    for _, row in customer_delivery_links.iterrows():
        edges.append(
            _edge_record(
                edge_id=f"delivered_to:{row.delivery_document}:{row.soldToParty}",
                edge_type="DELIVERED_TO",
                source_id=f"delivery:{row.delivery_document}",
                target_id=f"customer:{row.soldToParty}",
                link_status="direct",
                derivation_rule="order_to_delivery_bridge + sales_orders.soldToParty",
                provenance_columns="sales_order,delivery_document,soldToParty",
            )
        )

    delivery_plant_links = (
        canonical_frames["delivery_items"][["deliveryDocument", "plant"]]
        .dropna()
        .drop_duplicates(subset=["deliveryDocument", "plant"])
    )
    for _, row in delivery_plant_links.iterrows():
        edges.append(
            _edge_record(
                edge_id=f"shipped_from_delivery:{row.deliveryDocument}:{row['plant']}",
                edge_type="SHIPPED_FROM",
                source_id=f"delivery:{row.deliveryDocument}",
                target_id=f"plant:{row['plant']}",
                link_status="direct",
                derivation_rule="delivery_items.deliveryDocument -> delivery; delivery_items.plant -> plant",
                provenance_columns="deliveryDocument,plant",
            )
        )

    for _, row in bridges["order_to_delivery_bridge"].iterrows():
        if row.link_status != "direct":
            continue
        edges.append(
            _edge_record(
                edge_id=f"fulfilled_by:{row.sales_order}:{row.sales_order_item}:{row.delivery_document}:{row.delivery_document_item}",
                edge_type="FULFILLED_BY",
                source_id=f"sales_order_item:{row.sales_order}:{row.sales_order_item}",
                target_id=f"delivery_item:{row.delivery_document}:{row.delivery_document_item}",
                link_status=row.link_status,
                derivation_rule=row.derivation_rule,
                provenance_columns=row.provenance_columns,
                quantity=row.actual_delivery_quantity,
                metadata={"product": row["product"], "plant": row["plant"]},
            )
        )

    for _, row in bridges["delivery_to_billing_bridge"].iterrows():
        if row.link_status != "direct":
            continue
        edges.append(
            _edge_record(
                edge_id=f"billed_as:{row.delivery_document}:{row.delivery_document_item}:{row.billing_document}:{row.billing_document_item}",
                edge_type="BILLED_AS",
                source_id=f"delivery_item:{row.delivery_document}:{row.delivery_document_item}",
                target_id=f"billing_item:{row.billing_document}:{row.billing_document_item}",
                link_status=row.link_status,
                derivation_rule=row.derivation_rule,
                provenance_columns=row.provenance_columns,
                quantity=row.billing_quantity,
                metadata={"product": row["product"], "billing_currency": row["billing_currency"]},
            )
        )

    for _, row in bridges["billing_to_journal_bridge"].iterrows():
        if row.link_status != "direct":
            continue
        edges.append(
            _edge_record(
                edge_id=f"posted_to:{row.billing_document}:{row.journal_company_code}:{row.journal_fiscal_year}:{row.journal_accounting_document}:{row.journal_accounting_document_item}",
                edge_type="POSTED_TO",
                source_id=f"billing_document:{row.billing_document}",
                target_id=(
                    f"journal_entry:{row.journal_company_code}:{row.journal_fiscal_year}:"
                    f"{row.journal_accounting_document}:{row.journal_accounting_document_item}"
                ),
                link_status=row.link_status,
                derivation_rule=row.derivation_rule,
                provenance_columns=row.provenance_columns,
                quantity=row.journal_amount,
                edge_date=row.journal_posting_date,
            )
        )

    for _, row in bridges["journal_to_payment_bridge"].iterrows():
        if row.link_status != "direct":
            continue
        edges.append(
            _edge_record(
                edge_id=f"settled_by:{row.company_code}:{row.fiscal_year}:{row.accounting_document}:{row.accounting_document_item}",
                edge_type="SETTLED_BY",
                source_id=f"journal_entry:{row.company_code}:{row.fiscal_year}:{row.accounting_document}:{row.accounting_document_item}",
                target_id=f"payment:{row.company_code}:{row.fiscal_year}:{row.accounting_document}:{row.accounting_document_item}",
                link_status=row.link_status,
                derivation_rule=row.derivation_rule,
                provenance_columns=row.provenance_columns,
                quantity=row.payment_amount,
                edge_date=row.clearing_date,
                metadata={"clearing_accounting_document": row.clearing_accounting_document},
            )
        )

    cancellation_edges = canonical_frames["billing_documents"][["billingDocument", "cancelledBillingDocument"]].dropna()
    cancellation_edges = cancellation_edges.loc[cancellation_edges["cancelledBillingDocument"].astype(str) != ""]
    for _, row in cancellation_edges.iterrows():
        edges.append(
            _edge_record(
                edge_id=f"cancels:{row.billingDocument}:{row.cancelledBillingDocument}",
                edge_type="CANCELS",
                source_id=f"billing_document:{row.billingDocument}",
                target_id=f"billing_document:{row.cancelledBillingDocument}",
                link_status="direct",
                derivation_rule="billing_documents.cancelledBillingDocument -> billing_documents.billingDocument",
                provenance_columns="billingDocument,cancelledBillingDocument",
            )
        )

    nodes_df = pd.DataFrame(nodes).drop_duplicates(subset=["node_id"]).sort_values("node_id").reset_index(drop=True)
    edges_df = pd.DataFrame(edges).drop_duplicates(subset=["edge_id"]).sort_values("edge_id").reset_index(drop=True)
    return nodes_df, edges_df


def build_networkx_graph(nodes_df: pd.DataFrame, edges_df: pd.DataFrame) -> nx.MultiDiGraph:
    graph = nx.MultiDiGraph()
    for _, node in nodes_df.iterrows():
        graph.add_node(node["node_id"], **node.to_dict())
    for _, edge in edges_df.iterrows():
        graph.add_edge(edge["source_id"], edge["target_id"], key=edge["edge_id"], **edge.to_dict())
    return graph


def focused_subgraph(
    graph: nx.MultiDiGraph,
    center_node_id: str,
    depth: int = 1,
    include_item_nodes: bool = False,
    max_nodes: int = 60,
) -> nx.MultiDiGraph:
    if center_node_id not in graph:
        raise KeyError(f"Unknown node id: {center_node_id}")

    queue: deque[tuple[str, int]] = deque([(center_node_id, 0)])
    visited: set[str] = {center_node_id}
    undirected = graph.to_undirected()

    while queue:
        current, current_depth = queue.popleft()
        if current_depth >= depth:
            continue
        for neighbor in undirected.neighbors(current):
            if neighbor in visited:
                continue
            visited.add(neighbor)
            queue.append((neighbor, current_depth + 1))
            if len(visited) >= max_nodes:
                break
        if len(visited) >= max_nodes:
            break

    selected_nodes = set(visited)
    if not include_item_nodes:
        selected_nodes = {
            node_id
            for node_id in selected_nodes
            if graph.nodes[node_id]["node_type"] not in HIDDEN_BY_DEFAULT or node_id == center_node_id
        }
    return graph.subgraph(selected_nodes).copy()


def _layout_positions(subgraph: nx.MultiDiGraph) -> dict[str, tuple[float, float]]:
    grouped: dict[int, list[str]] = {}
    for node_id, data in subgraph.nodes(data=True):
        order = ENTITY_STYLE.get(data["node_type"], {"order": 99})["order"]
        grouped.setdefault(order, []).append(node_id)

    positions: dict[str, tuple[float, float]] = {}
    for order, node_ids in sorted(grouped.items()):
        node_ids = sorted(node_ids)
        count = len(node_ids)
        for index, node_id in enumerate(node_ids):
            offset = (count - 1) / 2.0
            positions[node_id] = (float(order), float(offset - index))
    return positions


def plot_subgraph(
    subgraph: nx.MultiDiGraph,
    title: str,
    highlight_node_ids: set[str] | None = None,
) -> go.Figure:
    highlight_node_ids = highlight_node_ids or set()
    positions = _layout_positions(subgraph)

    edge_x: list[float] = []
    edge_y: list[float] = []
    annotations: list[dict[str, Any]] = []
    for source, target, data in subgraph.edges(data=True):
        x0, y0 = positions[source]
        x1, y1 = positions[target]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
        if len(subgraph.edges) <= 24:
            annotations.append(
                {
                    "x": (x0 + x1) / 2.0,
                    "y": (y0 + y1) / 2.0,
                    "text": data["edge_type"],
                    "showarrow": False,
                    "font": {"size": 10, "color": "#555"},
                }
            )

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line={"width": 1.2, "color": "#bdbdbd"},
        hoverinfo="none",
        mode="lines",
    )

    node_x: list[float] = []
    node_y: list[float] = []
    node_text: list[str] = []
    node_labels: list[str] = []
    node_colors: list[str] = []
    node_symbols: list[str] = []
    node_sizes: list[int] = []

    for node_id, data in subgraph.nodes(data=True):
        x, y = positions[node_id]
        style = ENTITY_STYLE.get(data["node_type"], {"color": "#636363", "symbol": "circle"})
        node_x.append(x)
        node_y.append(y)
        node_labels.append(str(data["display_label"]))
        node_colors.append(style["color"])
        node_symbols.append(style["symbol"])
        node_sizes.append(24 if node_id in highlight_node_ids else 18)
        node_text.append(
            "<br>".join(
                [
                    f"{data['node_type']}: {data['display_label']}",
                    f"business key: {data['business_key']}",
                    f"subtitle: {data.get('subtitle') or 'NA'}",
                    f"status: {data.get('status') or 'NA'}",
                ]
            )
        )

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers+text",
        hoverinfo="text",
        hovertext=node_text,
        text=node_labels,
        textposition="top center",
        marker={
            "size": node_sizes,
            "color": node_colors,
            "symbol": node_symbols,
            "line": {"width": 1.2, "color": "#333"},
        },
    )

    return go.Figure(
        data=[edge_trace, node_trace],
        layout=go.Layout(
            title=title,
            showlegend=False,
            hovermode="closest",
            annotations=annotations,
            margin={"b": 20, "l": 20, "r": 20, "t": 50},
            xaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
            yaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
            plot_bgcolor="#ffffff",
        ),
    )
