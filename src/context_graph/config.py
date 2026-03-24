from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EntityConfig:
    raw_name: str
    canonical_name: str
    primary_key: tuple[str, ...]
    id_columns: frozenset[str]
    numeric_columns: frozenset[str]
    datetime_columns: frozenset[str]
    time_columns: frozenset[str]
    bool_columns: frozenset[str]
    precedence_columns: tuple[str, ...] = ()


ENTITY_CONFIGS: dict[str, EntityConfig] = {
    "sales_order_headers": EntityConfig(
        raw_name="sales_order_headers",
        canonical_name="sales_orders",
        primary_key=("salesOrder",),
        id_columns=frozenset({"salesOrder", "soldToParty", "salesOrganization", "distributionChannel", "organizationDivision"}),
        numeric_columns=frozenset({"totalNetAmount"}),
        datetime_columns=frozenset(
            {
                "creationDate",
                "lastChangeDateTime",
                "pricingDate",
                "requestedDeliveryDate",
            }
        ),
        time_columns=frozenset(),
        bool_columns=frozenset(),
        precedence_columns=("lastChangeDateTime", "creationDate"),
    ),
    "sales_order_items": EntityConfig(
        raw_name="sales_order_items",
        canonical_name="sales_order_items",
        primary_key=("salesOrder", "salesOrderItem"),
        id_columns=frozenset(
            {
                "salesOrder",
                "salesOrderItem",
                "material",
                "productionPlant",
                "storageLocation",
                "materialGroup",
            }
        ),
        numeric_columns=frozenset({"requestedQuantity", "netAmount"}),
        datetime_columns=frozenset(),
        time_columns=frozenset(),
        bool_columns=frozenset(),
    ),
    "sales_order_schedule_lines": EntityConfig(
        raw_name="sales_order_schedule_lines",
        canonical_name="schedule_lines",
        primary_key=("salesOrder", "salesOrderItem", "scheduleLine"),
        id_columns=frozenset({"salesOrder", "salesOrderItem", "scheduleLine"}),
        numeric_columns=frozenset({"confdOrderQtyByMatlAvailCheck"}),
        datetime_columns=frozenset({"confirmedDeliveryDate"}),
        time_columns=frozenset(),
        bool_columns=frozenset(),
    ),
    "outbound_delivery_headers": EntityConfig(
        raw_name="outbound_delivery_headers",
        canonical_name="deliveries",
        primary_key=("deliveryDocument",),
        id_columns=frozenset({"deliveryDocument", "shippingPoint"}),
        numeric_columns=frozenset(),
        datetime_columns=frozenset({"actualGoodsMovementDate", "creationDate", "lastChangeDate"}),
        time_columns=frozenset({"actualGoodsMovementTime", "creationTime"}),
        bool_columns=frozenset(),
        precedence_columns=("lastChangeDate", "creationDate"),
    ),
    "outbound_delivery_items": EntityConfig(
        raw_name="outbound_delivery_items",
        canonical_name="delivery_items",
        primary_key=("deliveryDocument", "deliveryDocumentItem"),
        id_columns=frozenset(
            {
                "deliveryDocument",
                "deliveryDocumentItem",
                "plant",
                "referenceSdDocument",
                "referenceSdDocumentItem",
                "storageLocation",
                "batch",
            }
        ),
        numeric_columns=frozenset({"actualDeliveryQuantity"}),
        datetime_columns=frozenset({"lastChangeDate"}),
        time_columns=frozenset(),
        bool_columns=frozenset(),
    ),
    "billing_document_headers": EntityConfig(
        raw_name="billing_document_headers",
        canonical_name="billing_documents",
        primary_key=("billingDocument",),
        id_columns=frozenset(
            {
                "billingDocument",
                "cancelledBillingDocument",
                "companyCode",
                "fiscalYear",
                "accountingDocument",
                "soldToParty",
            }
        ),
        numeric_columns=frozenset({"totalNetAmount"}),
        datetime_columns=frozenset({"creationDate", "lastChangeDateTime", "billingDocumentDate"}),
        time_columns=frozenset({"creationTime"}),
        bool_columns=frozenset({"billingDocumentIsCancelled"}),
        precedence_columns=("lastChangeDateTime", "creationDate"),
    ),
    "billing_document_items": EntityConfig(
        raw_name="billing_document_items",
        canonical_name="billing_items",
        primary_key=("billingDocument", "billingDocumentItem"),
        id_columns=frozenset(
            {
                "billingDocument",
                "billingDocumentItem",
                "material",
                "referenceSdDocument",
                "referenceSdDocumentItem",
            }
        ),
        numeric_columns=frozenset({"billingQuantity", "netAmount"}),
        datetime_columns=frozenset(),
        time_columns=frozenset(),
        bool_columns=frozenset(),
    ),
    "journal_entry_items_accounts_receivable": EntityConfig(
        raw_name="journal_entry_items_accounts_receivable",
        canonical_name="journal_entries_ar",
        primary_key=("companyCode", "fiscalYear", "accountingDocument", "accountingDocumentItem"),
        id_columns=frozenset(
            {
                "companyCode",
                "fiscalYear",
                "accountingDocument",
                "accountingDocumentItem",
                "glAccount",
                "referenceDocument",
                "costCenter",
                "profitCenter",
                "assignmentReference",
                "customer",
                "financialAccountType",
                "clearingAccountingDocument",
                "clearingDocFiscalYear",
            }
        ),
        numeric_columns=frozenset({"amountInTransactionCurrency", "amountInCompanyCodeCurrency"}),
        datetime_columns=frozenset({"postingDate", "documentDate", "lastChangeDateTime", "clearingDate"}),
        time_columns=frozenset(),
        bool_columns=frozenset(),
        precedence_columns=("lastChangeDateTime", "postingDate"),
    ),
    "payments_accounts_receivable": EntityConfig(
        raw_name="payments_accounts_receivable",
        canonical_name="payments_ar",
        primary_key=("companyCode", "fiscalYear", "accountingDocument", "accountingDocumentItem"),
        id_columns=frozenset(
            {
                "companyCode",
                "fiscalYear",
                "accountingDocument",
                "accountingDocumentItem",
                "clearingAccountingDocument",
                "clearingDocFiscalYear",
                "customer",
                "invoiceReference",
                "invoiceReferenceFiscalYear",
                "salesDocument",
                "salesDocumentItem",
                "assignmentReference",
                "glAccount",
                "financialAccountType",
                "profitCenter",
                "costCenter",
            }
        ),
        numeric_columns=frozenset({"amountInTransactionCurrency", "amountInCompanyCodeCurrency"}),
        datetime_columns=frozenset({"clearingDate", "postingDate", "documentDate"}),
        time_columns=frozenset(),
        bool_columns=frozenset(),
        precedence_columns=("clearingDate", "postingDate"),
    ),
    "business_partners": EntityConfig(
        raw_name="business_partners",
        canonical_name="customers",
        primary_key=("businessPartner",),
        id_columns=frozenset({"businessPartner", "customer", "businessPartnerGrouping", "createdByUser"}),
        numeric_columns=frozenset(),
        datetime_columns=frozenset({"creationDate", "lastChangeDate"}),
        time_columns=frozenset({"creationTime"}),
        bool_columns=frozenset({"businessPartnerIsBlocked", "isMarkedForArchiving"}),
        precedence_columns=("lastChangeDate", "creationDate"),
    ),
    "business_partner_addresses": EntityConfig(
        raw_name="business_partner_addresses",
        canonical_name="addresses",
        primary_key=("businessPartner", "addressId"),
        id_columns=frozenset({"businessPartner", "addressId", "addressUuid", "country", "region", "postalCode"}),
        numeric_columns=frozenset(),
        datetime_columns=frozenset({"validityStartDate", "validityEndDate"}),
        time_columns=frozenset(),
        bool_columns=frozenset({"poBoxIsWithoutNumber"}),
    ),
    "products": EntityConfig(
        raw_name="products",
        canonical_name="products",
        primary_key=("product",),
        id_columns=frozenset({"product", "productType", "createdByUser", "productOldId", "productGroup", "division", "industrySector"}),
        numeric_columns=frozenset({"grossWeight", "netWeight"}),
        datetime_columns=frozenset({"crossPlantStatusValidityDate", "creationDate", "lastChangeDate", "lastChangeDateTime"}),
        time_columns=frozenset(),
        bool_columns=frozenset({"isMarkedForDeletion"}),
        precedence_columns=("lastChangeDateTime", "lastChangeDate", "creationDate"),
    ),
    "product_descriptions": EntityConfig(
        raw_name="product_descriptions",
        canonical_name="product_descriptions",
        primary_key=("product", "language"),
        id_columns=frozenset({"product", "language"}),
        numeric_columns=frozenset(),
        datetime_columns=frozenset(),
        time_columns=frozenset(),
        bool_columns=frozenset(),
    ),
    "plants": EntityConfig(
        raw_name="plants",
        canonical_name="plants",
        primary_key=("plant",),
        id_columns=frozenset(
            {
                "plant",
                "plantCustomer",
                "plantSupplier",
                "factoryCalendar",
                "defaultPurchasingOrganization",
                "salesOrganization",
                "addressId",
                "distributionChannel",
                "division",
                "language",
            }
        ),
        numeric_columns=frozenset(),
        datetime_columns=frozenset(),
        time_columns=frozenset(),
        bool_columns=frozenset({"isMarkedForArchiving"}),
    ),
    "product_storage_locations": EntityConfig(
        raw_name="product_storage_locations",
        canonical_name="storage_locations",
        primary_key=("product", "plant", "storageLocation"),
        id_columns=frozenset({"product", "plant", "storageLocation"}),
        numeric_columns=frozenset(),
        datetime_columns=frozenset(),
        time_columns=frozenset(),
        bool_columns=frozenset(),
    ),
    "product_plants": EntityConfig(
        raw_name="product_plants",
        canonical_name="product_plant_assignments",
        primary_key=("product", "plant"),
        id_columns=frozenset({"product", "plant"}),
        numeric_columns=frozenset(),
        datetime_columns=frozenset(),
        time_columns=frozenset(),
        bool_columns=frozenset(),
    ),
    "customer_company_assignments": EntityConfig(
        raw_name="customer_company_assignments",
        canonical_name="customer_company_assignments",
        primary_key=("customer", "companyCode"),
        id_columns=frozenset({"customer", "companyCode", "accountingClerk", "alternativePayerAccount", "reconciliationAccount", "customerAccountGroup"}),
        numeric_columns=frozenset(),
        datetime_columns=frozenset(),
        time_columns=frozenset(),
        bool_columns=frozenset({"deletionIndicator"}),
    ),
    "customer_sales_area_assignments": EntityConfig(
        raw_name="customer_sales_area_assignments",
        canonical_name="customer_sales_area_assignments",
        primary_key=("customer", "salesOrganization", "distributionChannel", "division"),
        id_columns=frozenset(
            {
                "customer",
                "salesOrganization",
                "distributionChannel",
                "division",
                "creditControlArea",
                "currency",
                "deliveryPriority",
                "incotermsClassification",
                "incotermsLocation1",
                "salesGroup",
                "salesOffice",
                "shippingCondition",
                "supplyingPlant",
                "salesDistrict",
                "exchangeRateType",
            }
        ),
        numeric_columns=frozenset(),
        datetime_columns=frozenset(),
        time_columns=frozenset(),
        bool_columns=frozenset({"billingIsBlockedForCustomer", "completeDeliveryIsDefined", "slsUnlmtdOvrdelivIsAllwd"}),
    ),
    "billing_document_cancellations": EntityConfig(
        raw_name="billing_document_cancellations",
        canonical_name="billing_document_cancellations",
        primary_key=("billingDocument",),
        id_columns=frozenset(
            {
                "billingDocument",
                "cancelledBillingDocument",
                "companyCode",
                "fiscalYear",
                "accountingDocument",
                "soldToParty",
            }
        ),
        numeric_columns=frozenset({"totalNetAmount"}),
        datetime_columns=frozenset({"creationDate", "lastChangeDateTime", "billingDocumentDate"}),
        time_columns=frozenset({"creationTime"}),
        bool_columns=frozenset({"billingDocumentIsCancelled"}),
        precedence_columns=("lastChangeDateTime", "creationDate"),
    ),
}


BUSINESS_GLOSSARY = {
    "invoice": "billing document",
    "billing": "billing document",
    "billing doc": "billing document",
    "invoicing": "billing document",
    "product": "material",
    "material": "product",
    "customer": "business partner / sold-to party",
    "delivery": "outbound delivery",
    "ar": "accounts receivable",
    "journal entry": "accounts receivable journal entry",
    "payment clearing": "payment settlement against AR journal entries",
    "broken flow": "missing upstream or downstream process relationship",
    "sales order": "sales order",
}


APPROVED_VIEWS = {
    "v_sales_order_flow": "Sales order header and item flow with customer, product, delivery, and billing context.",
    "v_delivery_flow": "Delivery header and item flow with upstream sales order and aggregated downstream billing context.",
    "v_billing_flow": "Billing header and item flow with upstream delivery and sales order context.",
    "v_billing_trace": "Billing-item-level lineage from sales order through payment settlement.",
    "v_financial_flow": "Billing to AR journal entry to payment settlement flow.",
    "v_product_billing_summary": "Product-level billing aggregates and distinct billing document counts.",
    "v_customer_360": "Customer-centric rollup of order, delivery, billing, and payment activity.",
    "v_incomplete_order_flows": "Order and order-item anomalies, including delivered-not-billed and missing finance steps.",
}


ALLOWED_METRICS = (
    "count",
    "count_distinct",
    "sum",
    "avg",
    "min",
    "max",
    "ratio",
)
