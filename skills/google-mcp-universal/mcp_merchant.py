"""
Google Merchant Center Integration Module.

Handles products, product statuses, datafeeds, account management,
and Merchant Center reporting (performance, best sellers, pricing).
"""

from typing import Optional
from pydantic import BaseModel, Field

import google_ads_mcp as _gam

from google_ads_mcp import (
    mcp,
    _ensure_merchant_client,
)


# ============================================================================
# REQUEST MODELS
# ============================================================================


class MerchantListProductsRequest(BaseModel):
    """Request model for listing products in Merchant Center"""
    merchant_id: str = Field(..., description="Merchant ID")
    max_results: int = Field(50, description="Maximum number of results", ge=1, le=1000)
    page_token: Optional[str] = Field(None, description="Page token for pagination")


class MerchantGetProductRequest(BaseModel):
    """Request model for getting a single product"""
    merchant_id: str = Field(..., description="Merchant ID")
    product_id: str = Field(..., description="Product ID (format: online:en:US:product123)")


class MerchantListProductStatusesRequest(BaseModel):
    """Request model for listing product statuses"""
    merchant_id: str = Field(..., description="Merchant ID")
    max_results: int = Field(50, description="Maximum number of results", ge=1, le=1000)
    page_token: Optional[str] = Field(None, description="Page token for pagination")


class MerchantGetProductStatusRequest(BaseModel):
    """Request model for getting product status"""
    merchant_id: str = Field(..., description="Merchant ID")
    product_id: str = Field(..., description="Product ID (format: online:en:US:product123)")


class MerchantListAccountsRequest(BaseModel):
    """Request model for listing sub-accounts under MCA"""
    mca_id: str = Field(..., description="MCA (Multi-Client Account) ID")
    max_results: int = Field(50, description="Maximum number of results", ge=1, le=1000)
    page_token: Optional[str] = Field(None, description="Page token for pagination")


class MerchantGetAccountRequest(BaseModel):
    """Request model for getting account info"""
    merchant_id: str = Field(..., description="Merchant ID")


class MerchantUpdateProductRequest(BaseModel):
    """Request model for updating a product"""
    merchant_id: str = Field(..., description="Merchant ID")
    product_id: str = Field(..., description="Product ID (format: online:en:US:product123)")
    title: Optional[str] = Field(None, description="Product title")
    description: Optional[str] = Field(None, description="Product description")
    price: Optional[float] = Field(None, description="Product price")
    availability: Optional[str] = Field(None, description="Product availability")
    custom_label_0: Optional[str] = Field(None, description="Custom label 0")
    custom_label_1: Optional[str] = Field(None, description="Custom label 1")
    custom_label_2: Optional[str] = Field(None, description="Custom label 2")
    custom_label_3: Optional[str] = Field(None, description="Custom label 3")
    custom_label_4: Optional[str] = Field(None, description="Custom label 4")


class MerchantDeleteProductRequest(BaseModel):
    """Request model for deleting a product"""
    merchant_id: str = Field(..., description="Merchant ID")
    product_id: str = Field(..., description="Product ID (format: online:en:US:product123)")


class MerchantListDatafeedsRequest(BaseModel):
    """Request model for listing datafeeds"""
    merchant_id: str = Field(..., description="Merchant ID")
    max_results: int = Field(50, description="Maximum number of results", ge=1, le=1000)
    page_token: Optional[str] = Field(None, description="Page token for pagination")


class MerchantGetDatafeedRequest(BaseModel):
    """Request model for getting a single datafeed"""
    merchant_id: str = Field(..., description="Merchant ID")
    datafeed_id: str = Field(..., description="Datafeed ID (numeric)")


class MerchantInsertDatafeedRequest(BaseModel):
    """Request model for creating a new datafeed"""
    merchant_id: str = Field(..., description="Merchant ID")
    name: str = Field(..., description="Datafeed name")
    feed_url: str = Field(..., description="URL of the feed file")
    content_language: str = Field("en", description="Content language code")
    target_country: str = Field(..., description="Target country ISO code")
    fetch_hour: Optional[int] = Field(None, description="Hour (0-23) to fetch daily", ge=0, le=23)
    fetch_day: Optional[str] = Field(None, description="Day of week for weekly fetch")
    fetch_timezone: Optional[str] = Field("Europe/London", description="Timezone for fetch schedule")
    feed_format: Optional[str] = Field(None, description="Feed format: 'tsv', 'xml', 'csv'")


class MerchantUpdateDatafeedRequest(BaseModel):
    """Request model for updating an existing datafeed"""
    merchant_id: str = Field(..., description="Merchant ID")
    datafeed_id: str = Field(..., description="Datafeed ID (numeric)")
    name: Optional[str] = Field(None, description="New datafeed name")
    feed_url: Optional[str] = Field(None, description="New feed URL")
    fetch_hour: Optional[int] = Field(None, description="New fetch hour", ge=0, le=23)
    fetch_day: Optional[str] = Field(None, description="New fetch day")
    fetch_timezone: Optional[str] = Field(None, description="New timezone")
    content_language: Optional[str] = Field(None, description="New language code")
    target_country: Optional[str] = Field(None, description="New country code")


class MerchantDeleteDatafeedRequest(BaseModel):
    """Request model for deleting a datafeed"""
    merchant_id: str = Field(..., description="Merchant ID")
    datafeed_id: str = Field(..., description="Datafeed ID (numeric)")


class MerchantFetchDatafeedNowRequest(BaseModel):
    """Request model for triggering an immediate datafeed fetch"""
    merchant_id: str = Field(..., description="Merchant ID")
    datafeed_id: str = Field(..., description="Datafeed ID (numeric)")


class MerchantListDatafeedStatusesRequest(BaseModel):
    """Request model for listing datafeed statuses"""
    merchant_id: str = Field(..., description="Merchant ID")
    max_results: int = Field(50, description="Maximum number of results", ge=1, le=1000)
    page_token: Optional[str] = Field(None, description="Page token for pagination")


class MerchantGetDatafeedStatusRequest(BaseModel):
    """Request model for getting a single datafeed status"""
    merchant_id: str = Field(..., description="Merchant ID")
    datafeed_id: str = Field(..., description="Datafeed ID (numeric)")


class MerchantListAccountStatusesRequest(BaseModel):
    """Request model for listing account statuses under MCA"""
    mca_id: str = Field(..., description="MCA (Multi-Client Account) ID")
    max_results: int = Field(50, description="Maximum number of results", ge=1, le=1000)
    page_token: Optional[str] = Field(None, description="Page token for pagination")


class MerchantGetAccountStatusRequest(BaseModel):
    """Request model for getting account status for a specific merchant"""
    merchant_id: str = Field(..., description="Merchant ID")
    account_id: Optional[str] = Field(None, description="Account ID (defaults to merchant_id)")


class MerchantReportSearchRequest(BaseModel):
    """Request model for Merchant Center reports"""
    merchant_id: str = Field(..., description="Merchant ID")
    query: str = Field(..., description="MQL query for Merchant Center reports")
    page_size: int = Field(1000, description="Max rows to return", ge=1, le=5000)
    page_token: Optional[str] = Field(None, description="Page token for pagination")


class UpdateProductCustomLabelsRequest(BaseModel):
    """Request model for updating product custom labels"""
    merchant_id: str = Field(..., description="Merchant Center account ID")
    product_id: str = Field(..., description="Product ID (format: online:pl:PL:handle)")
    custom_labels: dict = Field(..., description="Dict of custom_label_N: value")


# ============================================================================
# PRODUCT MANAGEMENT TOOLS
# ============================================================================


@mcp.tool()
async def merchant_center_list_products(request: MerchantListProductsRequest) -> dict:
    """
    List products for a given merchant ID with optional filters.

    Returns product details including id, title, price, availability, condition.
    """
    try:
        _ensure_merchant_client()

        list_params = {
            "maxResults": request.max_results
        }
        if request.page_token:
            list_params["pageToken"] = request.page_token

        response = _gam.merchant_center_client.products().list(
            merchantId=request.merchant_id,
            **list_params
        ).execute()

        products = response.get("resources", [])

        return {
            "status": "success",
            "products": products,
            "count": len(products),
            "next_page_token": response.get("nextPageToken")
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to list products: {str(e)}"}


@mcp.tool()
async def merchant_center_get_product(request: MerchantGetProductRequest) -> dict:
    """
    Get detailed information for a single product by product ID and merchant ID.
    """
    try:
        _ensure_merchant_client()

        product = _gam.merchant_center_client.products().get(
            merchantId=request.merchant_id,
            productId=request.product_id
        ).execute()

        return {
            "status": "success",
            "product": product
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to get product: {str(e)}"}


@mcp.tool()
async def merchant_center_list_product_statuses(request: MerchantListProductStatusesRequest) -> dict:
    """
    List product statuses (approval status, disapprovals, data quality issues).
    """
    try:
        _ensure_merchant_client()

        list_params = {"maxResults": request.max_results}
        if request.page_token:
            list_params["pageToken"] = request.page_token

        response = _gam.merchant_center_client.productstatuses().list(
            merchantId=request.merchant_id,
            **list_params
        ).execute()

        statuses = response.get("resources", [])

        return {
            "status": "success",
            "statuses": statuses,
            "count": len(statuses),
            "next_page_token": response.get("nextPageToken")
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to list product statuses: {str(e)}"}


@mcp.tool()
async def merchant_center_get_product_status(request: MerchantGetProductStatusRequest) -> dict:
    """
    Get status for a single product by product ID and merchant ID.
    """
    try:
        _ensure_merchant_client()

        status = _gam.merchant_center_client.productstatuses().get(
            merchantId=request.merchant_id,
            productId=request.product_id
        ).execute()

        return {
            "status": "success",
            "product_status": status
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to get product status: {str(e)}"}


@mcp.tool()
async def merchant_center_update_product(request: MerchantUpdateProductRequest) -> dict:
    """
    Update product fields (title, description, price, availability, custom_label_0-4).
    """
    try:
        _ensure_merchant_client()

        product = {}
        if request.title:
            product["title"] = request.title
        if request.description:
            product["description"] = request.description
        if request.price:
            product["price"] = {"value": str(request.price), "currency": "USD"}
        if request.availability:
            product["availability"] = request.availability

        for i in range(5):
            label_attr = f"custom_label_{i}"
            label_val = getattr(request, label_attr, None)
            if label_val:
                product[label_attr] = label_val

        result = _gam.merchant_center_client.products().update(
            merchantId=request.merchant_id,
            productId=request.product_id,
            body=product
        ).execute()

        return {
            "status": "success",
            "message": f"Product {request.product_id} updated successfully",
            "product_id": request.product_id
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to update product: {str(e)}"}


@mcp.tool()
async def merchant_center_delete_product(request: MerchantDeleteProductRequest) -> dict:
    """
    Delete a product by merchant ID and product ID.
    """
    try:
        _ensure_merchant_client()

        _gam.merchant_center_client.products().delete(
            merchantId=request.merchant_id,
            productId=request.product_id
        ).execute()

        return {
            "status": "success",
            "message": f"Product {request.product_id} deleted successfully"
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to delete product: {str(e)}"}


# ============================================================================
# ACCOUNT MANAGEMENT TOOLS
# ============================================================================


@mcp.tool()
async def merchant_center_list_accounts(request: MerchantListAccountsRequest) -> dict:
    """
    List sub-accounts under an MCA (Multi-Client Account) by MCA ID.
    """
    try:
        _ensure_merchant_client()

        list_params = {"maxResults": request.max_results}
        if request.page_token:
            list_params["pageToken"] = request.page_token

        response = _gam.merchant_center_client.accounts().list(
            merchantId=request.mca_id,
            **list_params
        ).execute()

        accounts = response.get("resources", [])

        return {
            "status": "success",
            "accounts": accounts,
            "count": len(accounts),
            "next_page_token": response.get("nextPageToken")
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to list accounts: {str(e)}"}


@mcp.tool()
async def merchant_center_get_account(request: MerchantGetAccountRequest) -> dict:
    """
    Get account information by merchant ID.
    """
    try:
        _ensure_merchant_client()

        account = _gam.merchant_center_client.accounts().get(
            merchantId=request.merchant_id
        ).execute()

        return {
            "status": "success",
            "account": account
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to get account: {str(e)}"}


@mcp.tool()
async def merchant_center_get_account_status(request: MerchantGetAccountStatusRequest) -> dict:
    """
    Get detailed account status for a specific merchant.
    """
    try:
        _ensure_merchant_client()

        account_id = request.account_id or request.merchant_id

        status = _gam.merchant_center_client.accountstatuses().get(
            merchantId=request.merchant_id,
            accountId=account_id
        ).execute()

        return {
            "status": "success",
            "account_status": status
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to get account status: {str(e)}"}


@mcp.tool()
async def merchant_center_list_account_statuses(request: MerchantListAccountStatusesRequest) -> dict:
    """
    List account-level statuses for all sub-accounts under an MCA.
    """
    try:
        _ensure_merchant_client()

        list_params = {"maxResults": request.max_results}
        if request.page_token:
            list_params["pageToken"] = request.page_token

        response = _gam.merchant_center_client.accountstatuses().list(
            merchantId=request.mca_id,
            **list_params
        ).execute()

        statuses = response.get("resources", [])

        return {
            "status": "success",
            "account_statuses": statuses,
            "count": len(statuses),
            "next_page_token": response.get("nextPageToken")
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to list account statuses: {str(e)}"}


# ============================================================================
# DATAFEED MANAGEMENT TOOLS
# ============================================================================


@mcp.tool()
async def merchant_center_list_datafeeds(request: MerchantListDatafeedsRequest) -> dict:
    """
    List all datafeeds configured for a merchant.
    """
    try:
        _ensure_merchant_client()

        list_params = {"maxResults": request.max_results}
        if request.page_token:
            list_params["pageToken"] = request.page_token

        response = _gam.merchant_center_client.datafeeds().list(
            merchantId=request.merchant_id,
            **list_params
        ).execute()

        datafeeds = response.get("resources", [])

        return {
            "status": "success",
            "datafeeds": datafeeds,
            "count": len(datafeeds),
            "next_page_token": response.get("nextPageToken")
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to list datafeeds: {str(e)}"}


@mcp.tool()
async def merchant_center_get_datafeed(request: MerchantGetDatafeedRequest) -> dict:
    """
    Get detailed information for a single datafeed by ID.
    """
    try:
        _ensure_merchant_client()

        feed = _gam.merchant_center_client.datafeeds().get(
            merchantId=request.merchant_id,
            datafeedId=request.datafeed_id
        ).execute()

        return {
            "status": "success",
            "datafeed": feed
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to get datafeed: {str(e)}"}


@mcp.tool()
async def merchant_center_insert_datafeed(request: MerchantInsertDatafeedRequest) -> dict:
    """
    Create a new datafeed for a merchant.
    """
    try:
        _ensure_merchant_client()

        datafeed = {
            "name": request.name,
            "contentLanguage": request.content_language,
            "targetCountry": request.target_country,
            "fetchSchedule": {
                "fetchUrl": request.feed_url,
                "timeZone": request.fetch_timezone,
            }
        }

        if request.fetch_hour is not None:
            datafeed["fetchSchedule"]["hour"] = request.fetch_hour
        if request.fetch_day:
            datafeed["fetchSchedule"]["dayOfWeek"] = request.fetch_day.upper()
        if request.feed_format:
            datafeed["format"] = request.feed_format.upper()

        result = _gam.merchant_center_client.datafeeds().insert(
            merchantId=request.merchant_id,
            body=datafeed
        ).execute()

        return {
            "status": "success",
            "message": f"Datafeed '{request.name}' created successfully",
            "datafeed_id": result.get("id")
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to create datafeed: {str(e)}"}


@mcp.tool()
async def merchant_center_update_datafeed(request: MerchantUpdateDatafeedRequest) -> dict:
    """
    Update an existing datafeed's configuration.
    """
    try:
        _ensure_merchant_client()

        # Get current datafeed
        current = _gam.merchant_center_client.datafeeds().get(
            merchantId=request.merchant_id,
            datafeedId=request.datafeed_id
        ).execute()

        # Update fields
        if request.name:
            current["name"] = request.name
        if request.feed_url:
            current["fetchSchedule"]["fetchUrl"] = request.feed_url
        if request.fetch_hour is not None:
            current["fetchSchedule"]["hour"] = request.fetch_hour
        if request.fetch_day:
            current["fetchSchedule"]["dayOfWeek"] = request.fetch_day.upper()
        if request.fetch_timezone:
            current["fetchSchedule"]["timeZone"] = request.fetch_timezone
        if request.content_language:
            current["contentLanguage"] = request.content_language
        if request.target_country:
            current["targetCountry"] = request.target_country

        result = _gam.merchant_center_client.datafeeds().update(
            merchantId=request.merchant_id,
            datafeedId=request.datafeed_id,
            body=current
        ).execute()

        return {
            "status": "success",
            "message": f"Datafeed {request.datafeed_id} updated successfully"
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to update datafeed: {str(e)}"}


@mcp.tool()
async def merchant_center_delete_datafeed(request: MerchantDeleteDatafeedRequest) -> dict:
    """
    Delete a datafeed from a merchant.
    """
    try:
        _ensure_merchant_client()

        _gam.merchant_center_client.datafeeds().delete(
            merchantId=request.merchant_id,
            datafeedId=request.datafeed_id
        ).execute()

        return {
            "status": "success",
            "message": f"Datafeed {request.datafeed_id} deleted successfully"
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to delete datafeed: {str(e)}"}


@mcp.tool()
async def merchant_center_fetch_datafeed_now(request: MerchantFetchDatafeedNowRequest) -> dict:
    """
    Trigger an immediate fetch of a datafeed.
    """
    try:
        _ensure_merchant_client()

        _gam.merchant_center_client.datafeeds().fetchnow(
            merchantId=request.merchant_id,
            datafeedId=request.datafeed_id
        ).execute()

        return {
            "status": "success",
            "message": f"Datafeed {request.datafeed_id} fetch triggered"
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to fetch datafeed: {str(e)}"}


@mcp.tool()
async def merchant_center_list_datafeed_statuses(request: MerchantListDatafeedStatusesRequest) -> dict:
    """
    List processing statuses for all datafeeds in a merchant.
    """
    try:
        _ensure_merchant_client()

        list_params = {"maxResults": request.max_results}
        if request.page_token:
            list_params["pageToken"] = request.page_token

        response = _gam.merchant_center_client.datafeedstatuses().list(
            merchantId=request.merchant_id,
            **list_params
        ).execute()

        statuses = response.get("resources", [])

        return {
            "status": "success",
            "datafeed_statuses": statuses,
            "count": len(statuses),
            "next_page_token": response.get("nextPageToken")
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to list datafeed statuses: {str(e)}"}


@mcp.tool()
async def merchant_center_get_datafeed_status(request: MerchantGetDatafeedStatusRequest) -> dict:
    """
    Get processing status for a single datafeed.
    """
    try:
        _ensure_merchant_client()

        status = _gam.merchant_center_client.datafeedstatuses().get(
            merchantId=request.merchant_id,
            datafeedId=request.datafeed_id
        ).execute()

        return {
            "status": "success",
            "datafeed_status": status
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to get datafeed status: {str(e)}"}


# ============================================================================
# MERCHANT CENTER REPORTING
# ============================================================================


@mcp.tool()
async def merchant_center_report_search(request: MerchantReportSearchRequest) -> dict:
    """
    Execute a Merchant Center report query using MQL (Merchant Query Language).

    Supports multiple report types: MerchantPerformanceView, BestSellersBrandView,
    BestSellersProductClusterView, PriceCompetitivenessProductView,
    PriceInsightsProductView, CompetitiveVisibilityCompetitorView.
    """
    try:
        _ensure_merchant_client()

        search_request = {"query": request.query}
        if request.page_size:
            search_request["pageSize"] = request.page_size
        if request.page_token:
            search_request["pageToken"] = request.page_token

        response = _gam.merchant_center_client.reports().search(
            merchantId=request.merchant_id,
            body=search_request
        ).execute()

        results = response.get("results", [])

        return {
            "status": "success",
            "results": results,
            "count": len(results),
            "next_page_token": response.get("nextPageToken")
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to execute report query: {str(e)}"}


@mcp.tool()
async def merchant_center_update_custom_labels(request: UpdateProductCustomLabelsRequest) -> dict:
    """
    Update custom_label_0 through custom_label_4 on a Merchant Center product.

    Used for campaign segmentation, bid strategy grouping, seasonal tagging, priority tiers.
    """
    try:
        _ensure_merchant_client()

        # Get current product
        product = _gam.merchant_center_client.products().get(
            merchantId=request.merchant_id,
            productId=request.product_id
        ).execute()

        # Update custom labels
        for label_key, label_val in request.custom_labels.items():
            if label_key.startswith("custom_label_"):
                product[label_key] = label_val

        # Update the product
        result = _gam.merchant_center_client.products().update(
            merchantId=request.merchant_id,
            productId=request.product_id,
            body=product
        ).execute()

        return {
            "status": "success",
            "message": f"Custom labels updated for product {request.product_id}",
            "product_id": request.product_id
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to update custom labels: {str(e)}"}
