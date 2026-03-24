"""
Google Ads MCP Server

A comprehensive Model Context Protocol server for interacting with Google Ads API.
Supports campaign management, keyword optimization, reporting, and account analytics.
"""

import os
import sys
import json
import base64
import ssl
import urllib.request

# CRITICAL: When this file runs as __main__, sub-modules doing
# "from google_ads_mcp import mcp" would create a SECOND module instance.
# This alias ensures they get the SAME module (and the same mcp instance).
if __name__ == "__main__" or 'google_ads_mcp' not in sys.modules:
    sys.modules['google_ads_mcp'] = sys.modules[__name__]

# SSL context for downloading images (some CDNs have cert issues)
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional, Literal, List

from dotenv import load_dotenv
from fastmcp import FastMCP
from pydantic import BaseModel, Field

from google.ads.googleads.client import GoogleAdsClient
from google.api_core import gapic_v1
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

# Load environment variables
load_dotenv()

# Initialize FastMCP app
mcp = FastMCP("google-ads-server")

# Global client instances
google_ads_client: Optional[GoogleAdsClient] = None
merchant_center_client: Optional[object] = None

# API call tracking via BatchDB
_api_tracker_db = None

def _track_api_call(operation_type: str, count: int = 1):
    """Track an API call for quota monitoring. Silently ignores errors."""
    global _api_tracker_db
    try:
        if _api_tracker_db is None:
            from batch_db import BatchDB
            _api_tracker_db = BatchDB()
        _api_tracker_db.increment_api_calls(operation_type, count)
    except Exception:
        pass  # Never let tracking break actual API operations


# Pydantic Models for input validation
class CampaignListRequest(BaseModel):
    """Request model for listing campaigns"""
    customer_id: str = Field(
        ..., description="Customer ID (e.g., '846-741-2248' formatted as XXXXXXXXXX without dashes)"
    )
    campaign_type: Optional[Literal["SEARCH", "SHOPPING", "PERFORMANCE_MAX"]] = Field(
        None, description="Filter by campaign type"
    )
    status: Optional[Literal["ENABLED", "PAUSED", "REMOVED"]] = Field(
        None, description="Filter by campaign status"
    )
    page_size: int = Field(50, description="Number of results per page", ge=1, le=1000)


class CampaignDetailsRequest(BaseModel):
    """Request model for getting campaign details"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(
        None, description="Campaign ID (numeric)"
    )
    campaign_name: Optional[str] = Field(
        None, description="Campaign name (use if campaign_id not provided)"
    )


class AdGroupListRequest(BaseModel):
    """Request model for listing ad groups"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID to list ad groups for")
    page_size: int = Field(50, description="Number of results per page", ge=1, le=1000)


class KeywordListRequest(BaseModel):
    """Request model for listing keywords"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(None, description="Filter by campaign ID")
    min_quality_score: Optional[int] = Field(
        None, description="Minimum quality score filter (1-10)"
    )
    status: Optional[Literal["ENABLED", "PAUSED", "REMOVED"]] = Field(
        None, description="Filter by status"
    )
    page_size: int = Field(50, description="Number of results per page", ge=1, le=1000)


class PerformanceReportRequest(BaseModel):
    """Request model for performance reports"""
    customer_id: str = Field(..., description="Customer ID")
    start_date: str = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: str = Field(..., description="End date (YYYY-MM-DD)")
    entity_type: Literal["campaign", "ad_group", "keyword"] = Field(
        "campaign", description="Entity type to report on"
    )
    campaign_id: Optional[str] = Field(None, description="Filter by campaign ID")
    page_size: int = Field(50, description="Number of results per page", ge=1, le=1000)


class AssetListRequest(BaseModel):
    """Request model for listing assets"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(None, description="Filter by campaign ID")
    asset_type: Optional[str] = Field(
        None, description="Filter by asset type (e.g., 'TEXT', 'IMAGE', 'VIDEO')"
    )
    page_size: int = Field(50, description="Number of results per page", ge=1, le=1000)


class SearchTermsReportRequest(BaseModel):
    """Request model for search terms report"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(None, description="Filter by campaign ID")
    min_impressions: int = Field(0, description="Minimum impressions filter")
    start_date: str = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: str = Field(..., description="End date (YYYY-MM-DD)")
    page_size: int = Field(50, description="Number of results per page", ge=1, le=1000)


class AccountSummaryRequest(BaseModel):
    """Request model for account summary"""
    customer_id: str = Field(..., description="Customer ID")
    date_range: Literal["TODAY", "LAST_7_DAYS", "LAST_30_DAYS", "LAST_90_DAYS"] = Field(
        "LAST_30_DAYS", description="Date range for metrics"
    )


class CampaignStatusUpdateRequest(BaseModel):
    """Request model for updating campaign status"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    status: Literal["ENABLED", "PAUSED"] = Field(..., description="New campaign status")


class CampaignBudgetUpdateRequest(BaseModel):
    """Request model for updating campaign budget"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    daily_budget_micros: int = Field(
        ..., description="Daily budget in micros (e.g., 100000000 = $100)"
    )


class KeywordBidUpdateRequest(BaseModel):
    """Request model for updating keyword bid"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    criterion_id: str = Field(..., description="Keyword criterion ID")
    max_cpc_micros: int = Field(..., description="Max CPC in micros")


class KeywordStatusUpdateRequest(BaseModel):
    """Request model for pausing/enabling keywords"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    criterion_id: str = Field(..., description="Keyword criterion ID")


class NegativeKeywordAddRequest(BaseModel):
    """Request model for adding negative keywords"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    keyword_text: str = Field(..., description="Negative keyword text")
    match_type: Literal["BROAD", "PHRASE", "EXACT"] = Field(
        "PHRASE", description="Match type"
    )


class AssetUpdateRequest(BaseModel):
    """Request model for updating assets"""
    customer_id: str = Field(..., description="Customer ID")
    asset_id: str = Field(..., description="Asset ID")
    text: Optional[str] = Field(None, description="New text content")


# Merchant Center Request Models
# ============ Ad Extension Asset Request Models ============

class CreateCalloutAssetRequest(BaseModel):
    """Request model for creating callout assets and linking to campaign"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(None, description="Campaign ID (if linking to campaign)")
    ad_group_id: Optional[str] = Field(None, description="Ad group ID (if linking to specific ad group — lowest level possible)")
    callout_texts: List[str] = Field(
        ..., description="List of callout texts (max 25 chars each, 2-20 callouts)"
    )


class CreateSitelinkAssetRequest(BaseModel):
    """Request model for creating sitelink assets and linking to campaign"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(None, description="Campaign ID (if linking to campaign)")
    ad_group_id: Optional[str] = Field(None, description="Ad group ID (if linking to specific ad group — lowest level possible)")
    sitelinks: List[dict] = Field(
        ..., description="List of sitelink dicts with: link_text (max 25 chars), description1 (max 35 chars), description2 (max 35 chars), final_urls (list of URLs)"
    )


class CreateStructuredSnippetAssetRequest(BaseModel):
    """Request model for creating structured snippet assets"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(None, description="Campaign ID (if linking to campaign)")
    ad_group_id: Optional[str] = Field(None, description="Ad group ID (if linking to specific ad group — lowest level possible)")
    header: str = Field(
        ..., description="Snippet header/category (e.g., 'Brands', 'Types', 'Models', 'Destinations', 'Styles', 'Courses', 'Amenities', 'Insurance coverage', 'Neighborhoods', 'Service catalog', 'Shows', 'Degree programs')"
    )
    values: List[str] = Field(
        ..., description="List of snippet values (min 3, max 10, each max 25 chars)"
    )


class CreatePromotionAssetRequest(BaseModel):
    """Request model for creating promotion assets"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(None, description="Campaign ID (if linking to campaign)")
    ad_group_id: Optional[str] = Field(None, description="Ad group ID (if linking to specific ad group — lowest level possible)")
    promotion_target: str = Field(..., description="What is being promoted (max 20 chars)")
    percent_off: Optional[int] = Field(None, description="Percentage discount (e.g., 50 for 50%)")
    money_amount_off_micros: Optional[int] = Field(None, description="Money discount in micros")
    money_amount_off_currency: Optional[str] = Field(None, description="Currency code (e.g., 'TRY', 'USD', 'PLN')")
    occasion: Optional[str] = Field(
        None, description="Promotion occasion (e.g., 'NEW_YEARS', 'VALENTINES_DAY', 'EASTER', 'MOTHERS_DAY', 'FATHERS_DAY', 'LABOR_DAY', 'BACK_TO_SCHOOL', 'HALLOWEEN', 'BLACK_FRIDAY', 'CYBER_MONDAY', 'CHRISTMAS', 'BOXING_DAY', 'INDEPENDENCE_DAY', 'NATIONAL_DAY', 'END_OF_SEASON', 'WINTER_SALE', 'SUMMER_SALE', 'FALL_SALE', 'SPRING_SALE', 'RAMADAN', 'EID_AL_FITR', 'EID_AL_ADHA', 'SINGLES_DAY', 'WOMENS_DAY', 'HOLI', 'PARENTS_DAY', 'ST_NICHOLAS_DAY', 'CARNIVAL', 'EPIPHANY', 'ROSH_HASHANAH', 'PASSOVER', 'HANUKKAH', 'DIWALI', 'NAVRATRI', 'SONGKRAN', 'YEAR_END_GIFT')"
    )
    language_code: str = Field("tr", description="Language code (e.g., 'tr', 'pl', 'en')")
    final_urls: List[str] = Field(..., description="Landing page URLs for the promotion")
    promotion_start_date: Optional[str] = Field(None, description="Start date YYYY-MM-DD")
    promotion_end_date: Optional[str] = Field(None, description="End date YYYY-MM-DD")


class CreatePriceAssetRequest(BaseModel):
    """Request model for creating price assets"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(None, description="Campaign ID (if linking to campaign)")
    ad_group_id: Optional[str] = Field(None, description="Ad group ID (if linking to specific ad group — lowest level possible)")
    price_type: str = Field(
        "PRODUCT_CATEGORIES", description="Price type: 'BRANDS', 'EVENTS', 'LOCATIONS', 'NEIGHBORHOODS', 'PRODUCT_CATEGORIES', 'PRODUCT_TIERS', 'SERVICES', 'SERVICE_CATEGORIES', 'SERVICE_TIERS'"
    )
    language_code: str = Field("tr", description="Language code (e.g., 'tr', 'pl', 'en')")
    price_offerings: List[dict] = Field(
        ..., description="List of price offering dicts with: header (max 25 chars), description (max 25 chars), price_micros (int), currency_code (e.g., 'TRY'), unit (optional: 'PER_HOUR', 'PER_DAY', 'PER_WEEK', 'PER_MONTH', 'PER_YEAR', 'PER_NIGHT'), final_url (URL)"
    )


class CreateImageAssetForCampaignRequest(BaseModel):
    """Request model for creating image assets linked to campaigns (Search ads)"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(None, description="Campaign ID to link image to")
    ad_group_id: Optional[str] = Field(None, description="Ad group ID (if linking to specific ad group)")
    image_url: str = Field(..., description="Image URL to download")
    asset_name: Optional[str] = Field(None, description="Asset name")


class CreateBusinessIdentityAssetRequest(BaseModel):
    """Request model for setting business name and logo"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    business_name: Optional[str] = Field(None, description="Business name (max 25 chars)")
    logo_url: Optional[str] = Field(None, description="Logo image URL (1:1 aspect ratio, min 128x128)")
    landscape_logo_url: Optional[str] = Field(None, description="Landscape logo image URL (4:1 aspect ratio, min 512x128). Recommended for PMax campaigns.")


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


# ============================================================================
# PYDANTIC MODELS FOR MERCHANT CENTER — DATAFEEDS
# ============================================================================

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
    name: str = Field(..., description="Datafeed name (e.g., 'RO New Products Feed')")
    feed_url: str = Field(..., description="URL of the feed file (must be accessible by Google)")
    content_language: str = Field("en", description="Content language code (e.g., 'en', 'ro', 'pl')")
    target_country: str = Field(..., description="Target country ISO code (e.g., 'RO', 'PL', 'TR')")
    fetch_hour: Optional[int] = Field(None, description="Hour (0-23) to fetch the feed daily", ge=0, le=23)
    fetch_day: Optional[str] = Field(None, description="Day of week for weekly fetch: monday, tuesday, etc.")
    fetch_timezone: Optional[str] = Field("Europe/London", description="Timezone for fetch schedule")
    feed_format: Optional[str] = Field(None, description="Feed format: 'tsv', 'xml', 'csv'. Auto-detected if omitted.")


class MerchantUpdateDatafeedRequest(BaseModel):
    """Request model for updating an existing datafeed"""
    merchant_id: str = Field(..., description="Merchant ID")
    datafeed_id: str = Field(..., description="Datafeed ID (numeric)")
    name: Optional[str] = Field(None, description="New datafeed name")
    feed_url: Optional[str] = Field(None, description="New feed URL")
    fetch_hour: Optional[int] = Field(None, description="New fetch hour (0-23)", ge=0, le=23)
    fetch_day: Optional[str] = Field(None, description="New fetch day (for weekly schedule)")
    fetch_timezone: Optional[str] = Field(None, description="New timezone for fetch schedule")
    content_language: Optional[str] = Field(None, description="New content language code")
    target_country: Optional[str] = Field(None, description="New target country code")


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


# ============================================================================
# PYDANTIC MODELS FOR MERCHANT CENTER — ACCOUNT STATUSES
# ============================================================================

class MerchantListAccountStatusesRequest(BaseModel):
    """Request model for listing account statuses under MCA"""
    mca_id: str = Field(..., description="MCA (Multi-Client Account) ID")
    max_results: int = Field(50, description="Maximum number of results", ge=1, le=1000)
    page_token: Optional[str] = Field(None, description="Page token for pagination")


class MerchantGetAccountStatusRequest(BaseModel):
    """Request model for getting account status for a specific merchant"""
    merchant_id: str = Field(..., description="Merchant ID")
    account_id: Optional[str] = Field(None, description="Account ID (defaults to merchant_id)")


# ============================================================================
# PYDANTIC MODELS FOR MERCHANT CENTER — REPORTS
# ============================================================================

class MerchantReportSearchRequest(BaseModel):
    """Request model for Merchant Center reports (performance, best sellers, etc.)"""
    merchant_id: str = Field(..., description="Merchant ID")
    query: str = Field(..., description="MQL query for Merchant Center reports. Examples: "
                        "'SELECT segments.offer_id, metrics.impressions, metrics.clicks FROM MerchantPerformanceView WHERE segments.date BETWEEN \"2026-01-01\" AND \"2026-03-13\"', "
                        "'SELECT brand, rank, relative_demand FROM BestSellersBrandView WHERE report_country_code = \"RO\"', "
                        "'SELECT product_id, benchmark_price FROM PriceCompetitivenessProductView WHERE report_country_code = \"RO\"'")
    page_size: int = Field(1000, description="Max rows to return", ge=1, le=5000)
    page_token: Optional[str] = Field(None, description="Page token for pagination")


def _format_customer_id(customer_id: str) -> str:
    """Format customer ID from 'XXX-XXX-XXXX' to 'XXXXXXXXXX'"""
    return customer_id.replace("-", "")


def _to_micros(value: float) -> int:
    """Convert dollars to micros"""
    return int(value * 1_000_000)


def _link_assets_to_entity(customer_id: str, asset_resource_names: list, field_type, ad_group_id: str = None, campaign_id: str = None) -> str:
    """
    Link asset(s) to ad group, campaign, or account level.
    Priority: ad_group_id > campaign_id > account level.
    Returns a string describing where assets were linked.
    """
    if ad_group_id:
        ad_group_asset_service = google_ads_client.get_service("AdGroupAssetService")
        link_operations = []
        for asset_rn in asset_resource_names:
            link_op = google_ads_client.get_type("AdGroupAssetOperation")
            link = link_op.create
            link.asset = asset_rn
            link.ad_group = ad_group_asset_service.ad_group_path(customer_id, ad_group_id)
            link.field_type = field_type
            link_operations.append(link_op)
        ad_group_asset_service.mutate_ad_group_assets(
            customer_id=customer_id,
            operations=link_operations
        )
        _track_api_call('MUTATE_LINK', len(link_operations))
        return f"ad_group {ad_group_id}"
    elif campaign_id:
        campaign_asset_service = google_ads_client.get_service("CampaignAssetService")
        link_operations = []
        for asset_rn in asset_resource_names:
            link_op = google_ads_client.get_type("CampaignAssetOperation")
            link = link_op.create
            link.asset = asset_rn
            link.campaign = campaign_asset_service.campaign_path(customer_id, campaign_id)
            link.field_type = field_type
            link_operations.append(link_op)
        campaign_asset_service.mutate_campaign_assets(
            customer_id=customer_id,
            operations=link_operations
        )
        _track_api_call('MUTATE_LINK', len(link_operations))
        return f"campaign {campaign_id}"
    else:
        customer_asset_service = google_ads_client.get_service("CustomerAssetService")
        link_operations = []
        for asset_rn in asset_resource_names:
            link_op = google_ads_client.get_type("CustomerAssetOperation")
            link = link_op.create
            link.asset = asset_rn
            link.field_type = field_type
            link_operations.append(link_op)
        customer_asset_service.mutate_customer_assets(
            customer_id=customer_id,
            operations=link_operations
        )
        _track_api_call('MUTATE_LINK', len(link_operations))
        return "account"


def _from_micros(value: int) -> float:
    """Convert micros to dollars"""
    return value / 1_000_000


def _initialize_client():
    """Initialize Google Ads API client"""
    global google_ads_client

    credentials = {
        "developer_token": os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
        "client_id": os.getenv("GOOGLE_ADS_CLIENT_ID"),
        "client_secret": os.getenv("GOOGLE_ADS_CLIENT_SECRET"),
        "refresh_token": os.getenv("GOOGLE_ADS_REFRESH_TOKEN"),
        "login_customer_id": _format_customer_id(os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")),
        "use_proto_plus": True,
    }

    try:
        google_ads_client = GoogleAdsClient.load_from_dict(credentials)
        return True
    except Exception as e:
        raise RuntimeError(f"Failed to initialize Google Ads client: {str(e)}")


def _ensure_client():
    """Ensure client is initialized before use"""
    global google_ads_client
    if google_ads_client is None:
        _initialize_client()


def _initialize_merchant_client():
    """Initialize Google Merchant Center API client"""
    global merchant_center_client

    client_id = os.getenv("GOOGLE_ADS_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_ADS_CLIENT_SECRET")
    # Use dedicated Merchant Center token if available
    # Falls back to Google Ads token if not set
    refresh_token = os.getenv("MERCHANT_CENTER_REFRESH_TOKEN") or os.getenv("GOOGLE_ADS_REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        raise RuntimeError("Missing Merchant Center OAuth credentials in .env")

    try:
        credentials = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret
        )
        merchant_center_client = build('content', 'v2.1', credentials=credentials)
        return True
    except Exception as e:
        raise RuntimeError(f"Failed to initialize Merchant Center client: {str(e)}")


def _ensure_merchant_client():
    """Ensure Merchant Center client is initialized before use"""
    global merchant_center_client
    if merchant_center_client is None:
        _initialize_merchant_client()


# Helper function to execute GAQL queries
def _execute_gaql(customer_id: str, query: str, page_size: int = 100) -> list:
    """Execute a GAQL query and return results"""
    _ensure_client()
    ga_service = google_ads_client.get_service("GoogleAdsService")
    customer_id_formatted = _format_customer_id(customer_id)

    results = []
    try:
        stream = ga_service.search_stream(customer_id=customer_id_formatted, query=query)
        _track_api_call('GAQL')
        for batch in stream:
            for row in batch.results:
                results.append(row)
    except Exception as e:
        raise RuntimeError(f"GAQL query failed: {str(e)}")

    return results


def _safe_get_value(obj, path: str, default=None):
    """Safely get nested attribute values"""
    try:
        current = obj
        for part in path.split("."):
            if hasattr(current, part):
                current = getattr(current, part)
            else:
                return default
        return current
    except (AttributeError, TypeError):
        return default


# READ TOOLS

class ShoppingPerformanceRequest(BaseModel):
    """Request model for shopping performance report"""
    customer_id: str = Field(..., description="Customer ID")
    start_date: str = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: str = Field(..., description="End date (YYYY-MM-DD)")
    campaign_id: Optional[str] = Field(None, description="Filter by campaign ID")
    min_impressions: int = Field(10, description="Minimum impressions filter")
    page_size: int = Field(100, description="Number of results per page", ge=1, le=1000)


class ExecuteGaqlRequest(BaseModel):
    """Request model for executing raw GAQL query"""
    customer_id: str = Field(..., description="Customer ID")
    query: str = Field(..., description="Raw GAQL query")
    page_size: int = Field(100, description="Number of results per page", ge=1, le=1000)


class ListAdsRequest(BaseModel):
    """Request model for listing ads"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: Optional[str] = Field(None, description="Filter by ad group ID")
    campaign_id: Optional[str] = Field(None, description="Filter by campaign ID")
    status: Optional[Literal["ENABLED", "PAUSED", "REMOVED"]] = Field(None, description="Filter by status")
    page_size: int = Field(50, description="Number of results per page", ge=1, le=1000)


class ListCampaignCriteriaRequest(BaseModel):
    """Request model for listing campaign criteria"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    criterion_type: Optional[Literal["LOCATION", "LANGUAGE", "ALL"]] = Field("ALL", description="Filter by criterion type")


class CreateAdScheduleRequest(BaseModel):
    """Request model for creating ad schedule criteria with bid modifiers"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    schedules: List[dict] = Field(
        ..., description="List of schedule dicts with: day_of_week (MONDAY-SUNDAY), start_hour (0-23), start_minute (ZERO/FIFTEEN/THIRTY/FORTY_FIVE), end_hour (0-24), end_minute (ZERO/FIFTEEN/THIRTY/FORTY_FIVE), bid_modifier (float, e.g. 1.2 for +20%, 0.8 for -20%, 0.0 to exclude)"
    )


class RemoveAdScheduleRequest(BaseModel):
    """Request model for removing ad schedule criteria"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    criterion_ids: Optional[List[str]] = Field(
        None, description="Specific criterion IDs to remove. If None, removes ALL ad schedules from the campaign."
    )


class ListAdSchedulesRequest(BaseModel):
    """Request model for listing ad schedule criteria"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")


class UpdateDeviceBidModifierRequest(BaseModel):
    """Request model for updating device bid modifiers on a campaign"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    device_modifiers: List[dict] = Field(
        ..., description="List of dicts with: device_type (DESKTOP, MOBILE, TABLET), bid_modifier (float, e.g. 0.7 for -30%, 1.2 for +20%, 1.0 for no change). Uses fixed criterion IDs: DESKTOP=30000, MOBILE=30001, TABLET=30002."
    )


class ListConversionActionsRequest(BaseModel):
    """Request model for listing conversion actions"""
    customer_id: str = Field(..., description="Customer ID")
    status: Optional[Literal["ENABLED", "PAUSED", "REMOVED"]] = Field(None, description="Filter by status")


class ListRecommendationsRequest(BaseModel):
    """Request model for listing recommendations"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(None, description="Filter by campaign ID")
    page_size: int = Field(50, description="Number of results per page", ge=1, le=1000)


class UpdateAdGroupStatusRequest(BaseModel):
    """Request model for updating ad group status"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    status: Literal["ENABLED", "PAUSED", "REMOVED"] = Field(..., description="New ad group status (REMOVED permanently deletes the ad group)")


class UpdateAdGroupBidRequest(BaseModel):
    """Request model for updating ad group bid"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    cpc_bid_micros: int = Field(..., description="CPC bid in micros")


class UpdateAdStatusRequest(BaseModel):
    """Request model for updating ad status"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    ad_id: str = Field(..., description="Ad ID")
    status: Literal["ENABLED", "PAUSED"] = Field(..., description="New ad status")


class RemoveCampaignRequest(BaseModel):
    """Request model for removing campaign"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")


class UpdateCampaignBiddingStrategyRequest(BaseModel):
    """Request model for updating campaign bidding strategy"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    bidding_strategy: Literal["MANUAL_CPC", "TARGET_CPA", "TARGET_ROAS", "MAXIMIZE_CONVERSIONS", "MAXIMIZE_CONVERSION_VALUE"] = Field(..., description="Bidding strategy")
    target_cpa_micros: Optional[int] = Field(None, description="Target CPA in micros")
    target_roas: Optional[float] = Field(None, description="Target ROAS")
    enhanced_cpc: bool = Field(True, description="Enable enhanced CPC")


class SetCampaignGeoTargetRequest(BaseModel):
    """Request model for setting campaign geo targets"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    geo_target_constant_ids: List[str] = Field(..., description="Geo target constant IDs (e.g. '2616' for Poland)")
    negative: bool = Field(False, description="Mark as negative target (exclusion)")


class SetCampaignLanguageRequest(BaseModel):
    """Request model for setting campaign language"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    language_ids: List[str] = Field(..., description="Language constant IDs (e.g. '1017' for Polish)")


class SetCampaignLocationOptionsRequest(BaseModel):
    """Request model for setting campaign location targeting options"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    positive_geo_target_type: Literal["PRESENCE", "PRESENCE_OR_INTEREST"] = Field("PRESENCE", description="Location targeting: PRESENCE (people IN location) or PRESENCE_OR_INTEREST (people in OR interested in location)")
    negative_geo_target_type: Literal["PRESENCE", "PRESENCE_OR_INTEREST"] = Field("PRESENCE", description="Negative location targeting type")


class SetCampaignDsaSettingsRequest(BaseModel):
    """Request model for setting Dynamic Search Ads settings"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    domain_name: str = Field(..., description="Website domain for DSA (e.g., 'example.com')")
    language_code: str = Field(..., description="Language code for DSA (e.g., 'it', 'pl', 'de')")


class SetCampaignConversionGoalsRequest(BaseModel):
    """Request model for setting campaign-level conversion goals"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    purchase_goal: bool = Field(True, description="Enable Purchases (Website) goal")
    lead_form_goal: bool = Field(True, description="Enable Submit lead forms (Website) goal")
    add_to_cart_goal: bool = Field(False, description="Enable Add to cart (Website) goal")
    begin_checkout_goal: bool = Field(False, description="Enable Begin checkout (Website) goal")
    contact_goal: bool = Field(False, description="Enable Contacts (Website) goal")


class CreateSearchCampaignRequest(BaseModel):
    """Request model for creating search campaign"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_name: str = Field(..., description="Campaign name")
    daily_budget_micros: int = Field(..., description="Daily budget in micros (e.g., 100000000 = $100)")
    bidding_strategy: Literal["MANUAL_CPC", "TARGET_CPA", "MAXIMIZE_CONVERSIONS", "MAXIMIZE_CONVERSION_VALUE"] = Field("MAXIMIZE_CONVERSION_VALUE", description="Bidding strategy")
    target_cpa_micros: Optional[int] = Field(None, description="Target CPA in micros (for TARGET_CPA)")
    target_roas: Optional[float] = Field(None, description="Target ROAS (for MAXIMIZE_CONVERSION_VALUE, e.g. 1.6)")
    enhanced_cpc: bool = Field(True, description="Enable enhanced CPC (for MANUAL_CPC)")
    status: Literal["ENABLED", "PAUSED"] = Field("PAUSED", description="Campaign status")
    network_settings_search: bool = Field(False, description="Enable Search Network partners (target_search_network). DISABLED by default for NEW_PRODUCTS campaigns.")
    network_settings_search_partners: bool = Field(False, description="Enable Search Partners (deprecated in newer API versions)")
    network_settings_display: bool = Field(False, description="Enable Display network")


class CreateSearchAdGroupRequest(BaseModel):
    """Request model for creating search ad group"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    ad_group_name: str = Field(..., description="Ad group name")
    cpc_bid_micros: int = Field(1000000, description="CPC bid in micros (default $1.00)")


class CreateResponsiveSearchAdRequest(BaseModel):
    """Request model for creating responsive search ad"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    headlines: list = Field(..., description="Headlines (3-15 required). Each item can be a string or a dict with 'text' and optional 'pinned_field' (HEADLINE_1, HEADLINE_2, or HEADLINE_3)")
    descriptions: list = Field(..., description="Descriptions (2-4 required). Each item can be a string or a dict with 'text' and optional 'pinned_field' (DESCRIPTION_1 or DESCRIPTION_2)")
    final_urls: List[str] = Field(..., description="Final URLs")
    path1: Optional[str] = Field(None, description="Display URL path 1")
    path2: Optional[str] = Field(None, description="Display URL path 2")


class AddKeywordRequest(BaseModel):
    """Request model for adding keyword to ad group"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    keyword_text: str = Field(..., description="Keyword text")
    match_type: Literal["BROAD", "PHRASE", "EXACT"] = Field("BROAD", description="Match type")
    cpc_bid_micros: Optional[int] = Field(None, description="CPC bid override in micros")


class AddProductExclusionRequest(BaseModel):
    """Request model for adding product exclusion in Shopping campaign"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    item_ids: List[str] = Field(..., description="Product item IDs to exclude")


class CreateImageAssetRequest(BaseModel):
    """Request model for creating image asset and linking to asset group"""
    customer_id: str = Field(..., description="Customer ID")
    asset_group_id: str = Field(..., description="Asset group ID")
    image_url: str = Field(..., description="Image URL to download")
    asset_name: Optional[str] = Field(None, description="Asset name")


# ============================================================================
# PYDANTIC MODELS FOR CAMPAIGN CREATION
# ============================================================================

class CreateCampaignBudgetRequest(BaseModel):
    """Request model for creating a shared campaign budget"""
    customer_id: str = Field(..., description="Customer ID")
    budget_name: str = Field(..., description="Name for the budget")
    daily_budget_micros: int = Field(
        ..., description="Daily budget in micros (e.g., 100000000 = $100)"
    )


class CreateShoppingCampaignRequest(BaseModel):
    """Request model for creating a Standard Shopping campaign"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_name: str = Field(..., description="Name for the campaign")
    daily_budget_micros: int = Field(
        ..., description="Daily budget in micros (e.g., 100000000 = $100)"
    )
    merchant_id: str = Field(..., description="Google Merchant Center merchant ID")
    sales_country: str = Field(
        ..., description="Sales country code (e.g., 'US', 'PL', 'GB')"
    )
    feed_label: Optional[str] = Field(
        None, description="Feed label to filter products (e.g., 'PLFEEDNEW' for NEW Products feed). If not set, uses all products from the merchant."
    )
    campaign_priority: str = Field(
        "MEDIUM", description="Campaign priority: LOW, MEDIUM, or HIGH"
    )
    enable_local_inventory: bool = Field(
        False, description="Enable local inventory ads"
    )
    status: str = Field(
        "ENABLED", description="Campaign status: ENABLED or PAUSED"
    )


class CreateShoppingAdGroupRequest(BaseModel):
    """Request model for creating an ad group in a Shopping campaign"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    ad_group_name: str = Field(..., description="Name for the ad group")
    cpc_bid_micros: int = Field(
        ..., description="Max CPC bid in micros (e.g., 1000000 = $1.00)"
    )


class CreateShoppingAdRequest(BaseModel):
    """Request model for creating a Shopping ad"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")


class CreatePMaxCampaignRequest(BaseModel):
    """Request model for creating a Performance Max campaign"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_name: str = Field(..., description="Name for the campaign")
    daily_budget_micros: int = Field(
        ..., description="Daily budget in micros (e.g., 100000000 = $100)"
    )
    merchant_id: str = Field(..., description="Google Merchant Center merchant ID")
    sales_country: str = Field(
        ..., description="Sales country code (e.g., 'US', 'PL', 'GB')"
    )
    final_url: str = Field(..., description="Final URL for the campaign")
    status: str = Field(
        "ENABLED", description="Campaign status: ENABLED or PAUSED"
    )


class CreateAssetGroupRequest(BaseModel):
    """Request model for creating an asset group in a PMax campaign"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    asset_group_name: str = Field(..., description="Name for the asset group")
    final_url: str = Field(..., description="Final URL for the asset group")
    final_mobile_url: Optional[str] = Field(
        None, description="Final mobile URL (optional)"
    )


class CreateAssetGroupAssetsRequest(BaseModel):
    """Request model for creating text assets for an asset group"""
    customer_id: str = Field(..., description="Customer ID")
    asset_group_id: str = Field(..., description="Asset group ID")
    headlines: List[str] = Field(
        ..., description="List of headlines (3-15 headlines recommended)"
    )
    descriptions: List[str] = Field(
        ..., description="List of descriptions (2-4 descriptions recommended)"
    )
    long_headlines: Optional[List[str]] = Field(
        None, description="List of long headlines (optional, 0-5)"
    )
    business_name: str = Field(..., description="Business name")
    call_to_action_selection: Optional[str] = Field(
        None, description="Call-to-action (optional, e.g., SHOP_NOW, LEARN_MORE)"
    )


class CreateAssetGroupListingFilterRequest(BaseModel):
    """Request model for creating a product listing filter for asset group"""
    customer_id: str = Field(..., description="Customer ID")
    asset_group_id: str = Field(..., description="Asset group ID")
    filter_type: str = Field(
        "ALL_PRODUCTS", description="Filter type: ALL_PRODUCTS or CUSTOM"
    )
    product_brand: Optional[str] = Field(None, description="Product brand filter")
    product_type: Optional[str] = Field(None, description="Product type filter")
    custom_label_0: Optional[str] = Field(None, description="Custom label 0")
    custom_label_1: Optional[str] = Field(None, description="Custom label 1")
    custom_label_2: Optional[str] = Field(None, description="Custom label 2")
    custom_label_3: Optional[str] = Field(None, description="Custom label 3")
    custom_label_4: Optional[str] = Field(None, description="Custom label 4")


# ============================================================================
# CAMPAIGN CREATION TOOLS
# ============================================================================

@mcp.tool()
async def google_ads_create_campaign_budget(request: CreateCampaignBudgetRequest) -> dict:
    """
    Create a shared campaign budget.

    Shared budgets can be used by multiple campaigns to manage spending across
    multiple campaigns together.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        budget_service = google_ads_client.get_service("CampaignBudgetService")

        # Create budget operation
        operation = google_ads_client.get_type("CampaignBudgetOperation")
        budget = operation.create

        # Set budget fields
        budget.name = request.budget_name
        budget.amount_micros = request.daily_budget_micros
        budget.delivery_method = google_ads_client.enums.BudgetDeliveryMethodEnum.STANDARD

        # Perform the mutation
        response = budget_service.mutate_campaign_budgets(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_BUDGET')

        budget_resource_name = response.results[0].resource_name
        budget_id = budget_resource_name.split("/")[-1]

        return {
            "status": "success",
            "message": f"Budget '{request.budget_name}' created successfully",
            "budget_id": budget_id,
            "budget_resource_name": budget_resource_name,
            "daily_budget": _from_micros(request.daily_budget_micros)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create budget: {str(e)}"
        }


class ShoppingListingGroupSubdivisionRequest(BaseModel):
    """Request model for creating a listing group subdivision in Shopping campaign"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    parent_criterion_id: str = Field(
        ..., description="Parent listing group criterion ID to subdivide. Use the root criterion ID (type=SUBDIVISION at top level) to create first-level splits."
    )
    dimension: str = Field(
        ..., description="Dimension to subdivide by: custom_label_0, custom_label_1, custom_label_2, custom_label_3, custom_label_4, product_type, brand, channel, condition"
    )
    values: List[str] = Field(
        ..., description="List of dimension values to create as UNIT nodes (e.g., ['keto-diet-gr', 'prostonix-gr']). An 'Everything else' catch-all UNIT is auto-created."
    )
    cpc_bid_micros: int = Field(
        10000, description="Default CPC bid in micros for each new UNIT node (default 0.01)"
    )
    everything_else_bid_micros: int = Field(
        10000, description="CPC bid for the 'Everything else' catch-all node (default 0.01)"
    )


class ShoppingListingGroupBidRequest(BaseModel):
    """Request model for updating bids on Shopping listing group nodes"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    criterion_bids: List[dict] = Field(
        ..., description="List of {criterion_id: str, cpc_bid_micros: int} dicts to update bids on UNIT nodes"
    )


class ShoppingListingGroupRemoveRequest(BaseModel):
    """Request model for removing Shopping listing group nodes"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    criterion_ids: List[str] = Field(
        ..., description="List of criterion IDs to remove. Removing a SUBDIVISION node also removes all its children."
    )


class RebuildShoppingListingGroupTreeRequest(BaseModel):
    """Request model for atomic rebuild of listing group tree"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    root_dimension: str = Field(
        ..., description="Dimension for first-level split: custom_label_0, custom_label_1, custom_label_2, custom_label_3, custom_label_4, product_type, brand"
    )
    groups: List[dict] = Field(
        ..., description="List of product group definitions: [{value: str, cpc_bid_micros: int, subdivision: {dimension: str, children: [{value: str, cpc_bid_micros: int}]}}]. Use 'subdivision' for 2-level trees."
    )
    everything_else_bid_micros: int = Field(
        10000, description="CPC bid for the root-level 'Everything else' catch-all node"
    )


@mcp.tool()
async def google_ads_update_ad_group_status(request: UpdateAdGroupStatusRequest) -> dict:
    """Update ad group status (enable, pause, or permanently remove)."""
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ad_group_service = google_ads_client.get_service("AdGroupService")

        # REMOVED uses a different operation type (remove vs update)
        if request.status == "REMOVED":
            operation = google_ads_client.get_type("AdGroupOperation")
            resource_name = ad_group_service.ad_group_path(customer_id, request.ad_group_id)
            operation.remove = resource_name

            result = ad_group_service.mutate_ad_groups(
                customer_id=customer_id,
                operations=[operation]
            )
            _track_api_call('MUTATE_AD_GROUP')

            return {
                "status": "success",
                "message": f"Ad group {request.ad_group_id} permanently REMOVED",
                "resource_name": result.results[0].resource_name
            }

        operation = google_ads_client.get_type("AdGroupOperation")
        ad_group = operation.update
        ad_group.resource_name = ad_group_service.ad_group_path(customer_id, request.ad_group_id)

        status_map = {
            "ENABLED": google_ads_client.enums.AdGroupStatusEnum.ENABLED,
            "PAUSED": google_ads_client.enums.AdGroupStatusEnum.PAUSED
        }
        ad_group.status = status_map[request.status]

        from google.protobuf import field_mask_pb2
        fm = field_mask_pb2.FieldMask(paths=["status"])
        operation.update_mask = fm

        result = ad_group_service.mutate_ad_groups(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_AD_GROUP')

        return {
            "status": "success",
            "message": f"Ad group {request.ad_group_id} status updated to {request.status}",
            "resource_name": result.results[0].resource_name
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to update ad group status: {str(e)}"
        }


@mcp.tool()
async def google_ads_update_ad_group_bid(request: UpdateAdGroupBidRequest) -> dict:
    """Update ad group default CPC bid."""
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ad_group_service = google_ads_client.get_service("AdGroupService")

        operation = google_ads_client.get_type("AdGroupOperation")
        ad_group = operation.update
        ad_group.resource_name = ad_group_service.ad_group_path(customer_id, request.ad_group_id)
        ad_group.cpc_bid_micros = request.cpc_bid_micros

        from google.protobuf import field_mask_pb2
        fm = field_mask_pb2.FieldMask(paths=["cpc_bid_micros"])
        operation.update_mask = fm

        result = ad_group_service.mutate_ad_groups(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_AD_GROUP')

        return {
            "status": "success",
            "message": f"Ad group {request.ad_group_id} bid updated to ${_from_micros(request.cpc_bid_micros)}",
            "resource_name": result.results[0].resource_name
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to update ad group bid: {str(e)}"
        }


@mcp.tool()
async def google_ads_remove_campaign(request: RemoveCampaignRequest) -> dict:
    """Remove (permanently delete) a campaign. Sets status to REMOVED."""
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_service = google_ads_client.get_service("CampaignService")

        operation = google_ads_client.get_type("CampaignOperation")
        operation.remove = campaign_service.campaign_path(customer_id, request.campaign_id)

        result = campaign_service.mutate_campaigns(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_CAMPAIGN')

        return {
            "status": "success",
            "message": f"Campaign {request.campaign_id} removed",
            "resource_name": result.results[0].resource_name
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to remove campaign: {str(e)}"
        }


@mcp.tool()
async def google_ads_update_campaign_bidding_strategy(request: UpdateCampaignBiddingStrategyRequest) -> dict:
    """
    Update campaign bidding strategy.

    Supports MANUAL_CPC, TARGET_CPA, TARGET_ROAS, MAXIMIZE_CONVERSIONS, and MAXIMIZE_CONVERSION_VALUE.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_service = google_ads_client.get_service("CampaignService")

        operation = google_ads_client.get_type("CampaignOperation")
        campaign = operation.update
        campaign.resource_name = campaign_service.campaign_path(customer_id, request.campaign_id)

        field_paths = []

        if request.bidding_strategy == "MANUAL_CPC":
            campaign.manual_cpc.enhanced_cpc_enabled = request.enhanced_cpc
            field_paths.append("manual_cpc.enhanced_cpc_enabled")

        elif request.bidding_strategy == "TARGET_CPA":
            if request.target_cpa_micros is not None:
                campaign.target_cpa.target_cpa_micros = request.target_cpa_micros
                field_paths.append("target_cpa.target_cpa_micros")

        elif request.bidding_strategy == "TARGET_ROAS":
            if request.target_roas is not None:
                campaign.target_roas.target_roas = request.target_roas
                field_paths.append("target_roas.target_roas")

        elif request.bidding_strategy == "MAXIMIZE_CONVERSIONS":
            campaign.maximize_conversions.target_cpa_micros = 0
            field_paths.append("maximize_conversions.target_cpa_micros")

        elif request.bidding_strategy == "MAXIMIZE_CONVERSION_VALUE":
            campaign.maximize_conversion_value.target_roas = request.target_roas or 0
            field_paths.append("maximize_conversion_value.target_roas")

        from google.protobuf import field_mask_pb2
        fm = field_mask_pb2.FieldMask(paths=field_paths)
        operation.update_mask = fm

        result = campaign_service.mutate_campaigns(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_CAMPAIGN')

        return {
            "status": "success",
            "message": f"Campaign {request.campaign_id} bidding strategy updated to {request.bidding_strategy}",
            "resource_name": result.results[0].resource_name
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to update campaign bidding strategy: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_search_campaign(request: CreateSearchCampaignRequest) -> dict:
    """
    Create a new Search campaign with budget and bidding strategy.

    Automatically creates a dedicated budget. Default status is PAUSED.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_budget_service = google_ads_client.get_service("CampaignBudgetService")
        campaign_service = google_ads_client.get_service("CampaignService")

        # Step 1: Create budget (or find existing one with same name)
        budget_name = f"{request.campaign_name} Budget"
        budget_resource_name = None
        try:
            budget_operation = google_ads_client.get_type("CampaignBudgetOperation")
            budget = budget_operation.create
            budget.name = budget_name
            budget.amount_micros = request.daily_budget_micros
            budget.explicitly_shared = False  # Dedicated budget — compatible with all bidding strategies

            budget_result = campaign_budget_service.mutate_campaign_budgets(
                customer_id=customer_id,
                operations=[budget_operation]
            )
            _track_api_call('MUTATE_BUDGET')
            budget_resource_name = budget_result.results[0].resource_name
        except Exception as budget_err:
            if "DUPLICATE_NAME" in str(budget_err):
                # Budget already exists — find it by name
                ga_service = google_ads_client.get_service("GoogleAdsService")
                query = f"SELECT campaign_budget.resource_name FROM campaign_budget WHERE campaign_budget.name = '{budget_name}'"
                response = ga_service.search(customer_id=customer_id, query=query)
                _track_api_call('SEARCH_BUDGET')
                for row in response:
                    budget_resource_name = row.campaign_budget.resource_name
                    break
                if not budget_resource_name:
                    raise Exception(f"Budget '{budget_name}' exists but could not be found")
            else:
                raise

        # Step 2: Create campaign
        campaign_operation = google_ads_client.get_type("CampaignOperation")
        campaign = campaign_operation.create
        campaign.name = request.campaign_name
        campaign.campaign_budget = budget_resource_name
        campaign.advertising_channel_type = google_ads_client.enums.AdvertisingChannelTypeEnum.SEARCH

        if request.status == "PAUSED":
            campaign.status = google_ads_client.enums.CampaignStatusEnum.PAUSED
        else:
            campaign.status = google_ads_client.enums.CampaignStatusEnum.ENABLED

        # Network settings
        campaign.network_settings.target_search_network = request.network_settings_search
        try:
            campaign.network_settings.target_search_partners = request.network_settings_search_partners
        except (AttributeError, ValueError):
            pass  # Field removed in newer API versions
        campaign.network_settings.target_content_network = request.network_settings_display

        # Bidding strategy
        if request.bidding_strategy == "MANUAL_CPC":
            campaign.manual_cpc.enhanced_cpc_enabled = request.enhanced_cpc
        elif request.bidding_strategy == "TARGET_CPA":
            if request.target_cpa_micros:
                campaign.target_cpa.target_cpa_micros = request.target_cpa_micros
        elif request.bidding_strategy == "MAXIMIZE_CONVERSIONS":
            campaign.maximize_conversions.target_cpa_micros = 0
        elif request.bidding_strategy == "MAXIMIZE_CONVERSION_VALUE":
            if request.target_roas:
                campaign.maximize_conversion_value.target_roas = request.target_roas
            else:
                campaign.maximize_conversion_value.target_roas = 0  # No target ROAS constraint

        # EU political advertising declaration (required since API v19)
        # This is an ENUM (not bool!): 3 = DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING
        try:
            campaign.contains_eu_political_advertising = 3
        except Exception:
            pass

        result = campaign_service.mutate_campaigns(
            customer_id=customer_id,
            operations=[campaign_operation]
        )
        _track_api_call('MUTATE_CAMPAIGN')

        campaign_resource = result.results[0].resource_name
        campaign_id = campaign_resource.split("/")[-1]

        return {
            "status": "success",
            "message": f"Search campaign '{request.campaign_name}' created",
            "campaign_id": campaign_id,
            "campaign_resource_name": campaign_resource,
            "budget_resource_name": budget_resource_name
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create search campaign: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_search_ad_group(request: CreateSearchAdGroupRequest) -> dict:
    """Create a Search ad group in a campaign."""
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ad_group_service = google_ads_client.get_service("AdGroupService")

        operation = google_ads_client.get_type("AdGroupOperation")
        ad_group = operation.create
        ad_group.name = request.ad_group_name
        ad_group.campaign = ad_group_service.campaign_path(customer_id, request.campaign_id)
        ad_group.type_ = google_ads_client.enums.AdGroupTypeEnum.SEARCH_STANDARD
        ad_group.cpc_bid_micros = request.cpc_bid_micros

        result = ad_group_service.mutate_ad_groups(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_AD_GROUP')

        ad_group_resource = result.results[0].resource_name
        ad_group_id = ad_group_resource.split("/")[-1]

        return {
            "status": "success",
            "message": f"Search ad group '{request.ad_group_name}' created",
            "ad_group_id": ad_group_id,
            "ad_group_resource_name": ad_group_resource
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create search ad group: {str(e)}"
        }


class RemoveCampaignAssetLinkRequest(BaseModel):
    """Request model for removing campaign asset links"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_asset_resource_names: List[str] = Field(
        ..., description="List of campaign_asset resource names to remove (e.g., 'customers/{customer_id}/campaignAssets/{asset_id}~{type}~{status}')"
    )


class RemoveAdGroupAssetLinkRequest(BaseModel):
    """Request model for removing ad group asset links"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_asset_resource_names: List[str] = Field(
        ..., description="List of ad_group_asset resource names to remove"
    )


class ConversionDiagnosticsRequest(BaseModel):
    """Request model for conversion tracking diagnostics"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(None, description="Campaign ID to focus on (None = account-wide)")
    days_back: int = Field(90, description="Number of days to analyze", ge=7, le=365)


class UpdateProductCustomLabelsRequest(BaseModel):
    """Request model for updating product custom labels in Merchant Center"""
    merchant_id: str = Field(..., description="Merchant Center account ID")
    product_id: str = Field(..., description="Product ID (format: online:pl:PL:handle)")
    custom_labels: dict = Field(
        ...,
        description="Dict of custom_label_N: value. E.g., {'custom_label_0': 'hero', 'custom_label_1': 'solar'}"
    )


class PMaxNegativeKeywordRequest(BaseModel):
    """Request for adding negative keywords at PMax campaign level via brand exclusion lists"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="PMax Campaign ID")
    keywords: List[str] = Field(..., description="Negative keywords to add")
    match_type: str = Field("EXACT", description="Match type: EXACT or PHRASE")


class AccountPerformanceSummaryRequest(BaseModel):
    """Request for per-account performance summary"""
    customer_id: str = Field(..., description="Customer ID")
    days_back: int = Field(30, description="Days to analyze", ge=1, le=365)




# ============================================================================
# MODULE IMPORTS - Load all sub-modules at the end to register their tools
# ============================================================================
# Each module uses the shared 'mcp' instance defined above to register tools.
# Importing them here ensures all @mcp.tool() decorators are executed and
# the tools are available when the FastMCP server starts.

try:
    from . import mcp_campaigns
except ImportError:
    import mcp_campaigns

try:
    from . import mcp_keywords
except ImportError:
    import mcp_keywords

try:
    from . import mcp_ads_and_assets
except ImportError:
    import mcp_ads_and_assets

try:
    from . import mcp_shopping
except ImportError:
    import mcp_shopping

try:
    from . import mcp_targeting
except ImportError:
    import mcp_targeting

try:
    from . import mcp_reporting
except ImportError:
    import mcp_reporting

try:
    from . import mcp_merchant
except ImportError:
    import mcp_merchant

try:
    from . import mcp_pagespeed
except ImportError:
    import mcp_pagespeed

# ============================================================================
# BATCH MODULE IMPORTS - Register tools from batch_optimizer, batch_intelligence, batch_analytics
# ============================================================================
# These modules define their own register_*_tools(mcp_app) functions
# that register additional @mcp.tool() handlers on our shared mcp instance.

# Register batch optimizer tools (product setup, images, monitoring, shopping, warmup, etc.)
try:
    from batch_optimizer import register_batch_tools
    _batch_tools = register_batch_tools(mcp)
except ImportError:
    pass  # batch_optimizer not available

# Register batch intelligence tools (cross-campaign keyword miner)
try:
    from batch_intelligence import register_intelligence_tools
    _intel_tools = register_intelligence_tools(mcp)
except ImportError:
    pass  # batch_intelligence not available

# Register batch analytics tools (asset performance tracker)
try:
    from batch_analytics import register_analytics_tools
    _analytics_tools = register_analytics_tools(mcp)
except ImportError:
    pass  # batch_analytics not available


if __name__ == "__main__":
    mcp.run()
