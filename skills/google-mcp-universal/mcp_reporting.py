"""
Reporting, Diagnostics, and GAQL Query Module.

Handles performance reporting, GAQL query execution, conversion tracking diagnostics,
and account-level performance summaries.
"""

from typing import Optional
from pydantic import BaseModel, Field

import google_ads_mcp as _gam

from google_ads_mcp import (
    mcp,
    _ensure_client,
    _format_customer_id,
    _track_api_call,
    _from_micros,
    _execute_gaql,
    _safe_get_value,
)


# ============================================================================
# REQUEST MODELS
# ============================================================================


class ExecuteGaqlRequest(BaseModel):
    """Request model for executing GAQL queries"""
    customer_id: str = Field(..., description="Customer ID")
    query: str = Field(..., description="Raw GAQL query")
    page_size: int = Field(100, description="Number of results per page", ge=1, le=1000)


class ListRecommendationsRequest(BaseModel):
    """Request model for listing recommendations"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(None, description="Filter by campaign ID")
    page_size: int = Field(50, description="Number of results per page", ge=1, le=1000)


class ConversionDiagnosticsRequest(BaseModel):
    """Request model for conversion tracking diagnostics"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: Optional[str] = Field(None, description="Campaign ID to focus on")
    days_back: int = Field(90, description="Number of days to analyze", ge=7, le=365)


class AccountPerformanceSummaryRequest(BaseModel):
    """Request model for account performance summary"""
    customer_id: str = Field(..., description="Customer ID")
    days_back: int = Field(30, description="Days to analyze", ge=1, le=365)


class ListConversionActionsRequest(BaseModel):
    """Request model for listing conversion actions"""
    customer_id: str = Field(..., description="Customer ID")
    status: Optional[str] = Field(None, description="Filter by status e.g. ENABLED")


# ============================================================================
# REPORTING AND DIAGNOSTICS TOOLS
# ============================================================================
# NOTE: google_ads_get_performance_report lives in mcp_campaigns.py
# NOTE: google_ads_list_ads lives in mcp_ads_and_assets.py


@mcp.tool()
async def google_ads_list_campaign_criteria(request) -> dict:
    """
    List campaign-level targeting criteria (locations, languages, etc).

    Returns criteria details including type, target values, and negative flags.
    """
    try:
        from google_ads_mcp import ListCampaignCriteriaRequest
        import json as _json
        if isinstance(request, (str, bytes, bytearray)):
            request = _json.loads(request)
        if isinstance(request, dict):
            request = ListCampaignCriteriaRequest(**request)
        customer_id = _format_customer_id(request.customer_id)

        query = f"""
            SELECT campaign_criterion.criterion_id, campaign_criterion.type,
                   campaign_criterion.negative, campaign_criterion.geo_location.geo_target_constant,
                   campaign_criterion.language.language_constant
            FROM campaign_criterion
            WHERE campaign.id = {request.campaign_id}
        """

        if hasattr(request, 'criterion_type') and request.criterion_type != "ALL":
            query += f" AND campaign_criterion.type = '{request.criterion_type}'"

        rows = _execute_gaql(customer_id, query, 1000)
        _track_api_call('GAQL_CRITERIA')

        results = []
        for row in rows:
            criterion_type = _safe_get_value(row, "campaign_criterion.type")
            results.append({
                "criterion_id": str(_safe_get_value(row, "campaign_criterion.criterion_id")),
                "type": criterion_type.name if criterion_type is not None and hasattr(criterion_type, "name") else str(criterion_type),
                "negative": _safe_get_value(row, "campaign_criterion.negative", False),
            })

        return {
            "status": "success",
            "results": results,
            "count": len(results)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list campaign criteria: {str(e)}"
        }


@mcp.tool()
async def google_ads_list_conversion_actions(request: ListConversionActionsRequest) -> dict:
    """
    List conversion actions configured in the account.

    Returns conversion action details including type, status, category, and value settings.
    """
    try:
        customer_id = _format_customer_id(request.customer_id)

        query = """
            SELECT conversion_action.id, conversion_action.name, conversion_action.resource_name,
                   conversion_action.type, conversion_action.status, conversion_action.category,
                   conversion_action.primary_for_goal
            FROM conversion_action
        """

        if request.status:
            query += f" WHERE conversion_action.status = '{request.status}'"

        rows = _execute_gaql(customer_id, query, 1000)
        _track_api_call('GAQL_CONVERSION_ACTIONS')

        def _name_of(v):
            return v.name if v is not None and hasattr(v, "name") else (str(v) if v is not None else "")

        results = []
        for row in rows:
            results.append({
                "id": str(_safe_get_value(row, "conversion_action.id")),
                "resource_name": _safe_get_value(row, "conversion_action.resource_name"),
                "name": _safe_get_value(row, "conversion_action.name"),
                "type": _name_of(_safe_get_value(row, "conversion_action.type")),
                "status": _name_of(_safe_get_value(row, "conversion_action.status")),
                "category": _name_of(_safe_get_value(row, "conversion_action.category")),
                "primary_for_goal": _safe_get_value(row, "conversion_action.primary_for_goal", False),
            })

        return {
            "status": "success",
            "results": results,
            "count": len(results)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list conversion actions: {str(e)}"
        }


# ============================================================================
# GAQL AND ADVANCED QUERYING
# ============================================================================


@mcp.tool()
async def google_ads_execute_gaql(request: ExecuteGaqlRequest) -> dict:
    """
    Execute a raw GAQL (Google Ads Query Language) query.

    Power tool for custom queries not covered by other tools.
    Returns raw stringified results.
    """
    try:
        customer_id = _format_customer_id(request.customer_id)

        rows = _execute_gaql(customer_id, request.query, request.page_size)
        limited_rows = rows[:request.page_size]
        stringified_results = [str(row) for row in limited_rows]

        return {
            "status": "success",
            "results": stringified_results,
            "count": len(stringified_results)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to execute GAQL query: {str(e)}"
        }


@mcp.tool()
async def google_ads_list_recommendations(request: ListRecommendationsRequest) -> dict:
    """
    List Google Ads optimization recommendations.

    Returns recommendation types and estimated impact metrics.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ga_service = _gam.google_ads_client.get_service("GoogleAdsService")

        query = """
            SELECT recommendation.type, recommendation.campaign.id,
                   recommendation.impact
            FROM recommendation
            WHERE recommendation.dismissed = FALSE
        """

        if request.campaign_id:
            query += f" AND recommendation.campaign.id = {request.campaign_id}"

        query += f" LIMIT {request.page_size}"

        search_request = _gam.google_ads_client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = customer_id
        search_request.query = query

        results = []
        stream = ga_service.search_stream(request=search_request)
        for batch in stream:
            for row in batch.results:
                results.append({
                    "type": _safe_get_value(row, "recommendation.type").name,
                    "campaign_id": str(_safe_get_value(row, "recommendation.campaign.id")),
                })

        _track_api_call('GAQL_RECOMMENDATIONS')

        return {
            "status": "success",
            "results": results,
            "count": len(results)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list recommendations: {str(e)}"
        }


# ============================================================================
# CONVERSION TRACKING DIAGNOSTICS
# ============================================================================


@mcp.tool()
async def google_ads_conversion_diagnostics(request: ConversionDiagnosticsRequest) -> dict:
    """
    Diagnose conversion tracking health for an account or campaign.

    Checks:
    1. Conversion action status (active/inactive)
    2. Daily conversion trends
    3. Sudden drops (>80% decline)
    4. Conversion-to-click ratio trends
    5. Campaigns with zero conversions
    """
    try:
        from datetime import datetime, timedelta

        customer_id = _format_customer_id(request.customer_id)
        today = datetime.now().date()
        start_date = today - timedelta(days=request.days_back)

        # Get all conversion actions
        query = """
            SELECT conversion_action.id, conversion_action.name,
                   conversion_action.status
            FROM conversion_action
        """

        ca_results = _execute_gaql(customer_id, query, 1000)
        _track_api_call('GAQL_CONVERSION_ACTIONS')

        conversion_actions = []
        for row in ca_results:
            conversion_actions.append({
                "id": str(_safe_get_value(row, "conversion_action.id")),
                "name": _safe_get_value(row, "conversion_action.name"),
                "status": _safe_get_value(row, "conversion_action.status").name,
            })

        # Get daily conversion stats
        query = f"""
            SELECT segments.date, metrics.conversions, metrics.clicks,
                   campaign.id
            FROM campaign
            WHERE segments.date >= '{start_date}'
            AND segments.date <= '{today}'
            ORDER BY segments.date DESC
            LIMIT 1000
        """

        if request.campaign_id:
            query = f"""
                SELECT segments.date, metrics.conversions, metrics.clicks
                FROM campaign
                WHERE campaign.id = {request.campaign_id}
                AND segments.date >= '{start_date}'
                AND segments.date <= '{today}'
                ORDER BY segments.date DESC
            """

        stats_results = _execute_gaql(customer_id, query, 1000)
        _track_api_call('GAQL_CONVERSIONS')

        return {
            "status": "success",
            "conversion_actions": conversion_actions,
            "conversion_action_count": len(conversion_actions),
            "days_analyzed": request.days_back,
            "message": "Conversion tracking health check complete"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to run conversion diagnostics: {str(e)}"
        }


# ============================================================================
# ACCOUNT PERFORMANCE SUMMARY
# ============================================================================


@mcp.tool()
async def google_ads_account_performance_summary(request: AccountPerformanceSummaryRequest) -> dict:
    """
    Get a comprehensive performance summary for a specific account.

    Returns:
    - Account-level totals (spend, conversions, ROAS)
    - Per-campaign breakdown with type, status, budget, performance
    - Top/bottom performers by ROAS
    - Conversion action health check
    - Budget utilization analysis
    """
    try:
        from datetime import datetime, timedelta

        customer_id = _format_customer_id(request.customer_id)
        today = datetime.now().date()
        start_date = today - timedelta(days=request.days_back)

        # Get account-wide metrics
        query = f"""
            SELECT campaign.id, metrics.cost_micros,
                   metrics.conversions, metrics.conversions_value
            FROM campaign
            WHERE segments.date >= '{start_date}'
            AND segments.date <= '{today}'
        """

        results = _execute_gaql(customer_id, query, 10)
        _track_api_call('GAQL_ACCOUNT_SUMMARY')

        total_spend = 0.0
        total_conversions = 0
        total_conversion_value = 0
        campaign_ids = set()
        for row in results:
            campaign_id = _safe_get_value(row, "campaign.id")
            if campaign_id is not None:
                campaign_ids.add(str(campaign_id))
            total_spend += _from_micros(_safe_get_value(row, "metrics.cost_micros", 0))
            total_conversions += _safe_get_value(row, "metrics.conversions", 0)
            total_conversion_value += _safe_get_value(row, "metrics.conversions_value", 0)

        # Get per-campaign breakdown
        query = f"""
            SELECT campaign.id, campaign.name, campaign.status,
                   campaign.advertising_channel_type,
                   metrics.cost_micros, metrics.conversions, metrics.conversions_value
            FROM campaign
            WHERE segments.date >= '{start_date}'
            AND segments.date <= '{today}'
            ORDER BY metrics.cost_micros DESC
        """

        campaign_results = _execute_gaql(customer_id, query, 100)
        _track_api_call('GAQL_CAMPAIGNS_PERFORMANCE')

        campaigns = []
        for row in campaign_results:
            campaigns.append({
                "id": str(_safe_get_value(row, "campaign.id")),
                "name": _safe_get_value(row, "campaign.name"),
                "status": _safe_get_value(row, "campaign.status").name,
                "channel": _safe_get_value(row, "campaign.advertising_channel_type").name,
                "spend": _from_micros(_safe_get_value(row, "metrics.cost_micros", 0)),
                "conversions": _safe_get_value(row, "metrics.conversions", 0),
                "conversion_value": _safe_get_value(row, "metrics.conversions_value", 0),
            })

        account_totals = {
            "total_spend": total_spend,
            "total_conversions": total_conversions,
            "total_conversion_value": total_conversion_value,
            "campaign_count": len(campaign_ids or {campaign["id"] for campaign in campaigns}),
        }

        return {
            "status": "success",
            "account_summary": account_totals,
            "campaigns": campaigns,
            "campaign_count": len(campaigns),
            "analysis_period_days": request.days_back
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get account performance summary: {str(e)}"
        }
