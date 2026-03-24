"""
Google Ads Campaign Management Module

Campaign CRUD operations: list, get details, update status, update budget, delete
"""

import google_ads_mcp as _gam

from google_ads_mcp import (
    mcp,
    _format_customer_id, _from_micros, _ensure_client, _execute_gaql, _safe_get_value,
    _track_api_call,
    CampaignListRequest, CampaignDetailsRequest, CampaignStatusUpdateRequest,
    CampaignBudgetUpdateRequest, AdGroupListRequest
)


@mcp.tool()
async def google_ads_list_campaigns(request: CampaignListRequest) -> dict:
    """
    List all campaigns with status, type, budget, and key metrics.

    Supports filtering by campaign type and status.
    """
    try:
        customer_id = _format_customer_id(request.customer_id)

        # Build GAQL query
        query = """
            SELECT
                campaign.id,
                campaign.name,
                campaign.status,
                campaign.advertising_channel_type,
                campaign.advertising_channel_sub_type,
                campaign_budget.amount_micros,
                metrics.impressions,
                metrics.clicks,
                metrics.conversions,
                metrics.cost_micros
            FROM campaign
            WHERE campaign.status != REMOVED
        """

        # Add filters if specified
        if request.campaign_type:
            type_map = {
                "SEARCH": "SEARCH",
                "SHOPPING": "SHOPPING",
                "PERFORMANCE_MAX": "PERFORMANCE_MAX"
            }
            query += f" AND campaign.advertising_channel_type = {type_map.get(request.campaign_type)}"

        if request.status:
            query += f" AND campaign.status = {request.status}"

        query += " LIMIT " + str(request.page_size)

        results = _execute_gaql(customer_id, query, request.page_size)

        campaigns = []
        for row in results:
            campaign_data = {
                "id": str(_safe_get_value(row, "campaign.id")),
                "name": _safe_get_value(row, "campaign.name"),
                "status": _safe_get_value(row, "campaign.status"),
                "type": _safe_get_value(row, "campaign.advertising_channel_type"),
                "sub_type": _safe_get_value(row, "campaign.advertising_channel_sub_type"),
                "daily_budget": _from_micros(_safe_get_value(row, "campaign_budget.amount_micros", 0)),
                "metrics": {
                    "impressions": _safe_get_value(row, "metrics.impressions", 0),
                    "clicks": _safe_get_value(row, "metrics.clicks", 0),
                    "conversions": _safe_get_value(row, "metrics.conversions", 0),
                    "cost": _from_micros(_safe_get_value(row, "metrics.cost_micros", 0))
                }
            }
            campaigns.append(campaign_data)

        return {
            "status": "success",
            "campaigns": campaigns,
            "count": len(campaigns)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@mcp.tool()
async def google_ads_get_campaign_details(request: CampaignDetailsRequest) -> dict:
    """
    Get detailed information about a specific campaign by ID or name.
    """
    try:
        customer_id = _format_customer_id(request.customer_id)

        # Build query based on whether we're searching by ID or name
        if request.campaign_id:
            query = f"""
                SELECT
                    campaign.id,
                    campaign.name,
                    campaign.status,
                    campaign.advertising_channel_type,
                    campaign.advertising_channel_sub_type,
                    campaign.start_date,
                    campaign.end_date,
                    campaign_budget.amount_micros,
                    campaign_budget.period,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.conversions,
                    metrics.cost_micros,
                    metrics.average_cpc,
                    metrics.ctr,
                    metrics.conversion_rate
                FROM campaign
                WHERE campaign.id = {request.campaign_id}
            """
        else:
            query = f"""
                SELECT
                    campaign.id,
                    campaign.name,
                    campaign.status,
                    campaign.advertising_channel_type,
                    campaign.advertising_channel_sub_type,
                    campaign.start_date,
                    campaign.end_date,
                    campaign_budget.amount_micros,
                    campaign_budget.period,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.conversions,
                    metrics.cost_micros,
                    metrics.average_cpc,
                    metrics.ctr,
                    metrics.conversion_rate
                FROM campaign
                WHERE campaign.name = '{request.campaign_name}'
            """

        results = _execute_gaql(customer_id, query)

        if not results:
            return {
                "status": "error",
                "message": "Campaign not found"
            }

        row = results[0]
        campaign_data = {
            "id": str(_safe_get_value(row, "campaign.id")),
            "name": _safe_get_value(row, "campaign.name"),
            "status": _safe_get_value(row, "campaign.status"),
            "type": _safe_get_value(row, "campaign.advertising_channel_type"),
            "sub_type": _safe_get_value(row, "campaign.advertising_channel_sub_type"),
            "start_date": _safe_get_value(row, "campaign.start_date"),
            "end_date": _safe_get_value(row, "campaign.end_date"),
            "budget": {
                "daily_amount": _from_micros(_safe_get_value(row, "campaign_budget.amount_micros", 0)),
                "period": _safe_get_value(row, "campaign_budget.period")
            },
            "metrics": {
                "impressions": _safe_get_value(row, "metrics.impressions", 0),
                "clicks": _safe_get_value(row, "metrics.clicks", 0),
                "conversions": _safe_get_value(row, "metrics.conversions", 0),
                "cost": _from_micros(_safe_get_value(row, "metrics.cost_micros", 0)),
                "average_cpc": _from_micros(_safe_get_value(row, "metrics.average_cpc", 0)),
                "ctr_percent": _safe_get_value(row, "metrics.ctr", 0),
                "conversion_rate": _safe_get_value(row, "metrics.conversion_rate", 0)
            }
        }

        return {
            "status": "success",
            "campaign": campaign_data
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@mcp.tool()
async def google_ads_list_ad_groups(request: AdGroupListRequest) -> dict:
    """
    List ad groups for a campaign with metrics.
    """
    try:
        customer_id = _format_customer_id(request.customer_id)

        query = f"""
            SELECT
                campaign.id,
                ad_group.id,
                ad_group.name,
                ad_group.status,
                ad_group.type,
                metrics.impressions,
                metrics.clicks,
                metrics.conversions,
                metrics.cost_micros,
                metrics.average_cpc,
                metrics.ctr
            FROM ad_group
            WHERE campaign.id = {request.campaign_id}
            AND ad_group.status != REMOVED
            LIMIT {request.page_size}
        """

        results = _execute_gaql(customer_id, query, request.page_size)

        ad_groups = []
        for row in results:
            ag_data = {
                "id": str(_safe_get_value(row, "ad_group.id")),
                "name": _safe_get_value(row, "ad_group.name"),
                "status": _safe_get_value(row, "ad_group.status"),
                "type": _safe_get_value(row, "ad_group.type"),
                "metrics": {
                    "impressions": _safe_get_value(row, "metrics.impressions", 0),
                    "clicks": _safe_get_value(row, "metrics.clicks", 0),
                    "conversions": _safe_get_value(row, "metrics.conversions", 0),
                    "cost": _from_micros(_safe_get_value(row, "metrics.cost_micros", 0)),
                    "average_cpc": _from_micros(_safe_get_value(row, "metrics.average_cpc", 0)),
                    "ctr_percent": _safe_get_value(row, "metrics.ctr", 0)
                }
            }
            ad_groups.append(ag_data)

        return {
            "status": "success",
            "ad_groups": ad_groups,
            "count": len(ad_groups)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@mcp.tool()
async def google_ads_list_keywords(request) -> dict:
    """
    List keywords with quality scores, bids, and metrics.
    Supports filtering by quality score and status.
    """
    from google_ads_mcp import KeywordListRequest

    try:
        customer_id = _format_customer_id(request.customer_id)

        query = """
            SELECT
                ad_group_criterion.criterion_id,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.status,
                ad_group.id,
                ad_group.name,
                ad_group_criterion.quality_info.quality_score,
                ad_group_criterion.bid_modifier,
                ad_group_criterion.cpc_bid_micros,
                metrics.impressions,
                metrics.clicks,
                metrics.conversions,
                metrics.cost_micros
            FROM ad_group_criterion
            WHERE ad_group_criterion.type = KEYWORD
            AND ad_group_criterion.status != REMOVED
        """

        if request.campaign_id:
            query += f" AND campaign.id = {request.campaign_id}"

        if request.min_quality_score:
            query += f" AND ad_group_criterion.quality_info.quality_score >= {request.min_quality_score}"

        if request.status:
            query += f" AND ad_group_criterion.status = {request.status}"

        query += f" LIMIT {request.page_size}"

        results = _execute_gaql(customer_id, query, request.page_size)

        keywords = []
        for row in results:
            kw_data = {
                "criterion_id": str(_safe_get_value(row, "ad_group_criterion.criterion_id")),
                "text": _safe_get_value(row, "ad_group_criterion.keyword.text"),
                "match_type": _safe_get_value(row, "ad_group_criterion.keyword.match_type"),
                "status": _safe_get_value(row, "ad_group_criterion.status"),
                "ad_group": {
                    "id": str(_safe_get_value(row, "ad_group.id")),
                    "name": _safe_get_value(row, "ad_group.name")
                },
                "quality_score": _safe_get_value(row, "ad_group_criterion.quality_info.quality_score", 0),
                "bid_modifier": _safe_get_value(row, "ad_group_criterion.bid_modifier", 1.0),
                "max_cpc": _from_micros(_safe_get_value(row, "ad_group_criterion.cpc_bid_micros", 0)),
                "metrics": {
                    "impressions": _safe_get_value(row, "metrics.impressions", 0),
                    "clicks": _safe_get_value(row, "metrics.clicks", 0),
                    "conversions": _safe_get_value(row, "metrics.conversions", 0),
                    "cost": _from_micros(_safe_get_value(row, "metrics.cost_micros", 0))
                }
            }
            keywords.append(kw_data)

        return {
            "status": "success",
            "keywords": keywords,
            "count": len(keywords)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@mcp.tool()
async def google_ads_get_performance_report(request) -> dict:
    """
    Get performance report for campaigns, ad groups, or keywords over a date range.
    """
    from google_ads_mcp import PerformanceReportRequest
    from datetime import datetime, timedelta

    try:
        customer_id = _format_customer_id(request.customer_id)

        # Determine the entity to report on
        if request.entity_type == "campaign":
            entity_fields = """
                campaign.id,
                campaign.name,
                campaign.status
            """
            from_clause = "campaign"
            group_clause = "campaign.id, campaign.name, campaign.status"
        elif request.entity_type == "ad_group":
            entity_fields = """
                ad_group.id,
                ad_group.name,
                campaign.id,
                campaign.name
            """
            from_clause = "ad_group"
            group_clause = "ad_group.id, ad_group.name, campaign.id, campaign.name"
        else:  # keyword
            entity_fields = """
                ad_group_criterion.criterion_id,
                ad_group_criterion.keyword.text,
                ad_group.id,
                ad_group.name,
                campaign.id
            """
            from_clause = "ad_group_criterion"
            group_clause = "ad_group_criterion.criterion_id, ad_group_criterion.keyword.text, ad_group.id, ad_group.name, campaign.id"

        query = f"""
            SELECT
                {entity_fields},
                metrics.impressions,
                metrics.clicks,
                metrics.conversions,
                metrics.cost_micros,
                metrics.average_cpc,
                metrics.ctr,
                metrics.conversion_rate
            FROM {from_clause}
            WHERE segments.date >= '{request.start_date}'
            AND segments.date <= '{request.end_date}'
        """

        if request.campaign_id:
            query += f" AND campaign.id = {request.campaign_id}"

        query += f" GROUP BY {group_clause}"
        query += f" LIMIT {request.page_size}"

        results = _execute_gaql(customer_id, query, request.page_size)

        report_data = []
        for row in results:
            entry = {
                "metrics": {
                    "impressions": _safe_get_value(row, "metrics.impressions", 0),
                    "clicks": _safe_get_value(row, "metrics.clicks", 0),
                    "conversions": _safe_get_value(row, "metrics.conversions", 0),
                    "cost": _from_micros(_safe_get_value(row, "metrics.cost_micros", 0)),
                    "average_cpc": _from_micros(_safe_get_value(row, "metrics.average_cpc", 0)),
                    "ctr_percent": _safe_get_value(row, "metrics.ctr", 0),
                    "conversion_rate": _safe_get_value(row, "metrics.conversion_rate", 0)
                }
            }

            # Add entity-specific fields
            if request.entity_type == "campaign":
                entry["campaign_id"] = str(_safe_get_value(row, "campaign.id"))
                entry["campaign_name"] = _safe_get_value(row, "campaign.name")
                entry["campaign_status"] = _safe_get_value(row, "campaign.status")
            elif request.entity_type == "ad_group":
                entry["ad_group_id"] = str(_safe_get_value(row, "ad_group.id"))
                entry["ad_group_name"] = _safe_get_value(row, "ad_group.name")
                entry["campaign_id"] = str(_safe_get_value(row, "campaign.id"))
                entry["campaign_name"] = _safe_get_value(row, "campaign.name")
            else:  # keyword
                entry["keyword_id"] = str(_safe_get_value(row, "ad_group_criterion.criterion_id"))
                entry["keyword_text"] = _safe_get_value(row, "ad_group_criterion.keyword.text")
                entry["ad_group_id"] = str(_safe_get_value(row, "ad_group.id"))
                entry["ad_group_name"] = _safe_get_value(row, "ad_group.name")
                entry["campaign_id"] = str(_safe_get_value(row, "campaign.id"))

            report_data.append(entry)

        return {
            "status": "success",
            "entity_type": request.entity_type,
            "date_range": {
                "start": request.start_date,
                "end": request.end_date
            },
            "data": report_data,
            "count": len(report_data)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@mcp.tool()
async def google_ads_list_assets(request) -> dict:
    """
    List assets (images, headlines, descriptions) for campaigns or asset groups.
    """
    from google_ads_mcp import AssetListRequest

    try:
        customer_id = _format_customer_id(request.customer_id)

        query = """
            SELECT
                asset.id,
                asset.name,
                asset.type,
                asset.text_asset.text,
                asset.image_asset.full_size.url,
                asset.image_asset.full_size.width_pixels,
                asset.image_asset.full_size.height_pixels
            FROM asset
            WHERE asset.status != REMOVED
        """

        if request.asset_type:
            query += f" AND asset.type = {request.asset_type}"

        query += f" LIMIT {request.page_size}"

        results = _execute_gaql(customer_id, query, request.page_size)

        assets = []
        for row in results:
            asset_data = {
                "id": str(_safe_get_value(row, "asset.id")),
                "name": _safe_get_value(row, "asset.name"),
                "type": _safe_get_value(row, "asset.type"),
                "text": _safe_get_value(row, "asset.text_asset.text"),
                "image": {
                    "url": _safe_get_value(row, "asset.image_asset.full_size.url"),
                    "width": _safe_get_value(row, "asset.image_asset.full_size.width_pixels"),
                    "height": _safe_get_value(row, "asset.image_asset.full_size.height_pixels")
                } if _safe_get_value(row, "asset.image_asset.full_size.url") else None
            }
            assets.append(asset_data)

        return {
            "status": "success",
            "assets": assets,
            "count": len(assets)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@mcp.tool()
async def google_ads_list_search_terms(request) -> dict:
    """
    Get search terms report showing actual search queries triggering ads.
    """
    from google_ads_mcp import SearchTermsReportRequest

    try:
        customer_id = _format_customer_id(request.customer_id)

        query = f"""
            SELECT
                search_term_view.search_term,
                ad_group.id,
                ad_group.name,
                campaign.id,
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.conversions,
                metrics.cost_micros,
                metrics.ctr
            FROM search_term_view
            WHERE segments.date BETWEEN '{request.start_date}' AND '{request.end_date}'
            AND metrics.impressions >= {request.min_impressions}
        """

        if request.campaign_id:
            query += f" AND campaign.id = {request.campaign_id}"

        query += " ORDER BY metrics.impressions DESC"
        query += f" LIMIT {request.page_size}"

        results = _execute_gaql(customer_id, query, request.page_size)

        search_terms = []
        for row in results:
            term_data = {
                "search_term": _safe_get_value(row, "search_term_view.search_term"),
                "ad_group": {
                    "id": str(_safe_get_value(row, "ad_group.id")),
                    "name": _safe_get_value(row, "ad_group.name")
                },
                "campaign": {
                    "id": str(_safe_get_value(row, "campaign.id")),
                    "name": _safe_get_value(row, "campaign.name")
                },
                "metrics": {
                    "impressions": _safe_get_value(row, "metrics.impressions", 0),
                    "clicks": _safe_get_value(row, "metrics.clicks", 0),
                    "conversions": _safe_get_value(row, "metrics.conversions", 0),
                    "cost": _from_micros(_safe_get_value(row, "metrics.cost_micros", 0)),
                    "ctr_percent": _safe_get_value(row, "metrics.ctr", 0)
                }
            }
            search_terms.append(term_data)

        return {
            "status": "success",
            "search_terms": search_terms,
            "count": len(search_terms)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@mcp.tool()
async def google_ads_get_account_summary(request) -> dict:
    """
    Get account-level summary metrics for a given date range.
    """
    from google_ads_mcp import AccountSummaryRequest
    from datetime import datetime, timedelta

    try:
        customer_id = _format_customer_id(request.customer_id)

        # Calculate date range
        today = datetime.now().date()
        if request.date_range == "TODAY":
            start_date = today
        elif request.date_range == "LAST_7_DAYS":
            start_date = today - timedelta(days=7)
        elif request.date_range == "LAST_30_DAYS":
            start_date = today - timedelta(days=30)
        else:  # LAST_90_DAYS
            start_date = today - timedelta(days=90)

        query = f"""
            SELECT
                metrics.impressions,
                metrics.clicks,
                metrics.conversions,
                metrics.cost_micros,
                metrics.average_cpc,
                metrics.ctr,
                metrics.conversion_rate,
                metrics.average_position
            FROM customer
            WHERE segments.date >= '{start_date}'
            AND segments.date <= '{today}'
        """

        results = _execute_gaql(customer_id, query)

        if not results:
            return {
                "status": "success",
                "account_summary": {
                    "customer_id": request.customer_id,
                    "date_range": request.date_range,
                    "metrics": {
                        "impressions": 0,
                        "clicks": 0,
                        "conversions": 0,
                        "cost": 0,
                        "average_cpc": 0,
                        "ctr_percent": 0,
                        "conversion_rate": 0,
                        "average_position": 0
                    }
                }
            }

        # Aggregate metrics
        total_impressions = 0
        total_clicks = 0
        total_conversions = 0
        total_cost = 0

        for row in results:
            total_impressions += _safe_get_value(row, "metrics.impressions", 0)
            total_clicks += _safe_get_value(row, "metrics.clicks", 0)
            total_conversions += _safe_get_value(row, "metrics.conversions", 0)
            total_cost += _safe_get_value(row, "metrics.cost_micros", 0)

        # Calculate averages
        avg_cpc = _from_micros(total_cost) / total_clicks if total_clicks > 0 else 0
        ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        conversion_rate = (total_conversions / total_clicks * 100) if total_clicks > 0 else 0

        return {
            "status": "success",
            "account_summary": {
                "customer_id": request.customer_id,
                "date_range": request.date_range,
                "start_date": str(start_date),
                "end_date": str(today),
                "metrics": {
                    "impressions": total_impressions,
                    "clicks": total_clicks,
                    "conversions": total_conversions,
                    "cost": _from_micros(total_cost),
                    "average_cpc": avg_cpc,
                    "ctr_percent": round(ctr, 2),
                    "conversion_rate": round(conversion_rate, 2),
                    "average_position": _safe_get_value(results[0], "metrics.average_position", 0)
                }
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@mcp.tool()
async def google_ads_update_campaign_status(request: CampaignStatusUpdateRequest) -> dict:
    """
    Update campaign status (enable or pause).
    """
    from google.protobuf import field_mask_pb2

    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_service = _gam.google_ads_client.get_service("CampaignService")

        operation = _gam.google_ads_client.get_type("CampaignOperation")
        campaign = operation.update
        campaign.resource_name = campaign_service.campaign_path(customer_id, request.campaign_id)

        # Map status string to enum
        status_map = {
            "ENABLED": _gam.google_ads_client.enums.CampaignStatusEnum.ENABLED,
            "PAUSED": _gam.google_ads_client.enums.CampaignStatusEnum.PAUSED
        }
        campaign.status = status_map.get(request.status)

        fm = field_mask_pb2.FieldMask(paths=["status"])
        operation.update_mask = fm

        result = campaign_service.mutate_campaigns(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_CAMPAIGN')

        return {
            "status": "success",
            "message": f"Campaign {request.campaign_id} status updated to {request.status}",
            "result": str(result)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to update campaign status: {str(e)}"
        }


@mcp.tool()
async def google_ads_update_campaign_budget(request: CampaignBudgetUpdateRequest) -> dict:
    """
    Update campaign daily budget.
    """
    from google.protobuf import field_mask_pb2

    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_service = _gam.google_ads_client.get_service("CampaignService")

        # First get the campaign to get the budget resource name
        query = f"""
            SELECT
                campaign.id,
                campaign_budget.resource_name
            FROM campaign
            WHERE campaign.id = {request.campaign_id}
        """
        results = _execute_gaql(customer_id, query)

        if not results:
            return {
                "status": "error",
                "message": f"Campaign {request.campaign_id} not found"
            }

        budget_resource_name = _safe_get_value(results[0], "campaign_budget.resource_name")

        if not budget_resource_name:
            return {
                "status": "error",
                "message": "Could not retrieve budget resource name"
            }

        # Update the budget
        budget_service = _gam.google_ads_client.get_service("CampaignBudgetService")
        operation = _gam.google_ads_client.get_type("CampaignBudgetOperation")
        budget = operation.update
        budget.resource_name = budget_resource_name
        budget.amount_micros = request.daily_budget_micros

        fm = field_mask_pb2.FieldMask(paths=["amount_micros"])
        operation.update_mask = fm

        result = budget_service.mutate_campaign_budgets(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_BUDGET')

        return {
            "status": "success",
            "message": f"Campaign {request.campaign_id} budget updated to ${_from_micros(request.daily_budget_micros)}/day",
            "result": str(result)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to update campaign budget: {str(e)}"
        }


@mcp.tool()
async def google_ads_update_keyword_bid(request) -> dict:
    """
    Update max CPC bid for a keyword.
    """
    from google.protobuf import field_mask_pb2
    from google_ads_mcp import KeywordBidUpdateRequest

    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ad_group_criterion_service = _gam.google_ads_client.get_service("AdGroupCriterionService")

        operation = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
        criterion = operation.update
        criterion.resource_name = ad_group_criterion_service.ad_group_criterion_path(
            customer_id,
            request.ad_group_id,
            request.criterion_id
        )

        criterion.cpc_bid_micros = request.max_cpc_micros

        fm = field_mask_pb2.FieldMask(paths=["cpc_bid_micros"])
        operation.update_mask = fm

        result = ad_group_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_CRITERION')

        return {
            "status": "success",
            "message": f"Keyword bid updated to ${_from_micros(request.max_cpc_micros)}",
            "result": str(result)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to update keyword bid: {str(e)}"
        }


@mcp.tool()
async def google_ads_pause_keyword(request) -> dict:
    """
    Pause a keyword.
    """
    from google.protobuf import field_mask_pb2
    from google_ads_mcp import KeywordStatusUpdateRequest

    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ad_group_criterion_service = _gam.google_ads_client.get_service("AdGroupCriterionService")

        operation = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
        criterion = operation.update
        criterion.resource_name = ad_group_criterion_service.ad_group_criterion_path(
            customer_id,
            request.ad_group_id,
            request.criterion_id
        )

        status_enum = _gam.google_ads_client.enums.AdGroupCriterionStatusEnum.PAUSED
        criterion.status = status_enum

        fm = field_mask_pb2.FieldMask(paths=["status"])
        operation.update_mask = fm

        result = ad_group_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_CRITERION')

        return {
            "status": "success",
            "message": f"Keyword {request.criterion_id} paused",
            "result": str(result)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to pause keyword: {str(e)}"
        }


@mcp.tool()
async def google_ads_enable_keyword(request) -> dict:
    """
    Enable a paused keyword.
    """
    from google.protobuf import field_mask_pb2
    from google_ads_mcp import KeywordStatusUpdateRequest

    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ad_group_criterion_service = _gam.google_ads_client.get_service("AdGroupCriterionService")

        operation = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
        criterion = operation.update
        criterion.resource_name = ad_group_criterion_service.ad_group_criterion_path(
            customer_id,
            request.ad_group_id,
            request.criterion_id
        )

        status_enum = _gam.google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
        criterion.status = status_enum

        fm = field_mask_pb2.FieldMask(paths=["status"])
        operation.update_mask = fm

        result = ad_group_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_CRITERION')

        return {
            "status": "success",
            "message": f"Keyword {request.criterion_id} enabled",
            "result": str(result)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to enable keyword: {str(e)}"
        }


@mcp.tool()
async def google_ads_add_negative_keyword(request) -> dict:
    """
    Add a negative keyword to a campaign.
    """
    from google_ads_mcp import NegativeKeywordAddRequest

    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_criterion_service = _gam.google_ads_client.get_service("CampaignCriterionService")

        operation = _gam.google_ads_client.get_type("CampaignCriterionOperation")
        criterion = operation.create

        criterion.campaign = campaign_criterion_service.campaign_path(customer_id, request.campaign_id)

        # Set negative keyword
        criterion.keyword.text = request.keyword_text

        # Map match type
        match_type_map = {
            "BROAD": _gam.google_ads_client.enums.KeywordMatchTypeEnum.BROAD,
            "PHRASE": _gam.google_ads_client.enums.KeywordMatchTypeEnum.PHRASE,
            "EXACT": _gam.google_ads_client.enums.KeywordMatchTypeEnum.EXACT
        }
        criterion.keyword.match_type = match_type_map.get(request.match_type)

        # Mark as negative
        criterion.negative = True

        result = campaign_criterion_service.mutate_campaign_criteria(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_CRITERION')

        return {
            "status": "success",
            "message": f"Negative keyword '{request.keyword_text}' added to campaign {request.campaign_id}",
            "result": str(result)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to add negative keyword: {str(e)}"
        }


@mcp.tool()
async def google_ads_update_asset(request) -> dict:
    """
    Update text assets (headlines, descriptions) in asset groups.
    """
    from google.protobuf import field_mask_pb2
    from google_ads_mcp import AssetUpdateRequest

    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        asset_service = _gam.google_ads_client.get_service("AssetService")

        operation = _gam.google_ads_client.get_type("AssetOperation")
        asset = operation.update
        asset.resource_name = asset_service.asset_path(customer_id, request.asset_id)

        if request.text:
            asset.text_asset.text = request.text

        fm = field_mask_pb2.FieldMask(paths=["text_asset.text"])
        operation.update_mask = fm

        result = asset_service.mutate_assets(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_ASSET')

        return {
            "status": "success",
            "message": f"Asset {request.asset_id} updated successfully",
            "result": str(result)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to update asset: {str(e)}"
        }
