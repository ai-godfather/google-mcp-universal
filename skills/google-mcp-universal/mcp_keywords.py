"""
Google Ads Keywords Module
Handles keyword management including adding keywords and negative keywords to campaigns.
"""

import google_ads_mcp as _gam

from google_ads_mcp import (
    mcp,
    _ensure_client,
    _format_customer_id,
    _execute_gaql,
    _safe_get_value,
    _track_api_call,
    AddKeywordRequest,
    AddProductExclusionRequest,
    PMaxNegativeKeywordRequest,
)


@mcp.tool()
async def google_ads_add_keyword(request: AddKeywordRequest) -> dict:
    """
    Add a positive keyword to an ad group.

    Supports BROAD, PHRASE, and EXACT match types.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ad_group_criterion_service = _gam.google_ads_client.get_service("AdGroupCriterionService")

        operation = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
        criterion = operation.create
        criterion.ad_group = ad_group_criterion_service.ad_group_path(customer_id, request.ad_group_id)
        criterion.keyword.text = request.keyword_text

        match_type_map = {
            "BROAD": _gam.google_ads_client.enums.KeywordMatchTypeEnum.BROAD,
            "PHRASE": _gam.google_ads_client.enums.KeywordMatchTypeEnum.PHRASE,
            "EXACT": _gam.google_ads_client.enums.KeywordMatchTypeEnum.EXACT
        }
        criterion.keyword.match_type = match_type_map[request.match_type]

        if request.cpc_bid_micros is not None:
            criterion.cpc_bid_micros = request.cpc_bid_micros

        result = ad_group_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_CRITERION')

        return {
            "status": "success",
            "message": f"Keyword '{request.keyword_text}' ({request.match_type}) added to ad group {request.ad_group_id}",
            "criterion_resource_name": result.results[0].resource_name
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to add keyword: {str(e)}"
        }


@mcp.tool()
async def google_ads_pmax_negative_keywords(request: PMaxNegativeKeywordRequest) -> dict:
    """
    Add negative keywords to a Performance Max campaign via campaign-level negative keyword lists.

    PMax does NOT support traditional ad-group-level negatives. Instead, use:
    1. Campaign-level negative keyword LISTS (shared lists) — this tool creates one
    2. Account-level brand exclusions (separate mechanism)

    This creates a shared negative keyword list and attaches it to the PMax campaign.
    Note: This requires the account to have the PMax negative keywords feature enabled
    (rolled out to all accounts since mid-2024).
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)

        # Step 1: Create a shared negative keyword list
        shared_set_service = _gam.google_ads_client.get_service("SharedSetService")
        shared_set_operation = _gam.google_ads_client.get_type("SharedSetOperation")
        shared_set = shared_set_operation.create
        shared_set.name = f"PMax Negatives - Campaign {request.campaign_id}"
        shared_set.type_ = _gam.google_ads_client.enums.SharedSetTypeEnum.NEGATIVE_KEYWORDS

        set_result = shared_set_service.mutate_shared_sets(
            customer_id=customer_id,
            operations=[shared_set_operation]
        )
        _track_api_call('MUTATE_SHARED_SET')
        shared_set_rn = set_result.results[0].resource_name

        # Step 2: Add keywords to the shared set
        shared_criterion_service = _gam.google_ads_client.get_service("SharedCriterionService")
        kw_operations = []
        match_type_enum = _gam.google_ads_client.enums.KeywordMatchTypeEnum
        mt = match_type_enum.EXACT if request.match_type.upper() == "EXACT" else match_type_enum.PHRASE

        for kw in request.keywords:
            op = _gam.google_ads_client.get_type("SharedCriterionOperation")
            criterion = op.create
            criterion.shared_set = shared_set_rn
            criterion.keyword.text = kw
            criterion.keyword.match_type = mt
            kw_operations.append(op)

        if kw_operations:
            shared_criterion_service.mutate_shared_criteria(
                customer_id=customer_id,
                operations=kw_operations
            )
            _track_api_call('MUTATE_SHARED_CRITERION', len(kw_operations))

        # Step 3: Attach shared set to campaign
        campaign_shared_set_service = _gam.google_ads_client.get_service("CampaignSharedSetService")
        link_op = _gam.google_ads_client.get_type("CampaignSharedSetOperation")
        link = link_op.create
        link.campaign = _gam.google_ads_client.get_service("CampaignService").campaign_path(
            customer_id, request.campaign_id
        )
        link.shared_set = shared_set_rn

        campaign_shared_set_service.mutate_campaign_shared_sets(
            customer_id=customer_id,
            operations=[link_op]
        )
        _track_api_call('MUTATE_CAMPAIGN_SHARED_SET')

        return {
            "status": "success",
            "message": f"Added {len(request.keywords)} negative keywords to PMax campaign {request.campaign_id}",
            "campaign_id": request.campaign_id,
            "shared_set": shared_set_rn,
            "keywords_added": request.keywords,
            "match_type": request.match_type,
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to add PMax negative keywords: {str(e)}"}


@mcp.tool()
async def google_ads_add_product_exclusion(request: AddProductExclusionRequest) -> dict:
    """
    Exclude specific products from a Shopping campaign by item ID.

    Automatically finds the ad group and creates negative product partitions.
    Essential for image A/B testing — exclude losing image variants.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ad_group_criterion_service = _gam.google_ads_client.get_service("AdGroupCriterionService")

        # Get default ad group for the campaign
        query = f"""
            SELECT campaign.id, ad_group.id
            FROM ad_group
            WHERE campaign.id = {request.campaign_id}
            LIMIT 1
        """
        results = _execute_gaql(customer_id, query)

        if not results:
            return {
                "status": "error",
                "message": f"No ad group found for campaign {request.campaign_id}"
            }

        ad_group_id = str(_safe_get_value(results[0], "ad_group.id"))

        operations = []
        for item_id in request.item_ids:
            operation = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
            criterion = operation.create
            criterion.ad_group = ad_group_criterion_service.ad_group_path(customer_id, ad_group_id)
            criterion.negative = True
            criterion.listing_group.type_ = _gam.google_ads_client.enums.ListingGroupTypeEnum.UNIT
            criterion.listing_group.case_value.product_item_id.value = item_id
            operations.append(operation)

        result = ad_group_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id,
            operations=operations
        )
        _track_api_call('MUTATE_CRITERION', len(operations))

        return {
            "status": "success",
            "message": f"Excluded {len(request.item_ids)} products from campaign {request.campaign_id}",
            "count": len(result.results)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to add product exclusions: {str(e)}"
        }
