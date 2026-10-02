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

        # FIX (§9, 2026-05-31): a bare negative UNIT has no parent SUBDIVISION → Google rejects with
        # field_error REQUIRED 'parent_ad_group_criterion'. Negative product partitions MUST hang off a
        # SUBDIVISION root. So we ATOMICALLY rebuild the tree: remove existing listing-group criteria, then
        # create root SUBDIVISION + one negative UNIT per excluded item_id + one positive everything-else UNIT.
        # Works under Smart Bidding (TARGET_ROAS) where bid-to-min does nothing — negative = hard exclude.
        def _crit_path(temp_id):
            return f"customers/{customer_id}/adGroupCriteria/{ad_group_id}~{temp_id}"

        existing_q = (
            f"SELECT ad_group_criterion.criterion_id, ad_group_criterion.listing_group.type "
            f"FROM ad_group_criterion WHERE ad_group.id = {ad_group_id} "
            f"AND ad_group_criterion.type = 'LISTING_GROUP' AND ad_group_criterion.status != 'REMOVED'"
        )
        existing = _execute_gaql(customer_id, existing_q)
        operations = []
        # remove existing UNITs before SUBDIVISIONs (children before parents)
        ex = sorted(existing, key=lambda r: 0 if _safe_get_value(r, "ad_group_criterion.listing_group.type") == "UNIT" else 1)
        for r in ex:
            cid_ = _safe_get_value(r, "ad_group_criterion.criterion_id")
            op = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
            op.remove = f"customers/{customer_id}/adGroupCriteria/{ad_group_id}~{cid_}"
            operations.append(op)

        # root SUBDIVISION (temp -1)
        op_root = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
        cr = op_root.create
        cr.ad_group = ad_group_criterion_service.ad_group_path(customer_id, ad_group_id)
        cr.status = _gam.google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
        cr.listing_group.type_ = _gam.google_ads_client.enums.ListingGroupTypeEnum.SUBDIVISION
        cr.resource_name = _crit_path(-1)
        operations.append(op_root)

        temp = -1
        for item_id in request.item_ids:
            temp -= 1
            op = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
            c = op.create
            c.ad_group = ad_group_criterion_service.ad_group_path(customer_id, ad_group_id)
            c.status = _gam.google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
            c.negative = True
            c.listing_group.type_ = _gam.google_ads_client.enums.ListingGroupTypeEnum.UNIT
            c.listing_group.parent_ad_group_criterion = _crit_path(-1)
            c.listing_group.case_value.product_item_id.value = item_id
            c.resource_name = _crit_path(temp)
            operations.append(op)

        # everything-else UNIT (positive, empty case_value)
        temp -= 1
        op_else = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
        c = op_else.create
        c.ad_group = ad_group_criterion_service.ad_group_path(customer_id, ad_group_id)
        c.status = _gam.google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
        c.listing_group.type_ = _gam.google_ads_client.enums.ListingGroupTypeEnum.UNIT
        c.listing_group.parent_ad_group_criterion = _crit_path(-1)
        c.listing_group.case_value.product_item_id._pb.SetInParent()
        c.cpc_bid_micros = 10000
        c.resource_name = _crit_path(temp)
        operations.append(op_else)

        result = ad_group_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id,
            operations=operations
        )
        _track_api_call('MUTATE_CRITERION', len(operations))

        return {
            "status": "success",
            "message": f"Excluded {len(request.item_ids)} products from campaign {request.campaign_id} (atomic tree rebuild)",
            "count": len(result.results),
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to add product exclusions: {str(e)}"
        }
