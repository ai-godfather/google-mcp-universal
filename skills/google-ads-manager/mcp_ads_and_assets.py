"""
Google Ads Ads and Assets Module
Handles ad creation, asset management, and link operations for campaigns and ad groups.
"""

import urllib.request

import google_ads_mcp as _gam
from google.ads.googleads.errors import GoogleAdsException as _GoogleAdsException

from google_ads_mcp import (
    mcp,
    _ssl_ctx,
    _ensure_client,
    _format_customer_id,
    _track_api_call,
    _from_micros,
    _link_assets_to_entity,
    CreateAssetGroupRequest,
    CreateAssetGroupAssetsRequest,
    ListAdsRequest,
    UpdateAdStatusRequest,
    CreateResponsiveSearchAdRequest,
    CreateImageAssetRequest,
    CreateCalloutAssetRequest,
    CreateSitelinkAssetRequest,
    CreateStructuredSnippetAssetRequest,
    CreatePromotionAssetRequest,
    CreatePriceAssetRequest,
    CreateImageAssetForCampaignRequest,
    CreateBusinessIdentityAssetRequest,
    RemoveCampaignAssetLinkRequest,
    RemoveAdGroupAssetLinkRequest,
)


@mcp.tool()
async def google_ads_create_asset_group(request: CreateAssetGroupRequest) -> dict:
    """
    Create an asset group in a Performance Max campaign.

    Asset groups contain the creative assets (headlines, descriptions, images, etc.)
    used by the Performance Max campaign.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        asset_group_service = _gam.google_ads_client.get_service("AssetGroupService")

        # Create asset group
        operation = _gam.google_ads_client.get_type("AssetGroupOperation")
        asset_group = operation.create

        asset_group.name = request.asset_group_name
        asset_group.campaign = asset_group_service.campaign_path(customer_id, request.campaign_id)
        asset_group.status = _gam.google_ads_client.enums.AssetGroupStatusEnum.ENABLED

        # Set final URLs
        asset_group.final_urls.append(request.final_url)
        if request.final_mobile_url:
            asset_group.final_mobile_urls.append(request.final_mobile_url)

        # Perform the mutation
        response = asset_group_service.mutate_asset_groups(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_ASSET_GROUP')

        asset_group_resource_name = response.results[0].resource_name
        asset_group_id = asset_group_resource_name.split("/")[-1]

        return {
            "status": "success",
            "message": f"Asset group '{request.asset_group_name}' created successfully",
            "asset_group_id": asset_group_id,
            "asset_group_resource_name": asset_group_resource_name,
            "campaign_id": request.campaign_id,
            "next_step": "Add text assets with google_ads_create_asset_group_text_assets"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create asset group: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_asset_group_text_assets(request: CreateAssetGroupAssetsRequest) -> dict:
    """
    Create and link text assets (headlines, descriptions) to an asset group.

    Performance Max campaigns require multiple headlines and descriptions for
    optimal performance. Recommended: 3-15 headlines, 2-4 descriptions.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)

        asset_service = _gam.google_ads_client.get_service("AssetService")
        asset_group_asset_service = _gam.google_ads_client.get_service("AssetGroupAssetService")

        # Get the asset group resource name
        asset_group_service = _gam.google_ads_client.get_service("AssetGroupService")
        asset_group_resource_name = asset_group_service.asset_group_path(
            customer_id, request.asset_group_id
        )

        asset_resource_names = []

        # Create headline assets
        for i, headline in enumerate(request.headlines):
            asset_operation = _gam.google_ads_client.get_type("AssetOperation")
            asset = asset_operation.create
            asset.text_asset.text = headline

            asset_response = asset_service.mutate_assets(
                customer_id=customer_id,
                operations=[asset_operation]
            )
            _track_api_call('MUTATE_ASSET')

            asset_resource_name = asset_response.results[0].resource_name
            asset_resource_names.append({
                "resource_name": asset_resource_name,
                "field_type": _gam.google_ads_client.enums.AssetFieldTypeEnum.HEADLINE
            })

        # Create description assets
        for i, description in enumerate(request.descriptions):
            asset_operation = _gam.google_ads_client.get_type("AssetOperation")
            asset = asset_operation.create
            asset.text_asset.text = description

            asset_response = asset_service.mutate_assets(
                customer_id=customer_id,
                operations=[asset_operation]
            )
            _track_api_call('MUTATE_ASSET')

            asset_resource_name = asset_response.results[0].resource_name
            asset_resource_names.append({
                "resource_name": asset_resource_name,
                "field_type": _gam.google_ads_client.enums.AssetFieldTypeEnum.DESCRIPTION
            })

        # Create long headline assets if provided
        if request.long_headlines:
            for i, long_headline in enumerate(request.long_headlines):
                asset_operation = _gam.google_ads_client.get_type("AssetOperation")
                asset = asset_operation.create
                asset.text_asset.text = long_headline

                asset_response = asset_service.mutate_assets(
                    customer_id=customer_id,
                    operations=[asset_operation]
                )
                _track_api_call('MUTATE_ASSET')

                asset_resource_name = asset_response.results[0].resource_name
                asset_resource_names.append({
                    "resource_name": asset_resource_name,
                    "field_type": _gam.google_ads_client.enums.AssetFieldTypeEnum.LONG_HEADLINE
                })

        # Create business name asset
        asset_operation = _gam.google_ads_client.get_type("AssetOperation")
        asset = asset_operation.create
        asset.text_asset.text = request.business_name

        asset_response = asset_service.mutate_assets(
            customer_id=customer_id,
            operations=[asset_operation]
        )
        _track_api_call('MUTATE_ASSET')

        business_name_asset = asset_response.results[0].resource_name

        # Link all assets to the asset group
        asset_group_asset_operations = []

        for asset_info in asset_resource_names:
            ag_asset_op = _gam.google_ads_client.get_type("AssetGroupAssetOperation")
            ag_asset = ag_asset_op.create
            ag_asset.asset = asset_info["resource_name"]
            ag_asset.asset_group = asset_group_resource_name
            ag_asset.field_type = asset_info["field_type"]
            asset_group_asset_operations.append(ag_asset_op)

        # Link business name
        ag_asset_op = _gam.google_ads_client.get_type("AssetGroupAssetOperation")
        ag_asset = ag_asset_op.create
        ag_asset.asset = business_name_asset
        ag_asset.asset_group = asset_group_resource_name
        ag_asset.field_type = _gam.google_ads_client.enums.AssetFieldTypeEnum.BUSINESS_NAME
        asset_group_asset_operations.append(ag_asset_op)

        # Perform the mutations
        response = asset_group_asset_service.mutate_asset_group_assets(
            customer_id=customer_id,
            operations=asset_group_asset_operations
        )
        _track_api_call('MUTATE_LINK', len(asset_group_asset_operations))

        return {
            "status": "success",
            "message": f"Added {len(asset_resource_names) + 1} text assets to asset group {request.asset_group_id}",
            "asset_group_id": request.asset_group_id,
            "assets_created": {
                "headlines": len(request.headlines),
                "descriptions": len(request.descriptions),
                "long_headlines": len(request.long_headlines) if request.long_headlines else 0,
                "business_name": 1
            },
            "total_assets": len(asset_resource_names) + 1,
            "next_step": "Add listing group filter with google_ads_create_listing_group_filter"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create asset group text assets: {str(e)}"
        }


@mcp.tool()
async def google_ads_list_ads(request: ListAdsRequest) -> dict:
    """
    List ads with performance metrics.

    Returns ad details including ID, type, status, URLs, and metrics.
    """
    try:
        customer_id = _format_customer_id(request.customer_id)

        query = """
            SELECT
                ad_group_ad.ad.id,
                ad_group_ad.ad.name,
                ad_group_ad.ad.type,
                ad_group_ad.status,
                ad_group_ad.ad.final_urls,
                ad_group.id,
                ad_group.name,
                campaign.id,
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.ctr,
                metrics.cost_micros,
                metrics.conversions
            FROM ad_group_ad
            WHERE campaign.status != 'REMOVED'
        """

        if request.ad_group_id:
            query += f" AND ad_group.id = {request.ad_group_id}"
        if request.campaign_id:
            query += f" AND campaign.id = {request.campaign_id}"
        if request.status:
            query += f" AND ad_group_ad.status = '{request.status}'"

        query += f" LIMIT {request.page_size}"

        from google_ads_mcp import _execute_gaql, _safe_get_value
        results = _execute_gaql(customer_id, query, request.page_size)

        ads = []
        for row in results:
            ads.append({
                "id": str(_safe_get_value(row, "ad_group_ad.ad.id")),
                "name": _safe_get_value(row, "ad_group_ad.ad.name"),
                "type": str(_safe_get_value(row, "ad_group_ad.ad.type")),
                "status": str(_safe_get_value(row, "ad_group_ad.status")),
                "final_urls": _safe_get_value(row, "ad_group_ad.ad.final_urls"),
                "ad_group_id": str(_safe_get_value(row, "ad_group.id")),
                "ad_group_name": _safe_get_value(row, "ad_group.name"),
                "campaign_id": str(_safe_get_value(row, "campaign.id")),
                "campaign_name": _safe_get_value(row, "campaign.name"),
                "impressions": _safe_get_value(row, "metrics.impressions", 0),
                "clicks": _safe_get_value(row, "metrics.clicks", 0),
                "ctr": _safe_get_value(row, "metrics.ctr", 0),
                "cost": _from_micros(_safe_get_value(row, "metrics.cost_micros", 0)),
                "conversions": _safe_get_value(row, "metrics.conversions", 0)
            })

        return {
            "status": "success",
            "results": ads,
            "count": len(ads)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list ads: {str(e)}"
        }


@mcp.tool()
async def google_ads_update_ad_status(request: UpdateAdStatusRequest) -> dict:
    """Update individual ad status (enable or pause)."""
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ad_group_ad_service = _gam.google_ads_client.get_service("AdGroupAdService")

        operation = _gam.google_ads_client.get_type("AdGroupAdOperation")
        ad_group_ad = operation.update
        ad_group_ad.resource_name = f"customers/{customer_id}/adGroupAds/{request.ad_group_id}~{request.ad_id}"

        status_map = {
            "ENABLED": _gam.google_ads_client.enums.AdGroupAdStatusEnum.ENABLED,
            "PAUSED": _gam.google_ads_client.enums.AdGroupAdStatusEnum.PAUSED
        }
        ad_group_ad.status = status_map[request.status]

        from google.protobuf import field_mask_pb2
        fm = field_mask_pb2.FieldMask(paths=["status"])
        operation.update_mask = fm

        result = ad_group_ad_service.mutate_ad_group_ads(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_AD')

        return {
            "status": "success",
            "message": f"Ad {request.ad_id} status updated to {request.status}",
            "resource_name": result.results[0].resource_name
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to update ad status: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_responsive_search_ad(request: CreateResponsiveSearchAdRequest) -> dict:
    """
    Create a Responsive Search Ad (RSA) in an ad group.

    Requires 3-15 headlines and 2-4 descriptions. Google will automatically
    test combinations to find the best performing ad.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ad_group_ad_service = _gam.google_ads_client.get_service("AdGroupAdService")

        operation = _gam.google_ads_client.get_type("AdGroupAdOperation")
        ad_group_ad = operation.create
        ad_group_ad.ad_group = ad_group_ad_service.ad_group_path(customer_id, request.ad_group_id)

        ad = ad_group_ad.ad

        # Add headlines (support both string and dict with pinned_field)
        for headline in request.headlines:
            headline_asset = _gam.google_ads_client.get_type("AdTextAsset")
            if isinstance(headline, dict):
                headline_asset.text = headline["text"]
                if headline.get("pinned_field"):
                    headline_asset.pinned_field = getattr(
                        _gam.google_ads_client.enums.ServedAssetFieldTypeEnum,
                        headline["pinned_field"]
                    )
            else:
                headline_asset.text = headline
            ad.responsive_search_ad.headlines.append(headline_asset)

        # Add descriptions (support both string and dict with pinned_field)
        for desc in request.descriptions:
            desc_asset = _gam.google_ads_client.get_type("AdTextAsset")
            if isinstance(desc, dict):
                desc_asset.text = desc["text"]
                if desc.get("pinned_field"):
                    desc_asset.pinned_field = getattr(
                        _gam.google_ads_client.enums.ServedAssetFieldTypeEnum,
                        desc["pinned_field"]
                    )
            else:
                desc_asset.text = desc
            ad.responsive_search_ad.descriptions.append(desc_asset)

        # Set final URLs
        ad.final_urls.extend(request.final_urls)

        # Optional display path
        if request.path1:
            ad.responsive_search_ad.path1 = request.path1
        if request.path2:
            ad.responsive_search_ad.path2 = request.path2

        # --- Attempt 1: normal creation ---
        try:
            result = ad_group_ad_service.mutate_ad_group_ads(
                customer_id=customer_id,
                operations=[operation]
            )
            _track_api_call('MUTATE_AD')

            return {
                "status": "success",
                "message": f"Responsive Search Ad created in ad group {request.ad_group_id}",
                "ad_resource_name": result.results[0].resource_name
            }
        except _GoogleAdsException as policy_ex:
            # --- Attempt 2: policy exemption retry ---
            ignorable_topics = []
            has_non_policy_error = False
            for error in policy_ex.failure.errors:
                if error.details and error.details.policy_finding_details:
                    for entry in error.details.policy_finding_details.policy_topic_entries:
                        ignorable_topics.append(entry.topic)
                else:
                    has_non_policy_error = True

            if has_non_policy_error or not ignorable_topics:
                raise  # Non-policy error or no topics to exempt

            import logging
            logging.getLogger("mcp_ads").info(
                f"[PolicyExempt] RSA ag={request.ad_group_id}: retrying with ignorable_policy_topics={ignorable_topics}"
            )
            operation.policy_validation_parameter.ignorable_policy_topics.extend(ignorable_topics)
            result = ad_group_ad_service.mutate_ad_group_ads(
                customer_id=customer_id,
                operations=[operation]
            )
            _track_api_call('MUTATE_AD')

            return {
                "status": "success",
                "message": f"RSA created via policy exemption (topics: {ignorable_topics}) in ad group {request.ad_group_id}",
                "ad_resource_name": result.results[0].resource_name,
                "policy_exemption_used": True,
                "exempted_topics": ignorable_topics
            }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create responsive search ad: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_image_asset(request: CreateImageAssetRequest) -> dict:
    """
    Create an image asset from URL and link it to an asset group.

    Downloads the image, creates it as an asset in Google Ads, then links it
    to the specified PMax asset group as a MARKETING_IMAGE.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        asset_service = _gam.google_ads_client.get_service("AssetService")
        asset_group_asset_service = _gam.google_ads_client.get_service("AssetGroupAssetService")

        # Get image bytes from URL or local file path
        if request.image_url.startswith(("http://", "https://")):
            image_bytes = urllib.request.urlopen(request.image_url, context=_ssl_ctx).read()
        else:
            with open(request.image_url, "rb") as f:
                image_bytes = f.read()

        # Step 1: Create the image asset
        asset_operation = _gam.google_ads_client.get_type("AssetOperation")
        asset = asset_operation.create
        asset.name = request.asset_name or f"Image Asset {request.asset_group_id}"
        asset.type_ = _gam.google_ads_client.enums.AssetTypeEnum.IMAGE
        asset.image_asset.data = image_bytes

        asset_result = asset_service.mutate_assets(
            customer_id=customer_id,
            operations=[asset_operation]
        )
        _track_api_call('MUTATE_ASSET')
        asset_resource_name = asset_result.results[0].resource_name

        # Step 2: Link asset to asset group
        aga_operation = _gam.google_ads_client.get_type("AssetGroupAssetOperation")
        asset_group_asset = aga_operation.create
        asset_group_asset.asset = asset_resource_name
        asset_group_asset.asset_group = f"customers/{customer_id}/assetGroups/{request.asset_group_id}"
        asset_group_asset.field_type = _gam.google_ads_client.enums.AssetFieldTypeEnum.MARKETING_IMAGE

        aga_result = asset_group_asset_service.mutate_asset_group_assets(
            customer_id=customer_id,
            operations=[aga_operation]
        )
        _track_api_call('MUTATE_LINK')

        return {
            "status": "success",
            "message": f"Image asset created and linked to asset group {request.asset_group_id}",
            "asset_resource_name": asset_resource_name,
            "link_resource_name": aga_result.results[0].resource_name
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create image asset: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_callout_assets(request: CreateCalloutAssetRequest) -> dict:
    """
    Create callout assets and optionally link them to a campaign or ad group.

    Callouts are short snippets (max 25 chars) that highlight key business features
    like 'Free Shipping', '24/7 Support', 'Official Store'. Min 2, max 20 callouts.
    If no campaign_id is provided, callouts are created at account level.
    Use ad_group_id for product-specific callouts (lowest granularity available).
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        asset_service = _gam.google_ads_client.get_service("AssetService")

        asset_resource_names = []

        # Step 1: Create callout assets
        for callout_text in request.callout_texts:
            asset_operation = _gam.google_ads_client.get_type("AssetOperation")
            asset = asset_operation.create
            asset.callout_asset.callout_text = callout_text

            result = asset_service.mutate_assets(
                customer_id=customer_id,
                operations=[asset_operation]
            )
            _track_api_call('MUTATE_ASSET')
            asset_resource_names.append(result.results[0].resource_name)

        # Step 2: Link to ad group, campaign, or customer
        linked_to = _link_assets_to_entity(
            customer_id, asset_resource_names,
            _gam.google_ads_client.enums.AssetFieldTypeEnum.CALLOUT,
            ad_group_id=request.ad_group_id, campaign_id=request.campaign_id
        )

        return {
            "status": "success",
            "message": f"Created {len(request.callout_texts)} callout assets, linked to {linked_to}",
            "callouts": request.callout_texts,
            "asset_resource_names": asset_resource_names
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create callout assets: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_sitelink_assets(request: CreateSitelinkAssetRequest) -> dict:
    """
    Create sitelink assets and optionally link them to a campaign or ad group.

    Sitelinks add additional links below your ad, each with a title (max 25 chars),
    two description lines (max 35 chars each), and a landing page URL.
    Recommended: 4-8 sitelinks per campaign/ad group.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        asset_service = _gam.google_ads_client.get_service("AssetService")

        asset_resource_names = []

        # Step 1: Create sitelink assets
        for sl in request.sitelinks:
            asset_operation = _gam.google_ads_client.get_type("AssetOperation")
            asset = asset_operation.create

            asset.sitelink_asset.link_text = sl["link_text"]
            if sl.get("description1"):
                asset.sitelink_asset.description1 = sl["description1"]
            if sl.get("description2"):
                asset.sitelink_asset.description2 = sl["description2"]

            # Set final URLs
            for url in sl.get("final_urls", []):
                asset.final_urls.append(url)

            result = asset_service.mutate_assets(
                customer_id=customer_id,
                operations=[asset_operation]
            )
            _track_api_call('MUTATE_ASSET')
            asset_resource_names.append(result.results[0].resource_name)

        # Step 2: Link to ad group, campaign, or customer
        linked_to = _link_assets_to_entity(
            customer_id, asset_resource_names,
            _gam.google_ads_client.enums.AssetFieldTypeEnum.SITELINK,
            ad_group_id=request.ad_group_id, campaign_id=request.campaign_id
        )

        return {
            "status": "success",
            "message": f"Created {len(request.sitelinks)} sitelink assets, linked to {linked_to}",
            "sitelinks_created": len(request.sitelinks),
            "asset_resource_names": asset_resource_names
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create sitelink assets: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_structured_snippet_assets(request: CreateStructuredSnippetAssetRequest) -> dict:
    """
    Create structured snippet assets and optionally link to a campaign.

    Structured snippets highlight specific aspects of your products/services under
    a predefined header. Headers include: Brands, Types, Models, Destinations,
    Styles, Courses, Amenities, Insurance coverage, Neighborhoods, etc.
    Min 3 values required.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        asset_service = _gam.google_ads_client.get_service("AssetService")

        # Create the structured snippet asset
        asset_operation = _gam.google_ads_client.get_type("AssetOperation")
        asset = asset_operation.create
        asset.structured_snippet_asset.header = request.header
        for value in request.values:
            asset.structured_snippet_asset.values.append(value)

        result = asset_service.mutate_assets(
            customer_id=customer_id,
            operations=[asset_operation]
        )
        _track_api_call('MUTATE_ASSET')
        asset_resource_name = result.results[0].resource_name

        # Link to ad group, campaign, or customer
        linked_to = _link_assets_to_entity(
            customer_id, [asset_resource_name],
            _gam.google_ads_client.enums.AssetFieldTypeEnum.STRUCTURED_SNIPPET,
            ad_group_id=request.ad_group_id, campaign_id=request.campaign_id
        )

        return {
            "status": "success",
            "message": f"Created structured snippet '{request.header}' with {len(request.values)} values, linked to {linked_to}",
            "header": request.header,
            "values": request.values,
            "asset_resource_name": asset_resource_name
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create structured snippet assets: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_promotion_asset(request: CreatePromotionAssetRequest) -> dict:
    """
    Create a promotion asset and optionally link to a campaign.

    Promotion assets showcase sales and special offers. They can include percentage
    or monetary discounts, occasion labels (e.g., Black Friday, Summer Sale),
    and promotion dates. Use either percent_off OR money_amount_off, not both.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        asset_service = _gam.google_ads_client.get_service("AssetService")

        # Create the promotion asset
        asset_operation = _gam.google_ads_client.get_type("AssetOperation")
        asset = asset_operation.create
        asset.promotion_asset.promotion_target = request.promotion_target
        asset.promotion_asset.language_code = request.language_code

        # Set discount (either percent OR money, not both)
        if request.percent_off:
            asset.promotion_asset.percent_off = request.percent_off
        elif request.money_amount_off_micros and request.money_amount_off_currency:
            asset.promotion_asset.money_amount_off.amount_micros = request.money_amount_off_micros
            asset.promotion_asset.money_amount_off.currency_code = request.money_amount_off_currency

        # Set occasion if provided
        if request.occasion:
            asset.promotion_asset.occasion = getattr(
                _gam.google_ads_client.enums.PromotionExtensionOccasionEnum, request.occasion
            )

        # Set promotion dates if provided
        if request.promotion_start_date:
            asset.promotion_asset.start_date = request.promotion_start_date
        if request.promotion_end_date:
            asset.promotion_asset.end_date = request.promotion_end_date

        # Set final URLs
        for url in request.final_urls:
            asset.final_urls.append(url)

        result = asset_service.mutate_assets(
            customer_id=customer_id,
            operations=[asset_operation]
        )
        _track_api_call('MUTATE_ASSET')
        asset_resource_name = result.results[0].resource_name

        # Link to ad group, campaign, or customer
        linked_to = _link_assets_to_entity(
            customer_id, [asset_resource_name],
            _gam.google_ads_client.enums.AssetFieldTypeEnum.PROMOTION,
            ad_group_id=request.ad_group_id, campaign_id=request.campaign_id
        )

        return {
            "status": "success",
            "message": f"Created promotion asset for '{request.promotion_target}', linked to {linked_to}",
            "promotion_target": request.promotion_target,
            "discount": f"{request.percent_off}% off" if request.percent_off else f"{request.money_amount_off_micros/1000000} {request.money_amount_off_currency} off" if request.money_amount_off_micros else "No discount set",
            "occasion": request.occasion,
            "asset_resource_name": asset_resource_name
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create promotion asset: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_price_assets(request: CreatePriceAssetRequest) -> dict:
    """
    Create a price asset with price offerings and optionally link to a campaign.

    Price assets display your products/services with their prices directly in
    the ad. Each offering has a header (max 25 chars), description (max 25 chars),
    price, and landing page URL. Min 3 offerings required.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        asset_service = _gam.google_ads_client.get_service("AssetService")

        # Create the price asset
        asset_operation = _gam.google_ads_client.get_type("AssetOperation")
        asset = asset_operation.create

        # Set price type
        asset.price_asset.type_ = getattr(
            _gam.google_ads_client.enums.PriceExtensionTypeEnum, request.price_type
        )
        asset.price_asset.language_code = request.language_code

        # Add price offerings
        for offering in request.price_offerings:
            price_offering = _gam.google_ads_client.get_type("PriceOffering")
            price_offering.header = offering["header"]
            price_offering.description = offering["description"]
            price_offering.price.amount_micros = offering["price_micros"]
            price_offering.price.currency_code = offering["currency_code"]
            price_offering.final_url = offering["final_url"]

            if offering.get("unit"):
                price_offering.unit = getattr(
                    _gam.google_ads_client.enums.PriceExtensionPriceUnitEnum, offering["unit"]
                )

            asset.price_asset.price_offerings.append(price_offering)

        result = asset_service.mutate_assets(
            customer_id=customer_id,
            operations=[asset_operation]
        )
        _track_api_call('MUTATE_ASSET')
        asset_resource_name = result.results[0].resource_name

        # Link to ad group, campaign, or customer
        linked_to = _link_assets_to_entity(
            customer_id, [asset_resource_name],
            _gam.google_ads_client.enums.AssetFieldTypeEnum.PRICE,
            ad_group_id=request.ad_group_id, campaign_id=request.campaign_id
        )

        return {
            "status": "success",
            "message": f"Created price asset with {len(request.price_offerings)} offerings, linked to {linked_to}",
            "price_type": request.price_type,
            "offerings_count": len(request.price_offerings),
            "asset_resource_name": asset_resource_name
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create price assets: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_campaign_image_asset(request: CreateImageAssetForCampaignRequest) -> dict:
    """
    Create an image asset and link it to a Search campaign as an image extension.

    Image extensions add visual elements to Search ads. Images must meet Google's
    quality standards: no logo overlays, no text overlays, not blurry or poorly cropped.
    Recommended: landscape (1.91:1) or square (1:1) images, min 300x300px.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        asset_service = _gam.google_ads_client.get_service("AssetService")

        # Get image bytes from URL or local file path
        if request.image_url.startswith(("http://", "https://")):
            image_bytes = urllib.request.urlopen(request.image_url, context=_ssl_ctx).read()
        else:
            # Treat as local file path
            with open(request.image_url, "rb") as f:
                image_bytes = f.read()

        # Step 1: Create the image asset
        asset_operation = _gam.google_ads_client.get_type("AssetOperation")
        asset = asset_operation.create
        asset.name = request.asset_name or f"Image {request.ad_group_id or request.campaign_id}"
        asset.type_ = _gam.google_ads_client.enums.AssetTypeEnum.IMAGE
        asset.image_asset.data = image_bytes

        asset_result = asset_service.mutate_assets(
            customer_id=customer_id,
            operations=[asset_operation]
        )
        _track_api_call('MUTATE_ASSET')
        asset_resource_name = asset_result.results[0].resource_name

        # Step 2: Link image to ad group, campaign, or account
        linked_to = _link_assets_to_entity(
            customer_id, [asset_resource_name],
            _gam.google_ads_client.enums.AssetFieldTypeEnum.AD_IMAGE,
            ad_group_id=request.ad_group_id, campaign_id=request.campaign_id
        )

        return {
            "status": "success",
            "message": f"Image asset created and linked to {linked_to}",
            "asset_resource_name": asset_resource_name
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create campaign image asset: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_business_identity_assets(request: CreateBusinessIdentityAssetRequest) -> dict:
    """
    Set business name, logo, and/or landscape logo for a campaign.

    Business name (max 25 chars), square logo (1:1 ratio, min 128x128px),
    and landscape logo (4:1 ratio, min 512x128px) appear alongside your ads
    and help build brand recognition.

    For PMax campaigns, Google recommends providing BOTH square and landscape logos.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        asset_service = _gam.google_ads_client.get_service("AssetService")
        campaign_asset_service = _gam.google_ads_client.get_service("CampaignAssetService")

        results = {}

        # Create and link business name asset
        if request.business_name:
            asset_operation = _gam.google_ads_client.get_type("AssetOperation")
            asset = asset_operation.create
            asset.text_asset.text = request.business_name

            asset_result = asset_service.mutate_assets(
                customer_id=customer_id,
                operations=[asset_operation]
            )
            _track_api_call('MUTATE_ASSET')
            name_asset_rn = asset_result.results[0].resource_name

            link_op = _gam.google_ads_client.get_type("CampaignAssetOperation")
            link = link_op.create
            link.asset = name_asset_rn
            link.campaign = campaign_asset_service.campaign_path(customer_id, request.campaign_id)
            link.field_type = _gam.google_ads_client.enums.AssetFieldTypeEnum.BUSINESS_NAME

            campaign_asset_service.mutate_campaign_assets(
                customer_id=customer_id,
                operations=[link_op]
            )
            _track_api_call('MUTATE_LINK')
            results["business_name"] = {"text": request.business_name, "asset": name_asset_rn}

        # Create and link logo asset
        if request.logo_url:
            image_bytes = urllib.request.urlopen(request.logo_url, context=_ssl_ctx).read()

            asset_operation = _gam.google_ads_client.get_type("AssetOperation")
            asset = asset_operation.create
            asset.name = f"Business Logo {request.campaign_id}"
            asset.type_ = _gam.google_ads_client.enums.AssetTypeEnum.IMAGE
            asset.image_asset.data = image_bytes

            asset_result = asset_service.mutate_assets(
                customer_id=customer_id,
                operations=[asset_operation]
            )
            _track_api_call('MUTATE_ASSET')
            logo_asset_rn = asset_result.results[0].resource_name

            link_op = _gam.google_ads_client.get_type("CampaignAssetOperation")
            link = link_op.create
            link.asset = logo_asset_rn
            link.campaign = campaign_asset_service.campaign_path(customer_id, request.campaign_id)
            link.field_type = _gam.google_ads_client.enums.AssetFieldTypeEnum.BUSINESS_LOGO

            campaign_asset_service.mutate_campaign_assets(
                customer_id=customer_id,
                operations=[link_op]
            )
            _track_api_call('MUTATE_LINK')
            results["business_logo"] = {"asset": logo_asset_rn}

        # Create and link landscape logo asset (4:1 ratio)
        if request.landscape_logo_url:
            landscape_bytes = urllib.request.urlopen(request.landscape_logo_url, context=_ssl_ctx).read()

            asset_operation = _gam.google_ads_client.get_type("AssetOperation")
            asset = asset_operation.create
            asset.name = f"Landscape Logo {request.campaign_id}"
            asset.type_ = _gam.google_ads_client.enums.AssetTypeEnum.IMAGE
            asset.image_asset.data = landscape_bytes

            asset_result = asset_service.mutate_assets(
                customer_id=customer_id,
                operations=[asset_operation]
            )
            _track_api_call('MUTATE_ASSET')
            landscape_logo_rn = asset_result.results[0].resource_name

            link_op = _gam.google_ads_client.get_type("CampaignAssetOperation")
            link = link_op.create
            link.asset = landscape_logo_rn
            link.campaign = campaign_asset_service.campaign_path(customer_id, request.campaign_id)
            link.field_type = _gam.google_ads_client.enums.AssetFieldTypeEnum.LANDSCAPE_LOGO

            campaign_asset_service.mutate_campaign_assets(
                customer_id=customer_id,
                operations=[link_op]
            )
            _track_api_call('MUTATE_LINK')
            results["landscape_logo"] = {"asset": landscape_logo_rn}

        return {
            "status": "success",
            "message": f"Business identity assets created for campaign {request.campaign_id}",
            "campaign_id": request.campaign_id,
            "assets_created": results
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create business identity assets: {str(e)}"
        }


@mcp.tool()
async def google_ads_remove_campaign_asset_links(request: RemoveCampaignAssetLinkRequest) -> dict:
    """
    Remove campaign-level asset links (unlink assets from a campaign).

    This does NOT delete the underlying assets — it only removes the link
    between the asset and the campaign. Use this to move extensions from
    campaign-level to ad-group-level targeting.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_asset_service = _gam.google_ads_client.get_service("CampaignAssetService")

        operations = []
        for resource_name in request.campaign_asset_resource_names:
            op = _gam.google_ads_client.get_type("CampaignAssetOperation")
            op.remove = resource_name
            operations.append(op)

        campaign_asset_service.mutate_campaign_assets(
            customer_id=customer_id,
            operations=operations
        )
        _track_api_call('MUTATE_LINK', len(operations))

        return {
            "status": "success",
            "message": f"Removed {len(operations)} campaign asset links",
            "removed": request.campaign_asset_resource_names
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to remove campaign asset links: {str(e)}"
        }


@mcp.tool()
async def google_ads_remove_ad_group_asset_links(request: RemoveAdGroupAssetLinkRequest) -> dict:
    """
    Remove ad-group-level asset links (unlink assets from an ad group).

    This does NOT delete the underlying assets — it only removes the link
    between the asset and the ad group.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ad_group_asset_service = _gam.google_ads_client.get_service("AdGroupAssetService")

        operations = []
        for resource_name in request.ad_group_asset_resource_names:
            op = _gam.google_ads_client.get_type("AdGroupAssetOperation")
            op.remove = resource_name
            operations.append(op)

        ad_group_asset_service.mutate_ad_group_assets(
            customer_id=customer_id,
            operations=operations
        )
        _track_api_call('MUTATE_LINK', len(operations))

        return {
            "status": "success",
            "message": f"Removed {len(operations)} ad group asset links",
            "removed": request.ad_group_asset_resource_names
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to remove ad group asset links: {str(e)}"
        }
