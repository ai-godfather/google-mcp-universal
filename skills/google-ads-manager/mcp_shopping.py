"""
Shopping Campaign and Listing Group Management Module.

Handles creation and management of Google Shopping campaigns,
ad groups, shopping ads, listing groups, and performance metrics.
"""

from typing import List
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


class CreateShoppingCampaignRequest(BaseModel):
    """Request model for creating a Shopping campaign"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_name: str = Field(..., description="Name for the campaign")
    daily_budget_micros: int = Field(..., description="Daily budget in micros")
    merchant_id: str = Field(..., description="Google Merchant Center merchant ID")
    sales_country: str = Field(..., description="Sales country code (e.g., 'US', 'PL', 'GB')")
    status: str = Field("ENABLED", description="Campaign status: ENABLED or PAUSED")
    campaign_priority: str = Field("MEDIUM", description="Campaign priority: LOW, MEDIUM, or HIGH")
    feed_label: str = Field(None, description="Feed label to filter products (e.g., 'PLFEEDNEW' for NEW Products feed)")
    enable_local_inventory: bool = Field(False, description="Enable local inventory ads")


class CreateShoppingAdGroupRequest(BaseModel):
    """Request model for creating a Shopping ad group"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    ad_group_name: str = Field(..., description="Name for the ad group")
    cpc_bid_micros: int = Field(..., description="Max CPC bid in micros")


class CreateShoppingAdRequest(BaseModel):
    """Request model for creating a Shopping ad"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")


class ShoppingListingGroupSubdivisionRequest(BaseModel):
    """Request model for creating a listing group subdivision in Shopping campaign"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    parent_criterion_id: str = Field(
        ..., description="Parent listing group criterion ID to subdivide"
    )
    dimension: str = Field(
        ..., description="Dimension to subdivide by: custom_label_0-4, product_type, brand, channel, condition"
    )
    values: List[str] = Field(
        ..., description="List of dimension values to create as UNIT nodes"
    )
    cpc_bid_micros: int = Field(10000, description="Default CPC bid in micros for each UNIT node")
    everything_else_bid_micros: int = Field(10000, description="CPC bid for 'Everything else' catch-all node")


class ShoppingListingGroupBidRequest(BaseModel):
    """Request model for updating bids on Shopping listing group nodes"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    criterion_bids: List[dict] = Field(
        ..., description="List of {criterion_id: str, cpc_bid_micros: int} dicts"
    )


class ShoppingListingGroupRemoveRequest(BaseModel):
    """Request model for removing Shopping listing group nodes"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    criterion_ids: List[str] = Field(..., description="List of criterion IDs to remove")


class RebuildShoppingListingGroupTreeRequest(BaseModel):
    """Request model for atomic rebuild of listing group tree"""
    customer_id: str = Field(..., description="Customer ID")
    ad_group_id: str = Field(..., description="Ad group ID")
    root_dimension: str = Field(
        ..., description="Dimension for first-level split"
    )
    groups: List[dict] = Field(
        ..., description="List of product group definitions"
    )
    everything_else_bid_micros: int = Field(
        10000, description="CPC bid for root-level 'Everything else' catch-all node"
    )


class CreateAssetGroupListingFilterRequest(BaseModel):
    """Request model for creating listing group filter for asset group"""
    customer_id: str = Field(..., description="Customer ID")
    asset_group_id: str = Field(..., description="Asset group ID")
    filter_type: str = Field("ALL_PRODUCTS", description="ALL_PRODUCTS or CUSTOM")
    product_brand: str = Field(None, description="Product brand filter")
    product_type: str = Field(None, description="Product type filter")
    custom_label_0: str = Field(None, description="Custom label 0")
    custom_label_1: str = Field(None, description="Custom label 1")
    custom_label_2: str = Field(None, description="Custom label 2")
    custom_label_3: str = Field(None, description="Custom label 3")
    custom_label_4: str = Field(None, description="Custom label 4")


class ShoppingPerformanceRequest(BaseModel):
    """Request model for shopping performance report"""
    customer_id: str = Field(..., description="Customer ID")
    start_date: str = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: str = Field(..., description="End date (YYYY-MM-DD)")
    campaign_id: str = Field(None, description="Campaign ID to filter by")
    min_impressions: int = Field(10, description="Minimum impressions filter")
    page_size: int = Field(100, description="Number of results per page")


class CreatePMaxCampaignRequest(BaseModel):
    """Request model for creating Performance Max campaign"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_name: str = Field(..., description="Name for the campaign")
    daily_budget_micros: int = Field(..., description="Daily budget in micros")
    merchant_id: str = Field(..., description="Google Merchant Center merchant ID")
    sales_country: str = Field(..., description="Sales country code")
    final_url: str = Field(..., description="Final URL for the campaign")
    status: str = Field("ENABLED", description="Campaign status: ENABLED or PAUSED")


# ============================================================================
# SHOPPING CAMPAIGN TOOLS
# ============================================================================


@mcp.tool()
async def google_ads_create_shopping_campaign(request: CreateShoppingCampaignRequest) -> dict:
    """
    Create a Standard Shopping campaign linked to Google Merchant Center.

    Shopping campaigns display product ads from your Merchant Center feed.
    Automatically creates a dedicated budget for the campaign.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)

        # Services needed
        campaign_budget_service = _gam.google_ads_client.get_service("CampaignBudgetService")
        campaign_service = _gam.google_ads_client.get_service("CampaignService")

        # First create the budget
        budget_operation = _gam.google_ads_client.get_type("CampaignBudgetOperation")
        budget = budget_operation.create
        budget.name = f"{request.campaign_name} - Budget"
        budget.amount_micros = request.daily_budget_micros
        budget.delivery_method = _gam.google_ads_client.enums.BudgetDeliveryMethodEnum.STANDARD

        budget_response = campaign_budget_service.mutate_campaign_budgets(
            customer_id=customer_id,
            operations=[budget_operation]
        )
        _track_api_call('MUTATE_BUDGET')

        budget_resource_name = budget_response.results[0].resource_name

        # Now create the campaign
        campaign_operation = _gam.google_ads_client.get_type("CampaignOperation")
        campaign = campaign_operation.create

        campaign.name = request.campaign_name
        campaign.advertising_channel_type = _gam.google_ads_client.enums.AdvertisingChannelTypeEnum.SHOPPING
        campaign.campaign_budget = budget_resource_name

        # Set status
        status_map = {
            "ENABLED": _gam.google_ads_client.enums.CampaignStatusEnum.ENABLED,
            "PAUSED": _gam.google_ads_client.enums.CampaignStatusEnum.PAUSED
        }
        campaign.status = status_map.get(request.status, _gam.google_ads_client.enums.CampaignStatusEnum.ENABLED)

        # Configure shopping settings
        campaign.shopping_setting.merchant_id = int(request.merchant_id)
        campaign.shopping_setting.sales_country = request.sales_country

        # Set feed_label to filter products
        if request.feed_label:
            campaign.shopping_setting.feed_label = request.feed_label

        # Map priority
        priority_map = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
        campaign.shopping_setting.campaign_priority = priority_map.get(request.campaign_priority, 1)
        campaign.shopping_setting.enable_local = request.enable_local_inventory

        # Set manual CPC bidding
        campaign.manual_cpc.enhanced_cpc_enabled = True

        # Perform the mutation
        response = campaign_service.mutate_campaigns(
            customer_id=customer_id,
            operations=[campaign_operation]
        )
        _track_api_call('MUTATE_CAMPAIGN')

        campaign_resource_name = response.results[0].resource_name
        campaign_id = campaign_resource_name.split("/")[-1]

        return {
            "status": "success",
            "message": f"Shopping campaign '{request.campaign_name}' created successfully",
            "campaign_id": campaign_id,
            "campaign_resource_name": campaign_resource_name,
            "budget_id": budget_resource_name.split("/")[-1],
            "daily_budget": _from_micros(request.daily_budget_micros),
            "advertising_channel": "SHOPPING"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create Shopping campaign: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_shopping_ad_group(request: CreateShoppingAdGroupRequest) -> dict:
    """
    Create an ad group in a Standard Shopping campaign.

    Shopping ad groups organize products and bids for a campaign.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ad_group_service = _gam.google_ads_client.get_service("AdGroupService")

        # Create ad group
        operation = _gam.google_ads_client.get_type("AdGroupOperation")
        ad_group = operation.create

        ad_group.name = request.ad_group_name
        ad_group.campaign = ad_group_service.campaign_path(customer_id, request.campaign_id)
        ad_group.status = _gam.google_ads_client.enums.AdGroupStatusEnum.ENABLED
        ad_group.type_ = _gam.google_ads_client.enums.AdGroupTypeEnum.SHOPPING_PRODUCT_ADS

        # Set CPC bid
        ad_group.cpc_bid_micros = request.cpc_bid_micros

        # Perform the mutation
        response = ad_group_service.mutate_ad_groups(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_AD_GROUP')

        ad_group_resource_name = response.results[0].resource_name
        ad_group_id = ad_group_resource_name.split("/")[-1]

        return {
            "status": "success",
            "message": f"Ad group '{request.ad_group_name}' created successfully",
            "ad_group_id": ad_group_id,
            "ad_group_resource_name": ad_group_resource_name,
            "campaign_id": request.campaign_id,
            "cpc_bid": _from_micros(request.cpc_bid_micros)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create Shopping ad group: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_shopping_ad(request: CreateShoppingAdRequest) -> dict:
    """
    Create a Shopping ad in an ad group.

    Shopping ads are automatically generated from your Merchant Center feed.
    No creative content needs to be provided.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ad_group_ad_service = _gam.google_ads_client.get_service("AdGroupAdService")

        # Create the ad
        operation = _gam.google_ads_client.get_type("AdGroupAdOperation")
        ad_group_ad = operation.create

        ad_group_ad.ad_group = ad_group_ad_service.ad_group_path(
            customer_id, request.ad_group_id
        )

        # For Shopping ads, only the shopping_product_ad type is needed
        ad_group_ad.ad.shopping_product_ad = _gam.google_ads_client.get_type("ShoppingProductAdInfo")
        ad_group_ad.status = _gam.google_ads_client.enums.AdGroupAdStatusEnum.ENABLED

        # Perform the mutation
        response = ad_group_ad_service.mutate_ad_group_ads(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_AD')

        ad_resource_name = response.results[0].resource_name

        return {
            "status": "success",
            "message": f"Shopping ad created successfully in ad group {request.ad_group_id}",
            "ad_group_id": request.ad_group_id,
            "ad_resource_name": ad_resource_name,
            "note": "Product information is automatically pulled from Merchant Center feed"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create Shopping ad: {str(e)}"
        }


# ============================================================================
# SHOPPING LISTING GROUP TOOLS
# ============================================================================


@mcp.tool()
async def google_ads_create_shopping_listing_group_subdivision(request: ShoppingListingGroupSubdivisionRequest) -> dict:
    """
    Create a listing group subdivision in a Shopping campaign ad group.

    Splits a parent SUBDIVISION node by a dimension into multiple UNIT nodes
    with individual bids. Automatically creates an 'Everything else' catch-all.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ag_criterion_service = _gam.google_ads_client.get_service("AdGroupCriterionService")

        # Map dimension string to Google Ads enum/field
        dimension_map = {
            "custom_label_0": ("product_custom_attribute", "INDEX0"),
            "custom_label_1": ("product_custom_attribute", "INDEX1"),
            "custom_label_2": ("product_custom_attribute", "INDEX2"),
            "custom_label_3": ("product_custom_attribute", "INDEX3"),
            "custom_label_4": ("product_custom_attribute", "INDEX4"),
            "product_type": ("product_type", "LEVEL1"),
            "brand": ("product_brand", None),
            "channel": ("product_channel", None),
            "condition": ("product_condition", None),
        }

        if request.dimension not in dimension_map:
            return {"status": "error", "message": f"Unknown dimension: {request.dimension}"}

        dim_field, dim_index = dimension_map[request.dimension]
        parent_resource = f"customers/{customer_id}/adGroupCriteria/{request.ad_group_id}~{request.parent_criterion_id}"

        operations = []
        created_nodes = []

        # Create UNIT nodes for each value
        for value in request.values:
            op = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
            criterion = op.create
            criterion.ad_group = ag_criterion_service.ad_group_path(customer_id, request.ad_group_id)
            criterion.status = _gam.google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
            criterion.cpc_bid_micros = request.cpc_bid_micros

            # Set listing group type and parent
            criterion.listing_group.type_ = _gam.google_ads_client.enums.ListingGroupTypeEnum.UNIT
            criterion.listing_group.parent_ad_group_criterion = parent_resource

            # Set case value based on dimension
            if dim_field == "product_custom_attribute":
                criterion.listing_group.case_value.product_custom_attribute.index = (
                    getattr(_gam.google_ads_client.enums.ListingCustomAttributeIndexEnum, dim_index)
                )
                criterion.listing_group.case_value.product_custom_attribute.value = value
            elif dim_field == "product_brand":
                criterion.listing_group.case_value.product_brand.value = value
            elif dim_field == "product_type":
                criterion.listing_group.case_value.product_type.value = value
                criterion.listing_group.case_value.product_type.level = (
                    _gam.google_ads_client.enums.ProductTypeLevelEnum.LEVEL1
                )
            elif dim_field == "product_condition":
                cond_map = {
                    "NEW": _gam.google_ads_client.enums.ProductConditionEnum.NEW,
                    "USED": _gam.google_ads_client.enums.ProductConditionEnum.USED,
                    "REFURBISHED": _gam.google_ads_client.enums.ProductConditionEnum.REFURBISHED,
                }
                criterion.listing_group.case_value.product_condition.condition = cond_map.get(
                    value.upper(), _gam.google_ads_client.enums.ProductConditionEnum.NEW
                )

            operations.append(op)
            created_nodes.append({"value": value, "bid": _from_micros(request.cpc_bid_micros)})

        # Create 'Everything else' catch-all UNIT
        op_else = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
        criterion_else = op_else.create
        criterion_else.ad_group = ag_criterion_service.ad_group_path(customer_id, request.ad_group_id)
        criterion_else.status = _gam.google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
        criterion_else.cpc_bid_micros = request.everything_else_bid_micros
        criterion_else.listing_group.type_ = _gam.google_ads_client.enums.ListingGroupTypeEnum.UNIT
        criterion_else.listing_group.parent_ad_group_criterion = parent_resource
        operations.append(op_else)

        # Execute all operations atomically
        response = ag_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id,
            operations=operations
        )
        _track_api_call('MUTATE_LISTING_GROUP')

        result_ids = [r.resource_name.split("~")[-1] for r in response.results]

        return {
            "status": "success",
            "message": f"Created {len(request.values)} product groups + 1 'Everything else'",
            "dimension": request.dimension,
            "created_nodes": created_nodes,
            "everything_else_bid": _from_micros(request.everything_else_bid_micros),
            "criterion_ids": result_ids,
            "parent_criterion_id": request.parent_criterion_id,
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to create listing group subdivision: {str(e)}"}


@mcp.tool()
async def google_ads_update_shopping_listing_group_bids(request: ShoppingListingGroupBidRequest) -> dict:
    """
    Update CPC bids on individual Shopping listing group UNIT nodes.

    Allows setting different bids per product group.
    Only works on UNIT type nodes (leaf nodes with actual bids).
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ag_criterion_service = _gam.google_ads_client.get_service("AdGroupCriterionService")

        operations = []
        updates = []

        for item in request.criterion_bids:
            crit_id = str(item.get("criterion_id", ""))
            bid = int(item.get("cpc_bid_micros", 0))

            if not crit_id or bid <= 0:
                continue

            op = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
            criterion = op.update
            criterion.resource_name = f"customers/{customer_id}/adGroupCriteria/{request.ad_group_id}~{crit_id}"
            criterion.cpc_bid_micros = bid

            # Set field mask
            field_mask = _gam.google_ads_client.get_type("FieldMask")
            field_mask.paths.append("cpc_bid_micros")
            op.update_mask.CopyFrom(field_mask)

            operations.append(op)
            updates.append({"criterion_id": crit_id, "new_bid": _from_micros(bid)})

        if not operations:
            return {"status": "error", "message": "No valid criterion_bids provided"}

        response = ag_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id,
            operations=operations
        )
        _track_api_call('MUTATE_LISTING_GROUP_BID')

        return {
            "status": "success",
            "message": f"Updated bids on {len(operations)} listing group nodes",
            "updates": updates,
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to update listing group bids: {str(e)}"}


@mcp.tool()
async def google_ads_remove_shopping_listing_group_nodes(request: ShoppingListingGroupRemoveRequest) -> dict:
    """
    Remove listing group nodes from a Shopping campaign ad group.

    Can remove UNIT nodes or SUBDIVISION nodes (cascades to remove children).
    Use to clean up product group trees before rebuilding.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ag_criterion_service = _gam.google_ads_client.get_service("AdGroupCriterionService")

        operations = []
        for crit_id in request.criterion_ids:
            op = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
            op.remove = f"customers/{customer_id}/adGroupCriteria/{request.ad_group_id}~{crit_id}"
            operations.append(op)

        response = ag_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id,
            operations=operations
        )
        _track_api_call('MUTATE_LISTING_GROUP_REMOVE')

        return {
            "status": "success",
            "message": f"Removed {len(request.criterion_ids)} listing group nodes",
            "removed_criterion_ids": request.criterion_ids,
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to remove listing group nodes: {str(e)}"}


@mcp.tool()
async def google_ads_rebuild_shopping_listing_group_tree(request: RebuildShoppingListingGroupTreeRequest) -> dict:
    """
    Atomically rebuild the entire listing group tree in a Shopping ad group.

    Safe way to restructure product groups. Removes existing tree and creates
    new one in single atomic operation.

    Supports 1-level trees (root → products) and 2-level trees
    (root → first level → second level).
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ag_criterion_service = _gam.google_ads_client.get_service("AdGroupCriterionService")
        ga_service = _gam.google_ads_client.get_service("GoogleAdsService")

        dimension_map = {
            "custom_label_0": ("product_custom_attribute", "INDEX0"),
            "custom_label_1": ("product_custom_attribute", "INDEX1"),
            "custom_label_2": ("product_custom_attribute", "INDEX2"),
            "custom_label_3": ("product_custom_attribute", "INDEX3"),
            "custom_label_4": ("product_custom_attribute", "INDEX4"),
            "product_type": ("product_type", "LEVEL1"),
            "brand": ("product_brand", None),
        }

        if request.root_dimension not in dimension_map:
            return {"status": "error", "message": f"Unknown dimension: {request.root_dimension}"}

        # Step 1: Find and remove ALL existing listing group criteria
        query = (
            f"SELECT ad_group_criterion.criterion_id, ad_group_criterion.listing_group.type "
            f"FROM ad_group_criterion "
            f"WHERE ad_group.id = {request.ad_group_id} "
            f"AND ad_group_criterion.type = 'LISTING_GROUP' "
            f"AND ad_group_criterion.status != 'REMOVED'"
        )

        search_request = _gam.google_ads_client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = customer_id
        search_request.query = query
        search_request.page_size = 1000

        existing_criteria = []
        stream = ga_service.search_stream(request=search_request)
        for batch in stream:
            for row in batch.results:
                existing_criteria.append({
                    "id": str(row.ad_group_criterion.criterion_id),
                    "type": row.ad_group_criterion.listing_group.type_.name,
                })
        _track_api_call('GAQL_LISTING_GROUPS')

        # Build all operations: removes first, then creates
        all_operations = []

        # Remove existing
        for crit in existing_criteria:
            op = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
            op.remove = f"customers/{customer_id}/adGroupCriteria/{request.ad_group_id}~{crit['id']}"
            all_operations.append(op)

        removed_count = len(all_operations)

        # Helper to set case_value on a criterion
        def _set_case_value(criterion, dimension_name, value):
            dim_field, dim_index = dimension_map[dimension_name]
            if dim_field == "product_custom_attribute":
                criterion.listing_group.case_value.product_custom_attribute.index = (
                    getattr(_gam.google_ads_client.enums.ListingCustomAttributeIndexEnum, dim_index)
                )
                criterion.listing_group.case_value.product_custom_attribute.value = value
            elif dim_field == "product_brand":
                criterion.listing_group.case_value.product_brand.value = value
            elif dim_field == "product_type":
                criterion.listing_group.case_value.product_type.value = value
                criterion.listing_group.case_value.product_type.level = (
                    _gam.google_ads_client.enums.ProductTypeLevelEnum.LEVEL1
                )

        # Use temporary resource names for the new tree
        temp_id_counter = [-1]

        def _next_temp_id():
            temp_id_counter[0] -= 1
            return temp_id_counter[0]

        # Create new ROOT node (SUBDIVISION, no case_value)
        root_temp_id = _next_temp_id()
        op_root = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
        crit_root = op_root.create
        crit_root.ad_group = ag_criterion_service.ad_group_path(customer_id, request.ad_group_id)
        crit_root.status = _gam.google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
        crit_root.cpc_bid_micros = 10000
        crit_root.listing_group.type_ = _gam.google_ads_client.enums.ListingGroupTypeEnum.SUBDIVISION
        crit_root.resource_name = f"customers/{customer_id}/adGroupCriteria/{request.ad_group_id}~{root_temp_id}"
        all_operations.append(op_root)

        root_resource = crit_root.resource_name
        created_nodes = []

        for group in request.groups:
            value = group.get("value", "")
            subdivision = group.get("subdivision")

            if subdivision:
                # This value becomes a SUBDIVISION node (2-level tree)
                sub_temp_id = _next_temp_id()
                op_sub = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
                crit_sub = op_sub.create
                crit_sub.ad_group = ag_criterion_service.ad_group_path(customer_id, request.ad_group_id)
                crit_sub.status = _gam.google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
                crit_sub.cpc_bid_micros = 10000
                crit_sub.listing_group.type_ = _gam.google_ads_client.enums.ListingGroupTypeEnum.SUBDIVISION
                crit_sub.listing_group.parent_ad_group_criterion = root_resource
                _set_case_value(crit_sub, request.root_dimension, value)
                crit_sub.resource_name = f"customers/{customer_id}/adGroupCriteria/{request.ad_group_id}~{sub_temp_id}"
                all_operations.append(op_sub)

                sub_resource = crit_sub.resource_name
                sub_dim = subdivision.get("dimension", "custom_label_0")
                sub_children = subdivision.get("children", [])
                sub_else_bid = subdivision.get("everything_else_bid_micros", 10000)

                # Create child UNIT nodes under this subdivision
                for child in sub_children:
                    child_val = child.get("value", "")
                    child_bid = child.get("cpc_bid_micros", 10000)

                    op_child = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
                    crit_child = op_child.create
                    crit_child.ad_group = ag_criterion_service.ad_group_path(customer_id, request.ad_group_id)
                    crit_child.status = _gam.google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
                    crit_child.cpc_bid_micros = child_bid
                    crit_child.listing_group.type_ = _gam.google_ads_client.enums.ListingGroupTypeEnum.UNIT
                    crit_child.listing_group.parent_ad_group_criterion = sub_resource
                    _set_case_value(crit_child, sub_dim, child_val)
                    all_operations.append(op_child)
                    created_nodes.append({"parent": value, "value": child_val, "bid": _from_micros(child_bid)})

                # 'Everything else' for sub-level
                op_sub_else = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
                crit_sub_else = op_sub_else.create
                crit_sub_else.ad_group = ag_criterion_service.ad_group_path(customer_id, request.ad_group_id)
                crit_sub_else.status = _gam.google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
                crit_sub_else.cpc_bid_micros = sub_else_bid
                crit_sub_else.listing_group.type_ = _gam.google_ads_client.enums.ListingGroupTypeEnum.UNIT
                crit_sub_else.listing_group.parent_ad_group_criterion = sub_resource
                all_operations.append(op_sub_else)
            else:
                # Simple UNIT node at root level
                bid = group.get("cpc_bid_micros", 10000)
                op_unit = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
                crit_unit = op_unit.create
                crit_unit.ad_group = ag_criterion_service.ad_group_path(customer_id, request.ad_group_id)
                crit_unit.status = _gam.google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
                crit_unit.cpc_bid_micros = bid
                crit_unit.listing_group.type_ = _gam.google_ads_client.enums.ListingGroupTypeEnum.UNIT
                crit_unit.listing_group.parent_ad_group_criterion = root_resource
                _set_case_value(crit_unit, request.root_dimension, value)
                all_operations.append(op_unit)
                created_nodes.append({"value": value, "bid": _from_micros(bid)})

        # Root-level 'Everything else' catch-all
        op_root_else = _gam.google_ads_client.get_type("AdGroupCriterionOperation")
        crit_root_else = op_root_else.create
        crit_root_else.ad_group = ag_criterion_service.ad_group_path(customer_id, request.ad_group_id)
        crit_root_else.status = _gam.google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
        crit_root_else.cpc_bid_micros = request.everything_else_bid_micros
        crit_root_else.listing_group.type_ = _gam.google_ads_client.enums.ListingGroupTypeEnum.UNIT
        crit_root_else.listing_group.parent_ad_group_criterion = root_resource
        all_operations.append(op_root_else)

        # Execute ALL operations atomically
        response = ag_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id,
            operations=all_operations
        )
        _track_api_call('MUTATE_LISTING_GROUP_REBUILD')

        result_ids = [r.resource_name.split("~")[-1] for r in response.results]

        return {
            "status": "success",
            "message": f"Rebuilt tree: removed {removed_count} old, created {len(all_operations) - removed_count} new",
            "removed_count": removed_count,
            "created_nodes": created_nodes,
            "root_dimension": request.root_dimension,
            "everything_else_bid": _from_micros(request.everything_else_bid_micros),
            "new_criterion_ids": result_ids[removed_count:],
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to rebuild listing group tree: {str(e)}"}


@mcp.tool()
async def google_ads_create_listing_group_filter(request: CreateAssetGroupListingFilterRequest) -> dict:
    """
    Create a product listing filter for an asset group.

    Listing group filters determine which products from Merchant Center feed
    are eligible for ads in this asset group.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)

        listing_group_filter_service = _gam.google_ads_client.get_service(
            "AssetGroupListingGroupFilterService"
        )

        asset_group_service = _gam.google_ads_client.get_service("AssetGroupService")
        asset_group_resource_name = asset_group_service.asset_group_path(
            customer_id, request.asset_group_id
        )

        # Create operation
        operation = _gam.google_ads_client.get_type("AssetGroupListingGroupFilterOperation")
        listing_group_filter = operation.create

        listing_group_filter.asset_group = asset_group_resource_name
        listing_group_filter.vertical = _gam.google_ads_client.enums.ListingGroupFilterVerticalEnum.SHOPPING
        listing_group_filter.type_ = _gam.google_ads_client.enums.ListingGroupFilterTypeEnum.UNIT_INCLUDED

        # If ALL_PRODUCTS, we're done with the base filter
        if request.filter_type == "ALL_PRODUCTS":
            response = listing_group_filter_service.mutate_asset_group_listing_group_filters(
                customer_id=customer_id,
                operations=[operation]
            )
            _track_api_call('MUTATE_LISTING_GROUP')

            filter_resource_name = response.results[0].resource_name

            return {
                "status": "success",
                "message": "Product listing filter (ALL_PRODUCTS) created",
                "asset_group_id": request.asset_group_id,
                "filter_resource_name": filter_resource_name,
                "filter_type": "ALL_PRODUCTS"
            }

        # For CUSTOM filters, set the dimensions
        if request.filter_type == "CUSTOM":
            if request.product_brand:
                listing_group_filter.case_value.product_brand.value = request.product_brand

            if request.product_type:
                listing_group_filter.case_value.product_type.value = request.product_type
                listing_group_filter.case_value.product_type.level = _gam.google_ads_client.enums.ListingGroupFilterProductTypeLevelEnum.LEVEL1

            if request.custom_label_0:
                listing_group_filter.case_value.product_custom_attribute.index = (
                    _gam.google_ads_client.enums.ListingGroupFilterCustomAttributeIndexEnum.INDEX0
                )
                listing_group_filter.case_value.product_custom_attribute.value = request.custom_label_0

        # Perform the mutation
        response = listing_group_filter_service.mutate_asset_group_listing_group_filters(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_LISTING_GROUP')

        filter_resource_name = response.results[0].resource_name

        return {
            "status": "success",
            "message": f"Product listing filter ({request.filter_type}) created",
            "asset_group_id": request.asset_group_id,
            "filter_resource_name": filter_resource_name,
            "filter_type": request.filter_type
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create listing group filter: {str(e)}"
        }


@mcp.tool()
async def google_ads_get_shopping_performance(request: ShoppingPerformanceRequest) -> dict:
    """
    Get shopping performance metrics by product item ID.

    Returns product-level data from shopping_performance_view including
    impressions, clicks, CTR, cost, conversions, and conversion value.
    Essential for image A/B testing.
    """
    try:
        customer_id = _format_customer_id(request.customer_id)

        query = f"""
            SELECT
                segments.product_item_id,
                segments.product_title,
                metrics.impressions,
                metrics.clicks,
                metrics.ctr,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value
            FROM shopping_performance_view
            WHERE segments.date >= '{request.start_date}'
            AND segments.date <= '{request.end_date}'
            AND metrics.impressions >= {request.min_impressions}
        """

        if request.campaign_id:
            query += f" AND campaign.id = {request.campaign_id}"

        query += " ORDER BY metrics.impressions DESC"
        query += f" LIMIT {request.page_size}"

        results = _execute_gaql(customer_id, query, request.page_size)

        items = []
        for row in results:
            items.append({
                "item_id": _safe_get_value(row, "segments.product_item_id"),
                "title": _safe_get_value(row, "segments.product_title"),
                "impressions": _safe_get_value(row, "metrics.impressions", 0),
                "clicks": _safe_get_value(row, "metrics.clicks", 0),
                "ctr": _safe_get_value(row, "metrics.ctr", 0),
                "cost": _from_micros(_safe_get_value(row, "metrics.cost_micros", 0)),
                "conversions": _safe_get_value(row, "metrics.conversions", 0),
                "conversions_value": _safe_get_value(row, "metrics.conversions_value", 0)
            })

        return {
            "status": "success",
            "results": items,
            "count": len(items)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get shopping performance: {str(e)}"
        }


@mcp.tool()
async def google_ads_create_pmax_campaign(request: CreatePMaxCampaignRequest) -> dict:
    """
    Create a Performance Max campaign linked to Google Merchant Center.

    Performance Max campaigns automatically optimize across all Google channels
    (Search, Display, YouTube, Shopping) using machine learning.
    Automatically creates a dedicated budget. Uses Maximize Conversion Value bidding.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)

        # Services needed
        campaign_budget_service = _gam.google_ads_client.get_service("CampaignBudgetService")
        campaign_service = _gam.google_ads_client.get_service("CampaignService")

        # First create the budget
        budget_operation = _gam.google_ads_client.get_type("CampaignBudgetOperation")
        budget = budget_operation.create
        budget.name = f"{request.campaign_name} - PMax Budget"
        budget.amount_micros = request.daily_budget_micros
        budget.delivery_method = _gam.google_ads_client.enums.BudgetDeliveryMethodEnum.STANDARD

        budget_response = campaign_budget_service.mutate_campaign_budgets(
            customer_id=customer_id,
            operations=[budget_operation]
        )
        _track_api_call('MUTATE_BUDGET')

        budget_resource_name = budget_response.results[0].resource_name

        # Now create the campaign
        campaign_operation = _gam.google_ads_client.get_type("CampaignOperation")
        campaign = campaign_operation.create

        campaign.name = request.campaign_name
        campaign.advertising_channel_type = _gam.google_ads_client.enums.AdvertisingChannelTypeEnum.PERFORMANCE_MAX
        campaign.campaign_budget = budget_resource_name

        # Set status
        status_map = {
            "ENABLED": _gam.google_ads_client.enums.CampaignStatusEnum.ENABLED,
            "PAUSED": _gam.google_ads_client.enums.CampaignStatusEnum.PAUSED
        }
        campaign.status = status_map.get(request.status, _gam.google_ads_client.enums.CampaignStatusEnum.ENABLED)

        # Configure shopping settings (required for PMax with Merchant Center)
        campaign.shopping_setting.merchant_id = int(request.merchant_id)
        campaign.shopping_setting.feed_label = request.sales_country

        # Configure bidding strategy (maximize conversion value for e-commerce)
        campaign.maximize_conversion_value.target_roas = 0

        # Set final URL expansion
        campaign.url_expansion_opt_out = False

        # Perform the mutation
        response = campaign_service.mutate_campaigns(
            customer_id=customer_id,
            operations=[campaign_operation]
        )
        _track_api_call('MUTATE_CAMPAIGN')

        campaign_resource_name = response.results[0].resource_name
        campaign_id = campaign_resource_name.split("/")[-1]

        return {
            "status": "success",
            "message": f"Performance Max campaign '{request.campaign_name}' created",
            "campaign_id": campaign_id,
            "campaign_resource_name": campaign_resource_name,
            "budget_id": budget_resource_name.split("/")[-1],
            "daily_budget": _from_micros(request.daily_budget_micros),
            "advertising_channel": "PERFORMANCE_MAX"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create Performance Max campaign: {str(e)}"
        }
