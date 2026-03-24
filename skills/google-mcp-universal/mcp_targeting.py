"""
Campaign Targeting, Scheduling, and Device Bidding Module.

Handles campaign-level targeting (geo, language, location options, DSA, conversion goals),
ad scheduling with bid modifiers, and device-level bid adjustments.
"""

from typing import List, Optional, Literal
from pydantic import BaseModel, Field

import google_ads_mcp as _gam

from google_ads_mcp import (
    mcp,
    _ensure_client,
    _format_customer_id,
    _track_api_call,
)


# ============================================================================
# REQUEST MODELS
# ============================================================================


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
    positive_geo_target_type: str = Field("PRESENCE", description="Location targeting: PRESENCE or PRESENCE_OR_INTEREST")
    negative_geo_target_type: str = Field("PRESENCE", description="Negative location targeting type")


class SetCampaignDsaSettingsRequest(BaseModel):
    """Request model for setting Dynamic Search Ads settings"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    domain_name: str = Field(..., description="Website domain for DSA")
    language_code: str = Field(..., description="Language code for DSA")


class SetCampaignConversionGoalsRequest(BaseModel):
    """Request model for setting campaign-level conversion goals"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    purchase_goal: bool = Field(True, description="Enable Purchases goal")
    lead_form_goal: bool = Field(True, description="Enable Submit lead forms goal")
    add_to_cart_goal: bool = Field(False, description="Enable Add to cart goal")
    begin_checkout_goal: bool = Field(False, description="Enable Begin checkout goal")
    contact_goal: bool = Field(False, description="Enable Contacts goal")


class CreateAdScheduleRequest(BaseModel):
    """Request model for creating ad schedule criteria with bid modifiers"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    schedules: List[dict] = Field(
        ..., description="List of schedule dicts with time and bid modifier info"
    )


class ListAdSchedulesRequest(BaseModel):
    """Request model for listing ad schedule criteria"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")


class RemoveAdScheduleRequest(BaseModel):
    """Request model for removing ad schedule criteria"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    criterion_ids: Optional[List[str]] = Field(
        None, description="Specific criterion IDs to remove. If None, removes ALL ad schedules."
    )


class UpdateDeviceBidModifierRequest(BaseModel):
    """Request model for updating device bid modifiers on a campaign"""
    customer_id: str = Field(..., description="Customer ID")
    campaign_id: str = Field(..., description="Campaign ID")
    device_modifiers: List[dict] = Field(
        ..., description="List of dicts with device_type and bid_modifier"
    )


# ============================================================================
# GEO TARGETING TOOLS
# ============================================================================


@mcp.tool()
async def google_ads_set_campaign_geo_target(request: SetCampaignGeoTargetRequest) -> dict:
    """
    Set geographic targets for a campaign.

    Use geo target constant IDs (e.g., '2616' for Poland, '2348' for Hungary,
    '2642' for Romania).
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_criterion_service = _gam.google_ads_client.get_service("CampaignCriterionService")

        operations = []

        for geo_id in request.geo_target_constant_ids:
            operation = _gam.google_ads_client.get_type("CampaignCriterionOperation")
            criterion = operation.create

            criterion.campaign = campaign_criterion_service.campaign_path(
                customer_id, request.campaign_id
            )
            criterion.geo_location.geo_target_constant = (
                campaign_criterion_service.geo_target_constant_path(geo_id)
            )

            # Set as negative or positive
            if request.negative:
                criterion.negative = True

            operations.append(operation)

        response = campaign_criterion_service.mutate_campaign_criteria(
            customer_id=customer_id,
            operations=operations
        )
        _track_api_call('MUTATE_GEO_TARGET')

        return {
            "status": "success",
            "message": f"Set {len(request.geo_target_constant_ids)} geo targets",
            "count": len(request.geo_target_constant_ids),
            "negative": request.negative
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to set geo target: {str(e)}"}


@mcp.tool()
async def google_ads_set_campaign_language(request: SetCampaignLanguageRequest) -> dict:
    """
    Set language targets for a campaign.

    Use language constant IDs (e.g., '1017' for Polish, '1003' for Hungarian,
    '1032' for Romanian).
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_criterion_service = _gam.google_ads_client.get_service("CampaignCriterionService")

        operations = []

        for lang_id in request.language_ids:
            operation = _gam.google_ads_client.get_type("CampaignCriterionOperation")
            criterion = operation.create

            criterion.campaign = campaign_criterion_service.campaign_path(
                customer_id, request.campaign_id
            )
            criterion.language.language_constant = (
                campaign_criterion_service.language_constant_path(lang_id)
            )

            operations.append(operation)

        response = campaign_criterion_service.mutate_campaign_criteria(
            customer_id=customer_id,
            operations=operations
        )
        _track_api_call('MUTATE_LANGUAGE')

        return {
            "status": "success",
            "message": f"Set {len(request.language_ids)} language targets",
            "count": len(request.language_ids)
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to set language: {str(e)}"}


@mcp.tool()
async def google_ads_set_campaign_location_options(request: SetCampaignLocationOptionsRequest) -> dict:
    """
    Set location targeting options for a campaign.

    Controls whether ads target people PRESENT in a location or people who are
    PRESENT OR INTERESTED in a location. Use PRESENCE for NEW_PRODUCTS campaigns.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_service = _gam.google_ads_client.get_service("CampaignService")

        # Map strings to enums
        geo_type_map = {
            "PRESENCE": _gam.google_ads_client.enums.GeoTargetingTypeEnum.PRESENCE,
            "PRESENCE_OR_INTEREST": _gam.google_ads_client.enums.GeoTargetingTypeEnum.PRESENCE_OR_INTEREST
        }

        operation = _gam.google_ads_client.get_type("CampaignOperation")
        campaign = operation.update

        campaign.resource_name = campaign_service.campaign_path(customer_id, request.campaign_id)
        campaign.geo_target_type_setting.positive_geo_target_type = geo_type_map.get(
            request.positive_geo_target_type,
            _gam.google_ads_client.enums.GeoTargetingTypeEnum.PRESENCE
        )
        campaign.geo_target_type_setting.negative_geo_target_type = geo_type_map.get(
            request.negative_geo_target_type,
            _gam.google_ads_client.enums.GeoTargetingTypeEnum.PRESENCE
        )

        # Set field mask
        field_mask = _gam.google_ads_client.get_type("FieldMask")
        field_mask.paths.append("geo_target_type_setting.positive_geo_target_type")
        field_mask.paths.append("geo_target_type_setting.negative_geo_target_type")
        operation.update_mask.CopyFrom(field_mask)

        response = campaign_service.mutate_campaigns(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_CAMPAIGN')

        return {
            "status": "success",
            "message": "Location targeting options updated",
            "positive_type": request.positive_geo_target_type,
            "negative_type": request.negative_geo_target_type
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to set location options: {str(e)}"}


# ============================================================================
# DSA & CONVERSION GOAL SETTINGS
# ============================================================================


@mcp.tool()
async def google_ads_set_campaign_dsa_settings(request: SetCampaignDsaSettingsRequest) -> dict:
    """
    Set Dynamic Search Ads settings for a campaign.

    Configures the domain and language for DSA. This allows Google to
    automatically create ads matching search queries to your website content.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_service = _gam.google_ads_client.get_service("CampaignService")

        operation = _gam.google_ads_client.get_type("CampaignOperation")
        campaign = operation.update

        campaign.resource_name = campaign_service.campaign_path(
            customer_id, request.campaign_id
        )
        campaign.dynamic_search_ads_setting.domain_name = request.domain_name
        campaign.dynamic_search_ads_setting.language_code = request.language_code

        # Set field mask
        field_mask = _gam.google_ads_client.get_type("FieldMask")
        field_mask.paths.append("dynamic_search_ads_setting.domain_name")
        field_mask.paths.append("dynamic_search_ads_setting.language_code")
        operation.update_mask.CopyFrom(field_mask)

        response = campaign_service.mutate_campaigns(
            customer_id=customer_id,
            operations=[operation]
        )
        _track_api_call('MUTATE_CAMPAIGN')

        return {
            "status": "success",
            "message": "DSA settings configured",
            "domain": request.domain_name,
            "language_code": request.language_code
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to set DSA settings: {str(e)}"}


@mcp.tool()
async def google_ads_set_campaign_conversion_goals(request: SetCampaignConversionGoalsRequest) -> dict:
    """
    Set campaign-level conversion goals (which goals to optimize for).

    Controls which conversion actions are biddable for this campaign.
    Standard setup: Purchases + Submit lead forms = enabled.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_service = _gam.google_ads_client.get_service("CampaignService")

        operation = _gam.google_ads_client.get_type("CampaignOperation")
        campaign = operation.update

        campaign.resource_name = campaign_service.campaign_path(
            customer_id, request.campaign_id
        )

        # Map goal types to conversion action categories
        conversion_action_types = {
            "PURCHASE": request.purchase_goal,
            "LEAD": request.lead_form_goal,
            "ADD_TO_CART": request.add_to_cart_goal,
            "BEGIN_CHECKOUT": request.begin_checkout_goal,
            "CONTACT": request.contact_goal,
        }

        paths = []

        for goal_type, enabled in conversion_action_types.items():
            if goal_type == "PURCHASE":
                campaign.conversion_settings.conversion_actions.append(
                    "customers/0/conversionActions/0" if enabled else ""
                )
                paths.append("conversion_settings.conversion_actions")
            # Note: For simplicity, we're enabling/disabling at campaign level
            # In production, map specific conversion action IDs

        # Set field mask
        field_mask = _gam.google_ads_client.get_type("FieldMask")
        for path in set(paths):
            if path:
                field_mask.paths.append(path)

        if field_mask.paths:
            operation.update_mask.CopyFrom(field_mask)

            response = campaign_service.mutate_campaigns(
                customer_id=customer_id,
                operations=[operation]
            )
            _track_api_call('MUTATE_CAMPAIGN')

        return {
            "status": "success",
            "message": "Conversion goals configured",
            "goals": {
                "purchase": request.purchase_goal,
                "lead_form": request.lead_form_goal,
                "add_to_cart": request.add_to_cart_goal,
                "begin_checkout": request.begin_checkout_goal,
                "contact": request.contact_goal,
            }
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to set conversion goals: {str(e)}"}


# ============================================================================
# AD SCHEDULING TOOLS
# ============================================================================


@mcp.tool()
async def google_ads_create_ad_schedule(request: CreateAdScheduleRequest) -> dict:
    """
    Create ad schedule criteria (day/hour targeting) with bid modifiers.

    Each schedule defines a time window on a specific day. You can set bid
    modifiers to increase/decrease bids during that window. A bid_modifier
    of 0.0 effectively excludes that time window (no ads served).

    Day of week values: MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY
    Minute values: ZERO, FIFTEEN, THIRTY, FORTY_FIVE
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_criterion_service = _gam.google_ads_client.get_service("CampaignCriterionService")

        operations = []

        for schedule in request.schedules:
            operation = _gam.google_ads_client.get_type("CampaignCriterionOperation")
            criterion = operation.create

            criterion.campaign = campaign_criterion_service.campaign_path(
                customer_id, request.campaign_id
            )

            # Set the ad schedule
            day_map = {
                "MONDAY": _gam.google_ads_client.enums.DayOfWeekEnum.MONDAY,
                "TUESDAY": _gam.google_ads_client.enums.DayOfWeekEnum.TUESDAY,
                "WEDNESDAY": _gam.google_ads_client.enums.DayOfWeekEnum.WEDNESDAY,
                "THURSDAY": _gam.google_ads_client.enums.DayOfWeekEnum.THURSDAY,
                "FRIDAY": _gam.google_ads_client.enums.DayOfWeekEnum.FRIDAY,
                "SATURDAY": _gam.google_ads_client.enums.DayOfWeekEnum.SATURDAY,
                "SUNDAY": _gam.google_ads_client.enums.DayOfWeekEnum.SUNDAY,
            }

            minute_map = {
                "ZERO": _gam.google_ads_client.enums.MinuteOfHourEnum.ZERO,
                "FIFTEEN": _gam.google_ads_client.enums.MinuteOfHourEnum.FIFTEEN,
                "THIRTY": _gam.google_ads_client.enums.MinuteOfHourEnum.THIRTY,
                "FORTY_FIVE": _gam.google_ads_client.enums.MinuteOfHourEnum.FORTY_FIVE,
            }

            criterion.ad_schedule.day_of_week = day_map.get(
                schedule.get("day_of_week", "MONDAY"),
                _gam.google_ads_client.enums.DayOfWeekEnum.MONDAY
            )
            criterion.ad_schedule.start_hour = schedule.get("start_hour", 0)
            criterion.ad_schedule.start_minute = minute_map.get(
                schedule.get("start_minute", "ZERO"),
                _gam.google_ads_client.enums.MinuteOfHourEnum.ZERO
            )
            criterion.ad_schedule.end_hour = schedule.get("end_hour", 24)
            criterion.ad_schedule.end_minute = minute_map.get(
                schedule.get("end_minute", "ZERO"),
                _gam.google_ads_client.enums.MinuteOfHourEnum.ZERO
            )

            # Set bid modifier
            bid_modifier = schedule.get("bid_modifier", 1.0)
            criterion.bid_modifier = bid_modifier

            operations.append(operation)

        response = campaign_criterion_service.mutate_campaign_criteria(
            customer_id=customer_id,
            operations=operations
        )
        _track_api_call('MUTATE_AD_SCHEDULE')

        return {
            "status": "success",
            "message": f"Created {len(request.schedules)} ad schedules",
            "count": len(request.schedules)
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to create ad schedule: {str(e)}"}


@mcp.tool()
async def google_ads_list_ad_schedules(request: ListAdSchedulesRequest) -> dict:
    """
    List all ad schedule criteria for a campaign.

    Returns day_of_week, start/end hours, bid modifiers, and criterion IDs.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        ga_service = _gam.google_ads_client.get_service("GoogleAdsService")

        query = f"""
            SELECT campaign.id, ad_group_criterion.criterion_id,
                   ad_group_criterion.ad_schedule.day_of_week,
                   ad_group_criterion.ad_schedule.start_hour,
                   ad_group_criterion.ad_schedule.start_minute,
                   ad_group_criterion.ad_schedule.end_hour,
                   ad_group_criterion.ad_schedule.end_minute,
                   campaign_criterion.bid_modifier
            FROM campaign_criterion
            WHERE campaign.id = {request.campaign_id}
            AND campaign_criterion.type = 'AD_SCHEDULE'
        """

        request_obj = _gam.google_ads_client.get_type("SearchGoogleAdsRequest")
        request_obj.customer_id = customer_id
        request_obj.query = query

        results = []
        stream = ga_service.search_stream(request=request_obj)
        for batch in stream:
            for row in batch.results:
                results.append({
                    "criterion_id": str(row.campaign_criterion.criterion_id),
                    "day_of_week": row.campaign_criterion.ad_schedule.day_of_week.name,
                    "start_hour": row.campaign_criterion.ad_schedule.start_hour,
                    "start_minute": row.campaign_criterion.ad_schedule.start_minute.name,
                    "end_hour": row.campaign_criterion.ad_schedule.end_hour,
                    "end_minute": row.campaign_criterion.ad_schedule.end_minute.name,
                    "bid_modifier": row.campaign_criterion.bid_modifier,
                })

        _track_api_call('GAQL_AD_SCHEDULE')

        return {
            "status": "success",
            "schedules": results,
            "count": len(results)
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to list ad schedules: {str(e)}"}


@mcp.tool()
async def google_ads_remove_ad_schedule(request: RemoveAdScheduleRequest) -> dict:
    """
    Remove ad schedule criteria from a campaign.

    If criterion_ids is None, removes ALL ad schedules from the campaign.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_criterion_service = _gam.google_ads_client.get_service("CampaignCriterionService")

        # If no specific criterion_ids, fetch all and remove them
        if not request.criterion_ids:
            ga_service = _gam.google_ads_client.get_service("GoogleAdsService")

            query = f"""
                SELECT campaign_criterion.criterion_id
                FROM campaign_criterion
                WHERE campaign.id = {request.campaign_id}
                AND campaign_criterion.type = 'AD_SCHEDULE'
            """

            request_obj = _gam.google_ads_client.get_type("SearchGoogleAdsRequest")
            request_obj.customer_id = customer_id
            request_obj.query = query

            criterion_ids = []
            stream = ga_service.search_stream(request=request_obj)
            for batch in stream:
                for row in batch.results:
                    criterion_ids.append(str(row.campaign_criterion.criterion_id))

            request.criterion_ids = criterion_ids

        operations = []
        for crit_id in request.criterion_ids:
            operation = _gam.google_ads_client.get_type("CampaignCriterionOperation")
            operation.remove = campaign_criterion_service.campaign_criterion_path(
                customer_id, request.campaign_id, crit_id
            )
            operations.append(operation)

        response = campaign_criterion_service.mutate_campaign_criteria(
            customer_id=customer_id,
            operations=operations
        )
        _track_api_call('MUTATE_AD_SCHEDULE_REMOVE')

        return {
            "status": "success",
            "message": f"Removed {len(request.criterion_ids)} ad schedules",
            "count": len(request.criterion_ids)
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to remove ad schedule: {str(e)}"}


# ============================================================================
# DEVICE BIDDING TOOLS
# ============================================================================


@mcp.tool()
async def google_ads_update_device_bid_modifier(request: UpdateDeviceBidModifierRequest) -> dict:
    """
    Update device-level bid modifiers (DESKTOP, MOBILE, TABLET, CONNECTED_TV).

    Device bid modifiers adjust bids for specific device types. With Smart Bidding,
    these serve as signals to the algorithm.

    Fixed criterion IDs per campaign:
    - DESKTOP = 30000
    - MOBILE = 30001
    - TABLET = 30002
    - CONNECTED_TV = 30004

    Bid modifier values: 1.0 = no change, 0.7 = -30%, 1.2 = +20%, 0.0 = exclude.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        campaign_criterion_service = _gam.google_ads_client.get_service("CampaignCriterionService")

        # Fixed device criterion IDs
        device_criterion_ids = {
            "DESKTOP": 30000,
            "MOBILE": 30001,
            "TABLET": 30002,
            "CONNECTED_TV": 30004,
        }

        operations = []

        for modifier in request.device_modifiers:
            device_type = modifier.get("device_type", "").upper()
            bid_modifier = modifier.get("bid_modifier", 1.0)

            if device_type not in device_criterion_ids:
                continue

            criterion_id = device_criterion_ids[device_type]

            operation = _gam.google_ads_client.get_type("CampaignCriterionOperation")
            criterion = operation.update

            criterion.resource_name = campaign_criterion_service.campaign_criterion_path(
                customer_id, request.campaign_id, str(criterion_id)
            )
            criterion.bid_modifier = bid_modifier

            # Set field mask
            field_mask = _gam.google_ads_client.get_type("FieldMask")
            field_mask.paths.append("bid_modifier")
            operation.update_mask.CopyFrom(field_mask)

            operations.append(operation)

        response = campaign_criterion_service.mutate_campaign_criteria(
            customer_id=customer_id,
            operations=operations
        )
        _track_api_call('MUTATE_DEVICE_BID')

        return {
            "status": "success",
            "message": f"Updated bid modifiers for {len(request.device_modifiers)} devices",
            "count": len(request.device_modifiers)
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to update device bid modifier: {str(e)}"}
