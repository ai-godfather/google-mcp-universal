"""
Offline conversion upload module.

Handles Google Ads offline click conversion uploads for gclid, gbraid, and wbraid.
"""

from typing import Optional

from pydantic import BaseModel, Field, model_validator

import google_ads_mcp as _gam

from google_ads_mcp import (
    mcp,
    _ensure_client,
    _format_customer_id,
    _track_api_call,
)


class ClickConversionItem(BaseModel):
    """Single offline click conversion row."""

    gclid: Optional[str] = Field(None, description="Google Click ID")
    gbraid: Optional[str] = Field(None, description="GBRAID (iOS app/web-to-app)")
    wbraid: Optional[str] = Field(None, description="WBRAID (iOS web)")
    conversion_action: str = Field(
        ...,
        description="Full resource name customers/{cid}/conversionActions/{id} OR a bare numeric conversion action id"
    )
    conversion_date_time: str = Field(
        ...,
        description="Format 'YYYY-MM-DD HH:MM:SS+TZ' e.g. '2025-01-15 14:30:00+00:00' (Google requires offset)"
    )
    conversion_value: float = Field(..., description="Conversion value")
    currency_code: str = Field("USD", description="ISO 4217 currency code")
    order_id: Optional[str] = Field(None, description="Optional merchant order id for dedupe")

    @model_validator(mode="after")
    def validate_click_identifier(self):
        """Require at least one click identifier per row."""
        if self.gclid or self.gbraid or self.wbraid:
            return self
        raise ValueError("At least one of gclid, gbraid, or wbraid must be provided")


class UploadClickConversionsRequest(BaseModel):
    """Request model for uploading offline click conversions."""

    customer_id: str = Field(..., description="Customer ID")
    conversions: list[ClickConversionItem] = Field(..., description="Conversions to upload", min_length=1)


@mcp.tool()
async def google_ads_upload_click_conversions(request: UploadClickConversionsRequest) -> dict:
    """Upload offline click conversions (gclid/gbraid/wbraid) to Google Ads."""
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        conversion_upload_service = _gam.google_ads_client.get_service("ConversionUploadService")
        conversion_action_service = _gam.google_ads_client.get_service("ConversionActionService")

        conversions = []
        for row in request.conversions:
            click_conversion = _gam.google_ads_client.get_type("ClickConversion")

            if row.gclid:
                click_conversion.gclid = row.gclid
            elif row.gbraid:
                click_conversion.gbraid = row.gbraid
            else:
                click_conversion.wbraid = row.wbraid

            if "/" in row.conversion_action:
                click_conversion.conversion_action = row.conversion_action
            else:
                click_conversion.conversion_action = conversion_action_service.conversion_action_path(
                    customer_id, row.conversion_action
                )

            click_conversion.conversion_date_time = row.conversion_date_time
            click_conversion.conversion_value = row.conversion_value
            click_conversion.currency_code = row.currency_code

            if row.order_id:
                click_conversion.order_id = row.order_id

            conversions.append(click_conversion)

        response = conversion_upload_service.upload_click_conversions(
            customer_id=customer_id,
            conversions=conversions,
            partial_failure=True
        )
        _track_api_call('UPLOAD_CLICK_CONVERSIONS', len(conversions))

        errors_by_row = {}
        failure = response.partial_failure_error
        if failure and getattr(failure, "code", 0) != 0:
            failure_type = _gam.google_ads_client.get_type("GoogleAdsFailure")
            for detail in failure.details:
                ga_failure = type(failure_type).deserialize(detail.value)
                for err in ga_failure.errors:
                    idx = None
                    if err.location:
                        for field_path_element in err.location.field_path_elements:
                            if field_path_element.field_name == "operations":
                                idx = field_path_element.index
                    errors_by_row.setdefault(idx, []).append(err.message)

        failed_rows = set(errors_by_row.keys())

        return {
            "status": "success",
            "uploaded_count": len(request.conversions) - len(failed_rows),
            "failed_count": len(failed_rows),
            "errors": [
                {"row": idx, "messages": msgs}
                for idx, msgs in sorted(
                    errors_by_row.items(),
                    key=lambda item: (item[0] is None, item[0] if item[0] is not None else -1)
                )
            ],
            "total_submitted": len(request.conversions),
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to upload click conversions: {str(e)}"
        }
