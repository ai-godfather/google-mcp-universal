"""Bounded, atomic delivery of reviewed PAUSED Search campaigns and exact goals."""
from typing import Literal
from urllib.parse import urlsplit
from pydantic import BaseModel, Field, ConfigDict, model_validator
from google.ads.googleads.errors import GoogleAdsException
import re
import google_ads_mcp as gam
from accounts_config import load_config

# Naming and destination rules come from config.json "search_launch" (see config.example.json).
# Defaults: "<Prefix> | SEARCH | <CC> | <shop domain> | <YYYY-MM>", Shopify product URLs, no blocked families.
_DEFAULT_NAME_PATTERN = r'^[A-Za-z0-9 _-]{1,40} \| SEARCH \| [A-Z]{2} \| [a-z0-9.-]+ \| \d{4}-\d{2}$'
_HOST_PATTERN = r'^[a-z0-9-]+(\.[a-z0-9-]+)+$'  # length is capped by max_length=253 on the fields

def _launch_config() -> dict:
    return load_config().get('search_launch', {}) or {}

def _name_pattern() -> str:
    return _launch_config().get('campaign_name_pattern') or _DEFAULT_NAME_PATTERN

def _in_scope(campaign_name: str) -> bool:
    """Reviewed-delivery tools only touch campaigns that follow the configured naming pattern."""
    return re.fullmatch(_name_pattern(), campaign_name) is not None

class StrictInput(BaseModel):
    model_config = ConfigDict(extra='forbid')

class ReviewedKeyword(StrictInput):
    text: str = Field(min_length=1, max_length=80)
    match: Literal['EXACT', 'PHRASE']

class ReviewedSearchGroup(StrictInput):
    name: str = Field(min_length=1, max_length=255)
    final_url: str
    path1: str = Field(max_length=15)
    path2: str = Field(max_length=15)
    headlines: list[str] = Field(min_length=15, max_length=15)
    descriptions: list[str] = Field(min_length=4, max_length=4)
    keywords: list[ReviewedKeyword] = Field(min_length=1, max_length=40)

    @model_validator(mode='after')
    def validate_text(self):
        if any(not x.strip() or len(x)>30 for x in self.headlines):
            raise ValueError('headline_length')
        if len(set(self.headlines))!=15:
            raise ValueError('duplicate_headline')
        if any(not x.strip() or len(x)>90 for x in self.descriptions):
            raise ValueError('description_length')
        if len({(x.text,x.match) for x in self.keywords})!=len(self.keywords):
            raise ValueError('duplicate_keyword')
        cfg=_launch_config();name=self.name.lower()
        blocked=tuple(x.lower() for x in cfg.get('blocked_product_prefixes',[]))
        allowed=tuple(x.lower() for x in cfg.get('allowed_product_prefixes',[]))
        if blocked and name.startswith(blocked) and not (allowed and name.startswith(allowed)):
            raise ValueError('blocked_family')
        return self

class ReviewedSearchRequest(StrictInput):
    customer_id: str = Field(pattern=r'^\d{10}$')
    expected_currency: str = Field(pattern=r'^[A-Z]{3}$')
    campaign_name: str = Field(min_length=1, max_length=255)
    shop_domain: str = Field(pattern=_HOST_PATTERN, max_length=253)
    daily_budget_micros: int = Field(ge=10000, le=10000000)
    cpc_bid_micros: int = Field(ge=1, le=1000000)
    geo_id: str = Field(pattern=r'^\d+$')
    language_id: str = Field(pattern=r'^\d+$')
    groups: list[ReviewedSearchGroup] = Field(min_length=1, max_length=9)
    validate_only: bool = True

    @model_validator(mode='after')
    def same_destination(self):
        if len({g.name for g in self.groups})!=len(self.groups):raise ValueError('duplicate_group')
        if not _in_scope(self.campaign_name):raise ValueError('campaign_name_pattern')
        if f' | {self.shop_domain} | ' not in self.campaign_name:raise ValueError('campaign_domain')
        prefix=_launch_config().get('product_path_prefix','/products/')
        for g in self.groups:
            u=urlsplit(g.final_url)
            if u.scheme!='https' or u.hostname!=self.shop_domain or u.username or u.password or u.query or u.fragment or u.path!=prefix+g.name:
                raise ValueError('exact_product_url_required')
        return self

def _search(client, customer, query):
    result=list(client.get_service('GoogleAdsService').search(customer_id=customer,query=query,timeout=45))
    gam._track_api_call('GAQL_SEARCH_DELIVERY')
    return result

def build_search_operations(client, r):
    """Pure operation builder; no calls or side effects."""
    cid=r.customer_id;ops=[];metadata=[]
    def new(kind, meta):
        op=client.get_type('MutateOperation');ops.append(op);metadata.append(meta)
        return getattr(op,kind+'_operation').create
    budget=new('campaign_budget',{'kind':'budget'});budget.resource_name=f'customers/{cid}/campaignBudgets/-1';budget.name=r.campaign_name+' Budget';budget.amount_micros=r.daily_budget_micros;budget.explicitly_shared=False;budget.delivery_method=client.enums.BudgetDeliveryMethodEnum.STANDARD
    campaign=new('campaign',{'kind':'campaign'});campaign.resource_name=f'customers/{cid}/campaigns/-2';campaign.name=r.campaign_name;campaign.status=client.enums.CampaignStatusEnum.PAUSED;campaign.advertising_channel_type=client.enums.AdvertisingChannelTypeEnum.SEARCH;campaign.campaign_budget=budget.resource_name
    campaign.manual_cpc.enhanced_cpc_enabled=False
    campaign.network_settings.target_google_search=True;campaign.network_settings.target_search_network=False;campaign.network_settings.target_content_network=False
    campaign.geo_target_type_setting.positive_geo_target_type=client.enums.PositiveGeoTargetTypeEnum.PRESENCE;campaign.geo_target_type_setting.negative_geo_target_type=client.enums.NegativeGeoTargetTypeEnum.PRESENCE
    campaign.contains_eu_political_advertising=client.enums.EuPoliticalAdvertisingStatusEnum.DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING
    for kind,value in [('location',r.geo_id),('language',r.language_id)]:
        criterion=new('campaign_criterion',{'kind':kind});criterion.campaign=campaign.resource_name
        if kind=='location':criterion.location.geo_target_constant='geoTargetConstants/'+value
        else:criterion.language.language_constant='languageConstants/'+value
    for index,g in enumerate(r.groups):
        group=new('ad_group',{'kind':'ad_group','name':g.name});group.resource_name=f'customers/{cid}/adGroups/{-10-index}';group.name=g.name;group.campaign=campaign.resource_name;group.type_=client.enums.AdGroupTypeEnum.SEARCH_STANDARD;group.status=client.enums.AdGroupStatusEnum.PAUSED;group.cpc_bid_micros=r.cpc_bid_micros
        ad=new('ad_group_ad',{'kind':'rsa','group':g.name});ad.ad_group=group.resource_name;ad.status=client.enums.AdGroupAdStatusEnum.PAUSED;ad.ad.final_urls.append(g.final_url);ad.ad.responsive_search_ad.path1=g.path1;ad.ad.responsive_search_ad.path2=g.path2
        for field,values in [('headlines',g.headlines),('descriptions',g.descriptions)]:
            for text in values:
                asset=client.get_type('AdTextAsset');asset.text=text;getattr(ad.ad.responsive_search_ad,field).append(asset)
        for k in g.keywords:
            criterion=new('ad_group_criterion',{'kind':'keyword','group':g.name,'text':k.text,'match':k.match});criterion.ad_group=group.resource_name;criterion.status=client.enums.AdGroupCriterionStatusEnum.ENABLED;criterion.keyword.text=k.text;criterion.keyword.match_type=getattr(client.enums.KeywordMatchTypeEnum,k.match);criterion.cpc_bid_micros=r.cpc_bid_micros
    return ops,metadata

def _mutate(client, customer, operations, validate):
    response=client.get_service('GoogleAdsService').mutate(request={'customer_id':customer,'mutate_operations':operations,'partial_failure':False,'validate_only':validate},timeout=90)
    gam._track_api_call('VALIDATE_SEARCH_DELIVERY' if validate else 'MUTATE_SEARCH_DELIVERY',len(operations))
    return response

def _error(exc):
    if isinstance(exc,GoogleAdsException):
        return {'status':'error','request_id':exc.request_id,'errors':[{'code':str(x.error_code),'message':x.message,'location':str(x.location)} for x in exc.failure.errors]}
    return {'status':'error','message':str(exc),'readback_required':True}

@gam.mcp.tool()
async def google_ads_create_reviewed_search_campaign(request: ReviewedSearchRequest)->dict:
    """Validate or atomically create a reviewed PAUSED Search campaign. Always PAUSED at campaign/group/ad levels; Google Search only, Presence, Manual CPC, exact/phrase. Defaults validate_only=True. No retry on uncertain outcomes. Existing name stops creation."""
    try:
        gam._ensure_client();client=gam.google_ads_client;cid=request.customer_id
        customer=_search(client,cid,'SELECT customer.id, customer.currency_code FROM customer LIMIT 1')[0].customer
        if customer.currency_code!=request.expected_currency:raise ValueError('currency_mismatch')
        currency=_search(client,cid,f"SELECT currency_constant.billable_unit_micros FROM currency_constant WHERE currency_constant.code = '{request.expected_currency}' LIMIT 1")[0].currency_constant
        unit=currency.billable_unit_micros
        if request.cpc_bid_micros<unit or request.cpc_bid_micros%unit or request.daily_budget_micros%unit:raise ValueError('amount_not_billable_unit_'+str(unit))
        existing=_search(client,cid,f"SELECT campaign.id, campaign.status FROM campaign WHERE campaign.name = '{request.campaign_name}' AND campaign.status != 'REMOVED' LIMIT 2")
        if existing:return {'status':'exists_no_mutation','campaign_ids':[str(x.campaign.id) for x in existing]}
        operations,metadata=build_search_operations(client,request)
        response=_mutate(client,cid,operations,request.validate_only)
        out=[]
        for meta,result in zip(metadata,response.mutate_operation_responses):
            field=result._pb.WhichOneof('response');out.append({**meta,'resource_name':getattr(result,field).resource_name})
        return {'status':'validated' if request.validate_only else 'created_paused','validate_only':request.validate_only,'operation_count':len(operations),'billable_unit_micros':unit,'results':out}
    except Exception as e:return _error(e)

class ExactCampaignGoalRequest(StrictInput):
    customer_id: str = Field(pattern=r'^\d{10}$')
    campaign_ids: list[str] = Field(min_length=1,max_length=7)
    goal_name: str = Field(pattern=r'^[A-Za-z0-9 |_-]{1,120}$')
    conversion_action_ids: list[str] = Field(min_length=1,max_length=10)
    validate_only: bool = True
    @model_validator(mode='after')
    def identifiers(self):
        for values in [self.campaign_ids,self.conversion_action_ids]:
            if len(set(values))!=len(values) or any(not v.isdigit() for v in values):raise ValueError('invalid_identifiers')
        return self

@gam.mcp.tool()
async def google_ads_set_exact_campaign_conversion_goal(request: ExactCampaignGoalRequest)->dict:
    """Attach one exact custom goal to up to seven PAUSED reviewed Search campaigns (configured naming pattern) and disable all standard campaign goals. Reuses identical named goal; rejects drift. Does not modify account defaults or conversion actions. Defaults validate_only=True."""
    try:
        gam._ensure_client();c=gam.google_ads_client;cid=request.customer_id
        ids=','.join(request.campaign_ids);rows=_search(c,cid,f'SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type FROM campaign WHERE campaign.id IN ({ids}) LIMIT 8')
        if {str(x.campaign.id) for x in rows}!=set(request.campaign_ids):raise ValueError('campaign_missing')
        if any(x.campaign.status!=c.enums.CampaignStatusEnum.PAUSED or x.campaign.advertising_channel_type!=c.enums.AdvertisingChannelTypeEnum.SEARCH or not _in_scope(x.campaign.name) for x in rows):raise ValueError('campaign_scope_or_status')
        actions=[f'customers/{cid}/conversionActions/{v}' for v in request.conversion_action_ids]
        arows=_search(c,cid,'SELECT conversion_action.id, conversion_action.status FROM conversion_action WHERE conversion_action.id IN ('+','.join(request.conversion_action_ids)+') LIMIT 11')
        if {str(x.conversion_action.id) for x in arows}!=set(request.conversion_action_ids) or any(x.conversion_action.status!=c.enums.ConversionActionStatusEnum.ENABLED for x in arows):raise ValueError('conversion_action_not_enabled')
        existing=_search(c,cid,f"SELECT custom_conversion_goal.resource_name, custom_conversion_goal.conversion_actions FROM custom_conversion_goal WHERE custom_conversion_goal.name = '{request.goal_name}' AND custom_conversion_goal.status = 'ENABLED' LIMIT 2")
        if len(existing)>1:raise ValueError('ambiguous_custom_goal')
        ops=[]
        if existing:
            goal=existing[0].custom_conversion_goal
            if set(goal.conversion_actions)!=set(actions):raise ValueError('custom_goal_action_drift')
            goal_resource=goal.resource_name
        else:
            op=c.get_type('MutateOperation');goal=op.custom_conversion_goal_operation.create;goal.resource_name=f'customers/{cid}/customConversionGoals/-1';goal.name=request.goal_name;goal.conversion_actions.extend(actions);goal.status=c.enums.CustomConversionGoalStatusEnum.ENABLED;ops.append(op);goal_resource=goal.resource_name
        standard=_search(c,cid,f'SELECT campaign.id, campaign_conversion_goal.resource_name, campaign_conversion_goal.biddable FROM campaign_conversion_goal WHERE campaign.id IN ({ids}) LIMIT 200')
        if len(standard)>=200:raise ValueError('standard_goal_limit')
        for row in standard:
            op=c.get_type('MutateOperation');op.campaign_conversion_goal_operation.update.resource_name=row.campaign_conversion_goal.resource_name;op.campaign_conversion_goal_operation.update.biddable=False;op.campaign_conversion_goal_operation.update_mask.paths.append('biddable');ops.append(op)
        for id_ in request.campaign_ids:
            op=c.get_type('MutateOperation');config=op.conversion_goal_campaign_config_operation.update;config.resource_name=f'customers/{cid}/conversionGoalCampaignConfigs/{id_}';config.custom_conversion_goal=goal_resource;config.goal_config_level=c.enums.GoalConfigLevelEnum.CAMPAIGN;op.conversion_goal_campaign_config_operation.update_mask.paths.extend(['custom_conversion_goal','goal_config_level']);ops.append(op)
        result=_mutate(c,cid,ops,request.validate_only)
        resources=[]
        for r in result.mutate_operation_responses:
            field=r._pb.WhichOneof('response');resources.append(getattr(r,field).resource_name)
        return {'status':'validated' if request.validate_only else 'configured','operation_count':len(ops),'campaign_ids':request.campaign_ids,'actions':actions,'resources':resources}
    except Exception as e:return _error(e)

# Existing-entity optimization. This does not create campaigns, ads or budgets.
import hashlib
import json

class ReviewedRsaUpdate(StrictInput):
    ad_group_id: str = Field(pattern=r'^\d+$')
    ad_id: str = Field(pattern=r'^\d+$')
    expected_content_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    headlines: list[str] = Field(min_length=15,max_length=15)
    descriptions: list[str] = Field(min_length=4,max_length=4)
    @model_validator(mode='after')
    def text_limits(self):
        for texts,limit in [(self.headlines,30),(self.descriptions,90)]:
            if any(not s.strip() or len(s)>limit for s in texts) or len(set(texts))!=len(texts):raise ValueError('invalid_or_duplicate_rsa_text')
        return self

class ReviewedSearchOptimization(StrictInput):
    customer_id: str = Field(pattern=r'^\d{10}$')
    campaign_id: str = Field(pattern=r'^\d+$')
    shop_domain: str = Field(pattern=_HOST_PATTERN,max_length=253)
    business_name: str = Field(min_length=1,max_length=25)
    existing_logo_asset_id: str = Field(pattern=r'^\d+$')
    existing_callout_asset_ids: list[str] = Field(min_length=4,max_length=8)
    ads: list[ReviewedRsaUpdate] = Field(min_length=1,max_length=9)
    validate_only: bool = True
    @model_validator(mode='after')
    def identity_and_ids(self):
        if _launch_config().get('business_name_must_match_domain') and self.business_name!=self.shop_domain:raise ValueError('business_name_must_match_destination_domain')
        if len(set(self.existing_callout_asset_ids))!=len(self.existing_callout_asset_ids) or any(not s.isdigit() for s in self.existing_callout_asset_ids):raise ValueError('invalid_callout_ids')
        if len({a.ad_id for a in self.ads})!=len(self.ads):raise ValueError('duplicate_ad')
        return self

def rsa_content(ad):
    rsa=ad.responsive_search_ad
    return {'headlines':[x.text for x in rsa.headlines],'descriptions':[x.text for x in rsa.descriptions],'final_urls':list(ad.final_urls),'path1':rsa.path1,'path2':rsa.path2}

def rsa_content_sha256(content):
    return hashlib.sha256(json.dumps(content,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def build_rsa_update(client,customer,request):
    op=client.get_type('MutateOperation');ad=op.ad_operation.update;ad.resource_name=f'customers/{customer}/ads/{request.ad_id}'
    for name,texts in [('headlines',request.headlines),('descriptions',request.descriptions)]:
        for text in texts:
            asset=client.get_type('AdTextAsset');asset.text=text;getattr(ad.responsive_search_ad,name).append(asset)
    op.ad_operation.update_mask.paths.extend(['responsive_search_ad.headlines','responsive_search_ad.descriptions'])
    return op

@gam.mcp.tool()
async def google_ads_optimize_reviewed_search(request: ReviewedSearchOptimization)->dict:
    """Atomically update exact existing reviewed-campaign RSA text and campaign business identity/callout links. Requires unchanged ad content SHA, preserves ad IDs, URLs, status, keywords, bids and budgets. Reuses the existing account logo and callout assets. Domain-exact business name. Defaults validate_only=True; read back on any uncertain result."""
    try:
        gam._ensure_client();c=gam.google_ads_client;cid=request.customer_id;campaign=f'customers/{cid}/campaigns/{request.campaign_id}'
        rows=_search(c,cid,f'SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type FROM campaign WHERE campaign.id = {request.campaign_id} LIMIT 1')
        if len(rows)!=1 or not _in_scope(rows[0].campaign.name) or f' | {request.shop_domain} | ' not in rows[0].campaign.name or rows[0].campaign.status==c.enums.CampaignStatusEnum.REMOVED or rows[0].campaign.advertising_channel_type!=c.enums.AdvertisingChannelTypeEnum.SEARCH:raise ValueError('campaign_scope_mismatch')
        ids=','.join(a.ad_id for a in request.ads)
        arows=_search(c,cid,f'SELECT campaign.id, ad_group.id, ad_group_ad.status, ad_group_ad.ad.id, ad_group_ad.ad.type, ad_group_ad.ad.final_urls, ad_group_ad.ad.responsive_search_ad.headlines, ad_group_ad.ad.responsive_search_ad.descriptions, ad_group_ad.ad.responsive_search_ad.path1, ad_group_ad.ad.responsive_search_ad.path2 FROM ad_group_ad WHERE campaign.id = {request.campaign_id} AND ad_group_ad.ad.id IN ({ids}) LIMIT 10')
        amap={(str(x.ad_group.id),str(x.ad_group_ad.ad.id)):x.ad_group_ad for x in arows}
        if set(amap)!={(a.ad_group_id,a.ad_id) for a in request.ads}:raise ValueError('ad_scope_mismatch')
        ops=[]
        for a in request.ads:
            current=amap[(a.ad_group_id,a.ad_id)];ad=current.ad;before=rsa_content(ad)
            if current.status==c.enums.AdGroupAdStatusEnum.REMOVED or ad.type_!=c.enums.AdTypeEnum.RESPONSIVE_SEARCH_AD:raise ValueError('invalid_ad_type_or_status')
            if any(urlsplit(u).hostname!=request.shop_domain or urlsplit(u).scheme!='https' for u in ad.final_urls) or not ad.final_urls:raise ValueError('ad_destination_drift')
            if any(x.pinned_field!=c.enums.ServedAssetFieldTypeEnum.UNSPECIFIED for x in list(ad.responsive_search_ad.headlines)+list(ad.responsive_search_ad.descriptions)):raise ValueError('pinned_asset_not_supported')
            if rsa_content_sha256(before)!=a.expected_content_sha256:raise ValueError('ad_content_drift_'+a.ad_id)
            if before['headlines']!=a.headlines or before['descriptions']!=a.descriptions:ops.append(build_rsa_update(c,cid,a))
        logos=_search(c,cid,f"SELECT customer_asset.asset, asset.id FROM customer_asset WHERE customer_asset.field_type = 'BUSINESS_LOGO' AND customer_asset.status = 'ENABLED' AND asset.id = {request.existing_logo_asset_id} LIMIT 2")
        if not logos:raise ValueError('logo_not_enabled_on_customer')
        callouts=_search(c,cid,'SELECT asset.id, asset.callout_asset.callout_text FROM asset WHERE asset.id IN ('+','.join(request.existing_callout_asset_ids)+') LIMIT 9')
        if {str(x.asset.id) for x in callouts}!=set(request.existing_callout_asset_ids) or any(not x.asset.callout_asset.callout_text for x in callouts):raise ValueError('invalid_callout_asset')
        existing=_search(c,cid,f"SELECT campaign.id, campaign_asset.asset, campaign_asset.field_type, campaign_asset.status, asset.text_asset.text FROM campaign_asset WHERE campaign.id = {request.campaign_id} AND campaign_asset.field_type IN ('BUSINESS_NAME','BUSINESS_LOGO','CALLOUT') AND campaign_asset.status != 'REMOVED' LIMIT 30")
        if len(existing)>=30:raise ValueError('asset_link_limit')
        present={(str(x.campaign_asset.field_type.name),x.campaign_asset.asset) for x in existing}
        for x in existing:
            if x.campaign_asset.status!=c.enums.AssetLinkStatusEnum.ENABLED:raise ValueError('existing_link_not_enabled')
            if x.campaign_asset.field_type==c.enums.AssetFieldTypeEnum.BUSINESS_NAME and x.asset.text_asset.text!=request.business_name:raise ValueError('conflicting_business_name')
            if x.campaign_asset.field_type==c.enums.AssetFieldTypeEnum.BUSINESS_LOGO and x.campaign_asset.asset!=f'customers/{cid}/assets/{request.existing_logo_asset_id}':raise ValueError('conflicting_logo')
        names=_search(c,cid,f"SELECT asset.resource_name FROM asset WHERE asset.text_asset.text = '{request.business_name}' AND asset.type = 'TEXT' LIMIT 2")
        if len(names)>1:raise ValueError('ambiguous_existing_business_name')
        if names:name_resource=names[0].asset.resource_name
        else:
            op=c.get_type('MutateOperation');asset=op.asset_operation.create;asset.resource_name=f'customers/{cid}/assets/-1';asset.text_asset.text=request.business_name;ops.append(op);name_resource=asset.resource_name
        desired=[('BUSINESS_NAME',name_resource),('BUSINESS_LOGO',f'customers/{cid}/assets/{request.existing_logo_asset_id}')]+[('CALLOUT',f'customers/{cid}/assets/{a}') for a in request.existing_callout_asset_ids]
        for kind,resource in desired:
            if (kind,resource) in present:continue
            op=c.get_type('MutateOperation');link=op.campaign_asset_operation.create;link.campaign=campaign;link.asset=resource;link.field_type=getattr(c.enums.AssetFieldTypeEnum,kind);ops.append(op)
        if not ops:return {'status':'success','message':'already_matches','operation_count':0}
        response=_mutate(c,cid,ops,request.validate_only);resources=[]
        for result in response.mutate_operation_responses:
            field=result._pb.WhichOneof('response');resources.append(getattr(result,field).resource_name)
        return {'status':'validated' if request.validate_only else 'success','operation_count':len(ops),'campaign_id':request.campaign_id,'business_name':request.business_name,'resources':resources}
    except Exception as e:return _error(e)
