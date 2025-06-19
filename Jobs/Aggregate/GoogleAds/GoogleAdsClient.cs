using Google.Ads.Gax.Config;
using Google.Ads.GoogleAds.Config;
using Google.Ads.GoogleAds.Util;
using Google.Ads.GoogleAds.V18.Errors;
using Google.Ads.GoogleAds.V18.Services;
using Greenhouse.Auth;
using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.Jobs.Aggregate.GoogleAds;

public class GoogleAdsClient
{
    private readonly GoogleAdsServiceClient googleAdsService;
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly Google.Ads.GoogleAds.Lib.GoogleAdsClient client;
    private readonly Auth.OAuthAuthenticator oAuth;
    private readonly string developerToken;
    private readonly string loginCustomerID;
    private readonly string googleAdsReplaceStrings;

    public GoogleAdsClient(IHttpClientProvider httpClientProvider, string developerToken, string loginCustomerID, OAuthAuthenticator oAuth, string JobGUID,
        Action<string> logAction, bool enableDetailedLogs, string replaceStringLookup, int? logLevelSDK, object lockObject)
    {
        _httpClientProvider = httpClientProvider;
        this.oAuth = oAuth;
        this.loginCustomerID = loginCustomerID;
        this.developerToken = developerToken;
        this.googleAdsReplaceStrings = $@"{replaceStringLookup}";

        this.client = new Google.Ads.GoogleAds.Lib.GoogleAdsClient(new GoogleAdsConfig
        {
            DeveloperToken = developerToken,
            LoginCustomerId = loginCustomerID,
            OAuth2Mode = OAuth2Flow.APPLICATION,
            OAuth2ClientId = this.oAuth.ClientId,
            OAuth2ClientSecret = this.oAuth.ClientSecret,
            OAuth2RefreshToken = this.oAuth.RefreshToken
        });

        if (logLevelSDK.HasValue && (Enum.IsDefined(typeof(SourceLevels), logLevelSDK.Value)))
        {
            SourceLevels level = (SourceLevels)logLevelSDK.Value;

            var customListener = new CustomTraceListener(logAction);

            TraceUtilities.Configure(
                enableDetailedLogs
                    ? TraceUtilities.DETAILED_REQUEST_LOGS_SOURCE
                    : TraceUtilities.SUMMARY_REQUEST_LOGS_SOURCE,
                customListener, level);
        }

        // Get the GoogleAdsService.
        this.googleAdsService = this.client.GetService(
            Google.Ads.GoogleAds.Services.V18.GoogleAdsService);
    }

    public T DownloadAsJson<T>(string endpoint, string query, string FileGUID)
    {
        return _httpClientProvider.SendRequestAndDeserializeAsync<T>(new HttpRequestOptions
        {
            Uri = endpoint,
            AuthToken = oAuth.GetAccessToken,
            Method = HttpMethod.Post,
            ContentType = "application/json",
            Content =
                new StringContent(JsonConvert.SerializeObject(new { query }), Encoding.UTF8,
                    "application/json"),
            Headers = new Dictionary<string, string>
            {
                { "developer-token", developerToken }, { "login-customer-id", loginCustomerID }
            }
        }).GetAwaiter().GetResult();
    }

    public int DownloadAsCSV(string customerID, string query, StreamWriter file,
        IEnumerable<APIReportField> fields, Dictionary<string, string> contextInfo, Action<string> logInfo)
    {
        int lineCount = 0;
        try
        {
            var requestedColumns = fields.Where(r => r.IsActive)
                .OrderBy(r => r.SortOrder);

            // creating a list of ordered functions returning the column value
            var columnValues = BuildColumnValues(requestedColumns);

            // Issue a search request.
            this.googleAdsService.SearchStream(customerID, query,
                delegate (SearchGoogleAdsStreamResponse resp)
                {
                    logInfo($"GoogleAdsService.SearchStream RequestId={resp.RequestId} CustomerId={customerID} - Queue Details:{string.Join(" / ", contextInfo)}");

                    // Loop through the results.
                    foreach (GoogleAdsRow row in resp.Results)
                    {
                        StreamRowToCSV(file, columnValues, row, this.googleAdsReplaceStrings);

                        lineCount++;
                    }
                });

            return lineCount;
        }
        catch (GoogleAdsException e)
        {
            throw (new APIReportException($"GoogleAds SDK ERROR for customerID {customerID} logInfo {string.Join(" / ", contextInfo)} : Message: {e.Message} - Failure: {e.Failure} - Request ID: {e.RequestId}"));
        }
    }

    private static void StreamRowToCSV(StreamWriter file, List<Func<GoogleAdsRow, string>> columnValues, GoogleAdsRow row, string replaceStringsPattern)
    {
        int columnValuesCount = columnValues.Count;

        for (int i = 0; i < columnValuesCount; i++)
        {
            file.Write('"');
            //calling the function to extract the column value from the row
            string value = columnValues[i](row);
            if (value != null)
            {
                // per diat-14738 - values ending in backslash cause issue with the Redshift COPY command
                // because they make \" due to the ending double-quotes on line 172
                // we escape the backslash here as a workaround to make \\"
                if (value.EndsWith(Constants.BACKWARD_SLASH))
                    value = value.Remove(value.Length - 1, 1) + "\\\\";
                value = Regex.Replace(value, replaceStringsPattern, string.Empty);
                file.Write(value.Replace("\"", "\\\""));
            }

            file.Write('"');

            //add a comma between values
            if (i < columnValuesCount - 1)
            {
                file.Write(",");
            }
            else
            {
                // that was the last value on the line, starting a new line
                file.WriteLine();
            }
        }
    }

    [Obsolete]
    private static List<Func<GoogleAdsRow, string>> BuildColumnValues(IOrderedEnumerable<APIReportField> requestedColumns)
    {
        var commands = new List<Func<GoogleAdsRow, string>>();
        foreach (var column in requestedColumns)
        {
            switch (column.APIReportFieldName)
            {
                case "metrics.active_view_cpm":
                    commands.Add((row) => row?.Metrics?.ActiveViewCpm.ToString());
                    break;
                case "metrics.active_view_impressions":
                    commands.Add((row) => row?.Metrics?.ActiveViewImpressions.ToString());
                    break;
                case "metrics.active_view_measurability":
                    commands.Add((row) => row?.Metrics?.ActiveViewMeasurability.ToString());
                    break;
                case "metrics.active_view_measurable_cost_micros":
                    commands.Add((row) => row?.Metrics?.ActiveViewMeasurableCostMicros.ToString());
                    break;
                case "metrics.active_view_measurable_impressions":
                    commands.Add((row) => row?.Metrics?.ActiveViewMeasurableImpressions.ToString());
                    break;
                case "metrics.active_view_viewability":
                    commands.Add((row) => row?.Metrics?.ActiveViewViewability.ToString());
                    break;
                case "metrics.all_conversions":
                    commands.Add((row) => row?.Metrics?.AllConversions.ToString());
                    break;
                case "metrics.all_conversions_value":
                    commands.Add((row) => row?.Metrics?.AllConversionsValue.ToString());
                    break;
                case "metrics.clicks":
                    commands.Add((row) => row?.Metrics?.Clicks.ToString());
                    break;
                case "metrics.conversions":
                    commands.Add((row) => row?.Metrics?.Conversions.ToString());
                    break;
                case "metrics.conversions_value":
                    commands.Add((row) => row?.Metrics?.ConversionsValue.ToString());
                    break;
                case "metrics.cost_micros":
                    commands.Add((row) => row?.Metrics?.CostMicros.ToString());
                    break;
                case "metrics.cross_device_conversions":
                    commands.Add((row) => row?.Metrics?.CrossDeviceConversions.ToString());
                    break;
                case "metrics.current_model_attributed_conversions":
                    commands.Add((row) => row?.Metrics?.CurrentModelAttributedConversions.ToString());
                    break;
                case "metrics.current_model_attributed_conversions_value":
                    commands.Add((row) => row?.Metrics?.CurrentModelAttributedConversionsValue.ToString());
                    break;
                case "metrics.engagements":
                    commands.Add((row) => row?.Metrics?.Engagements.ToString());
                    break;
                case "metrics.gmail_forwards":
                    commands.Add((row) => row?.Metrics?.GmailForwards.ToString());
                    break;
                case "metrics.gmail_saves":
                    commands.Add((row) => row?.Metrics?.GmailSaves.ToString());
                    break;
                case "metrics.gmail_secondary_clicks":
                    commands.Add((row) => row?.Metrics?.GmailSecondaryClicks.ToString());
                    break;
                case "metrics.impressions":
                    commands.Add((row) => row?.Metrics?.Impressions.ToString());
                    break;
                case "metrics.interaction_event_types":
                    commands.Add((row) => row?.Metrics?.InteractionEventTypes?.ToString());
                    break;
                case "metrics.interactions":
                    commands.Add((row) => row?.Metrics?.Interactions.ToString());
                    break;
                case "metrics.video_quartile_p100_rate":
                    commands.Add((row) => row?.Metrics?.VideoQuartileP100Rate.ToString());
                    break;
                case "metrics.video_quartile_p25_rate":
                    commands.Add((row) => row?.Metrics?.VideoQuartileP25Rate.ToString());
                    break;
                case "metrics.video_quartile_p50_rate":
                    commands.Add((row) => row?.Metrics?.VideoQuartileP50Rate.ToString());
                    break;
                case "metrics.video_quartile_p75_rate":
                    commands.Add((row) => row?.Metrics?.VideoQuartileP75Rate.ToString());
                    break;
                case "metrics.video_view_rate":
                    commands.Add((row) => row?.Metrics?.VideoViewRate.ToString());
                    break;
                case "metrics.video_views":
                    commands.Add((row) => row?.Metrics?.VideoViews.ToString());
                    break;
                case "metrics.view_through_conversions":
                    commands.Add((row) => row?.Metrics?.ViewThroughConversions.ToString());
                    break;
                case "metrics.all_conversions_value_by_conversion_date":
                    commands.Add((row) => row?.Metrics?.AllConversionsValueByConversionDate.ToString());
                    break;
                case "metrics.all_conversions_by_conversion_date":
                    commands.Add((row) => row?.Metrics?.AllConversionsByConversionDate.ToString());
                    break;
                case "metrics.conversions_value_by_conversion_date":
                    commands.Add((row) => row?.Metrics?.ConversionsValueByConversionDate.ToString());
                    break;
                case "metrics.conversions_by_conversion_date":
                    commands.Add((row) => row?.Metrics?.ConversionsByConversionDate.ToString());
                    break;
                case "metrics.biddable_app_install_conversions":
                    commands.Add((row) => row?.Metrics?.BiddableAppInstallConversions.ToString());
                    break;
                case "metrics.biddable_app_post_install_conversions":
                    commands.Add((row) => row?.Metrics?.BiddableAppPostInstallConversions.ToString());
                    break;
                case "keyword_view.resource_name":
                    commands.Add((row) => row?.KeywordView?.ResourceName?.ToString());
                    break;
                case "segments.date":
                    commands.Add((row) => row?.Segments?.Date?.ToString());
                    break;
                case "segments.device":
                    commands.Add((row) => row?.Segments?.Device.ToString());
                    break;
                case "customer.id":
                    commands.Add((row) => row?.Customer?.Id.ToString());
                    break;
                case "customer.descriptive_name":
                    commands.Add((row) => row?.Customer?.DescriptiveName?.ToString());
                    break;
                case "customer.currency_code":
                    commands.Add((row) => row?.Customer?.CurrencyCode?.ToString());
                    break;
                case "customer.time_zone":
                    commands.Add((row) => row?.Customer?.TimeZone?.ToString());
                    break;
                case "campaign.ad_serving_optimization_status":
                    commands.Add((row) => row?.Campaign?.AdServingOptimizationStatus.ToString());
                    break;
                case "campaign.advertising_channel_sub_type":
                    commands.Add((row) => row?.Campaign?.AdvertisingChannelSubType.ToString());
                    break;
                case "campaign.advertising_channel_type":
                    commands.Add((row) => row?.Campaign?.AdvertisingChannelType.ToString());
                    break;
                case "campaign.app_campaign_setting.app_id":
                    commands.Add((row) => row?.Campaign?.AppCampaignSetting?.AppId?.ToString());
                    break;
                case "campaign.app_campaign_setting.app_store":
                    commands.Add((row) => row?.Campaign?.AppCampaignSetting?.AppStore.ToString());
                    break;
                case "campaign.app_campaign_setting.bidding_strategy_goal_type":
                    commands.Add((row) => row?.Campaign?.AppCampaignSetting?.BiddingStrategyGoalType.ToString());
                    break;
                case "campaign.base_campaign":
                    commands.Add((row) => row?.Campaign?.BaseCampaign?.ToString());
                    break;
                case "campaign.bidding_strategy":
                    commands.Add((row) => row?.Campaign?.BiddingStrategy?.ToString());
                    break;
                case "campaign.bidding_strategy_type":
                    commands.Add((row) => row?.Campaign?.BiddingStrategyType.ToString());
                    break;
                case "campaign.campaign_budget":
                    commands.Add((row) => row?.Campaign?.CampaignBudget?.ToString());
                    break;
                case "campaign.commission.commission_rate_micros":
                    commands.Add((row) => row?.Campaign?.Commission?.CommissionRateMicros.ToString());
                    break;
                case "campaign.dynamic_search_ads_setting.domain_name":
                    commands.Add((row) => row?.Campaign?.DynamicSearchAdsSetting?.DomainName?.ToString());
                    break;
                case "campaign.dynamic_search_ads_setting.feeds":
                    commands.Add((row) => row?.Campaign?.DynamicSearchAdsSetting?.Feeds?.ToString());
                    break;
                case "campaign.dynamic_search_ads_setting.language_code":
                    commands.Add((row) => row?.Campaign?.DynamicSearchAdsSetting?.LanguageCode?.ToString());
                    break;
                case "campaign.dynamic_search_ads_setting.use_supplied_urls_only":
                    commands.Add((row) => row?.Campaign?.DynamicSearchAdsSetting?.UseSuppliedUrlsOnly.ToString());
                    break;
                case "campaign.end_date":
                    commands.Add((row) => row?.Campaign?.EndDate?.ToString());
                    break;
                case "campaign.experiment_type":
                    commands.Add((row) => row?.Campaign?.ExperimentType.ToString());
                    break;
                case "campaign.final_url_suffix":
                    commands.Add((row) => row?.Campaign?.FinalUrlSuffix?.ToString());
                    break;
                case "campaign.frequency_caps":
                    commands.Add((row) => row?.Campaign?.FrequencyCaps?.ToString());
                    break;
                case "campaign.geo_target_type_setting.negative_geo_target_type":
                    commands.Add((row) => row?.Campaign?.GeoTargetTypeSetting?.NegativeGeoTargetType.ToString());
                    break;
                case "campaign.geo_target_type_setting.positive_geo_target_type":
                    commands.Add((row) => row?.Campaign?.GeoTargetTypeSetting?.PositiveGeoTargetType.ToString());
                    break;
                case "campaign.hotel_setting.hotel_center_id":
                    commands.Add((row) => row?.Campaign?.HotelSetting?.HotelCenterId.ToString());
                    break;
                case "campaign.id":
                    commands.Add((row) => row?.Campaign?.Id.ToString());
                    break;
                case "campaign.labels":
                    commands.Add((row) => row?.Campaign?.Labels?.ToString());
                    break;
                case "campaign_label.label":
                    commands.Add((row) => row?.CampaignLabel?.Label?.ToString());
                    break;
                case "campaign.local_campaign_setting.location_source_type":
                    commands.Add((row) => row?.Campaign?.LocalCampaignSetting?.LocationSourceType.ToString());
                    break;
                case "campaign.manual_cpc.enhanced_cpc_enabled":
                    commands.Add((row) => row?.Campaign?.ManualCpc?.EnhancedCpcEnabled.ToString());
                    break;
                case "campaign.manual_cpm":
                    commands.Add((row) => row?.Campaign?.ManualCpm?.ToString());
                    break;
                case "campaign.manual_cpv":
                    commands.Add((row) => row?.Campaign?.ManualCpv?.ToString());
                    break;
                case "campaign.maximize_conversion_value.target_roas":
                    commands.Add((row) => row?.Campaign?.MaximizeConversionValue?.TargetRoas.ToString());
                    break;
                case "campaign.maximize_conversions.target_cpa_micros":
                    commands.Add((row) => row?.Campaign?.MaximizeConversions?.TargetCpaMicros.ToString());
                    break;
                case "campaign.name":
                    commands.Add((row) => row?.Campaign?.Name?.ToString());
                    break;
                case "campaign.network_settings.target_content_network":
                    commands.Add((row) => row?.Campaign?.NetworkSettings?.TargetContentNetwork.ToString());
                    break;
                case "campaign.network_settings.target_google_search":
                    commands.Add((row) => row?.Campaign?.NetworkSettings?.TargetGoogleSearch.ToString());
                    break;
                case "campaign.network_settings.target_partner_search_network":
                    commands.Add((row) => row?.Campaign?.NetworkSettings?.TargetPartnerSearchNetwork.ToString());
                    break;
                case "campaign.network_settings.target_search_network":
                    commands.Add((row) => row?.Campaign?.NetworkSettings?.TargetSearchNetwork.ToString());
                    break;
                case "campaign.optimization_goal_setting.optimization_goal_types":
                    commands.Add((row) => row?.Campaign?.OptimizationGoalSetting?.OptimizationGoalTypes?.ToString());
                    break;
                case "campaign.optimization_score":
                    commands.Add((row) => row?.Campaign?.OptimizationScore.ToString());
                    break;
                case "campaign.payment_mode":
                    commands.Add((row) => row?.Campaign?.PaymentMode.ToString());
                    break;
                case "campaign.percent_cpc.cpc_bid_ceiling_micros":
                    commands.Add((row) => row?.Campaign?.PercentCpc?.CpcBidCeilingMicros.ToString());
                    break;
                case "campaign.percent_cpc.enhanced_cpc_enabled":
                    commands.Add((row) => row?.Campaign?.PercentCpc?.EnhancedCpcEnabled.ToString());
                    break;
                case "campaign.real_time_bidding_setting.opt_in":
                    commands.Add((row) => row?.Campaign?.RealTimeBiddingSetting?.OptIn.ToString());
                    break;
                case "campaign.resource_name":
                    commands.Add((row) => row?.Campaign?.ResourceName?.ToString());
                    break;
                case "campaign.selective_optimization.conversion_actions":
                    commands.Add((row) => row?.Campaign?.SelectiveOptimization?.ConversionActions?.ToString());
                    break;
                case "campaign.serving_status":
                    commands.Add((row) => row?.Campaign?.ServingStatus.ToString());
                    break;
                case "campaign.shopping_setting.campaign_priority":
                    commands.Add((row) => row?.Campaign?.ShoppingSetting?.CampaignPriority.ToString());
                    break;
                case "campaign.shopping_setting.enable_local":
                    commands.Add((row) => row?.Campaign?.ShoppingSetting?.EnableLocal.ToString());
                    break;
                case "campaign.shopping_setting.merchant_id":
                    commands.Add((row) => row?.Campaign?.ShoppingSetting?.MerchantId.ToString());
                    break;
                case "campaign.shopping_setting.feed_label":
                    commands.Add((row) => row?.Campaign?.ShoppingSetting?.FeedLabel?.ToString());
                    break;
                case "campaign.start_date":
                    commands.Add((row) => row?.Campaign?.StartDate?.ToString());
                    break;
                case "campaign.status":
                    commands.Add((row) => row?.Campaign?.Status.ToString());
                    break;
                case "campaign.target_cpa.cpc_bid_ceiling_micros":
                    commands.Add((row) => row?.Campaign?.TargetCpa?.CpcBidCeilingMicros.ToString());
                    break;
                case "campaign.target_cpa.cpc_bid_floor_micros":
                    commands.Add((row) => row?.Campaign?.TargetCpa?.CpcBidFloorMicros.ToString());
                    break;
                case "campaign.target_cpa.target_cpa_micros":
                    commands.Add((row) => row?.Campaign?.TargetCpa?.TargetCpaMicros.ToString());
                    break;
                case "campaign.target_impression_share.cpc_bid_ceiling_micros":
                    commands.Add((row) => row?.Campaign?.TargetImpressionShare?.CpcBidCeilingMicros.ToString());
                    break;
                case "campaign.target_impression_share.location":
                    commands.Add((row) => row?.Campaign?.TargetImpressionShare?.Location.ToString());
                    break;
                case "campaign.target_impression_share.location_fraction_micros":
                    commands.Add((row) => row?.Campaign?.TargetImpressionShare?.LocationFractionMicros.ToString());
                    break;
                case "campaign.target_roas.cpc_bid_ceiling_micros":
                    commands.Add((row) => row?.Campaign?.TargetRoas?.CpcBidCeilingMicros.ToString());
                    break;
                case "campaign.target_roas.cpc_bid_floor_micros":
                    commands.Add((row) => row?.Campaign?.TargetRoas?.CpcBidFloorMicros.ToString());
                    break;
                case "campaign.target_roas.target_roas":
                    commands.Add((row) => row?.Campaign?.TargetRoas?.TargetRoas_.ToString());
                    break;
                case "campaign.target_spend.cpc_bid_ceiling_micros":
                    commands.Add((row) => row?.Campaign?.TargetSpend?.CpcBidCeilingMicros.ToString());
                    break;
                case "campaign.target_spend.target_spend_micros":
                    commands.Add((row) => row?.Campaign?.TargetSpend?.TargetSpendMicros.ToString());
                    break;
                case "campaign.targeting_setting.target_restrictions":
                    commands.Add((row) => row?.Campaign?.TargetingSetting?.TargetRestrictions?.ToString());
                    break;
                case "campaign.tracking_setting.tracking_url":
                    commands.Add((row) => row?.Campaign?.TrackingSetting?.TrackingUrl?.ToString());
                    break;
                case "campaign.tracking_url_template":
                    commands.Add((row) => row?.Campaign?.TrackingUrlTemplate?.ToString());
                    break;
                case "campaign.url_custom_parameters":
                    commands.Add((row) => row?.Campaign?.UrlCustomParameters?.ToString());
                    break;
                case "campaign.vanity_pharma.vanity_pharma_display_url_mode":
                    commands.Add((row) => row?.Campaign?.VanityPharma?.VanityPharmaDisplayUrlMode.ToString());
                    break;
                case "campaign.vanity_pharma.vanity_pharma_text":
                    commands.Add((row) => row?.Campaign?.VanityPharma?.VanityPharmaText.ToString());
                    break;
                case "campaign.video_brand_safety_suitability":
                    commands.Add((row) => row?.Campaign?.VideoBrandSafetySuitability.ToString());
                    break;
                case "ad_group.ad_rotation_mode":
                    commands.Add((row) => row?.AdGroup?.AdRotationMode.ToString());
                    break;
                case "ad_group.base_ad_group":
                    commands.Add((row) => row?.AdGroup?.BaseAdGroup?.ToString());
                    break;
                case "ad_group.campaign":
                    commands.Add((row) => row?.AdGroup?.Campaign?.ToString());
                    break;
                case "ad_group.cpc_bid_micros":
                    commands.Add((row) => row?.AdGroup?.CpcBidMicros.ToString());
                    break;
                case "ad_group.cpm_bid_micros":
                    commands.Add((row) => row?.AdGroup?.CpmBidMicros.ToString());
                    break;
                case "ad_group.cpv_bid_micros":
                    commands.Add((row) => row?.AdGroup?.CpvBidMicros.ToString());
                    break;
                case "ad_group_criterion.criterion_id":
                    commands.Add((row) => row?.AdGroupCriterion?.CriterionId.ToString());
                    break;
                case "ad_group_criterion.keyword.text":
                    commands.Add((row) => row?.AdGroupCriterion?.Keyword.Text.ToString());
                    break;
                case "ad_group_criterion.keyword.match_type":
                    commands.Add((row) => row?.AdGroupCriterion?.Keyword.MatchType.ToString());
                    break;
                case "ad_group_criterion.quality_info.quality_score":
                    commands.Add((row) => row?.AdGroupCriterion?.QualityInfo?.QualityScore.ToString());
                    break;
                case "ad_group.display_custom_bid_dimension":
                    commands.Add((row) => row?.AdGroup?.DisplayCustomBidDimension.ToString());
                    break;
                case "ad_group.effective_target_cpa_micros":
                    commands.Add((row) => row?.AdGroup?.EffectiveTargetCpaMicros.ToString());
                    break;
                case "ad_group.effective_target_cpa_source":
                    commands.Add((row) => row?.AdGroup?.EffectiveTargetCpaSource.ToString());
                    break;
                case "ad_group.effective_target_roas":
                    commands.Add((row) => row?.AdGroup?.EffectiveTargetRoas.ToString());
                    break;
                case "ad_group.effective_target_roas_source":
                    commands.Add((row) => row?.AdGroup?.EffectiveTargetRoasSource.ToString());
                    break;
                case "ad_group.final_url_suffix":
                    commands.Add((row) => row?.AdGroup?.FinalUrlSuffix?.ToString());
                    break;
                case "ad_group.id":
                    commands.Add((row) => row?.AdGroup?.Id.ToString());
                    break;
                case "ad_group.labels":
                    commands.Add((row) => row?.AdGroup?.Labels?.ToString());
                    break;
                case "ad_group_label.label":
                    commands.Add((row) => row?.AdGroupLabel?.Label?.ToString());
                    break;
                case "ad_group.name":
                    commands.Add((row) => row?.AdGroup?.Name?.ToString());
                    break;
                case "ad_group.percent_cpc_bid_micros":
                    commands.Add((row) => row?.AdGroup?.PercentCpcBidMicros.ToString());
                    break;
                case "ad_group.resource_name":
                    commands.Add((row) => row?.AdGroup?.ResourceName?.ToString());
                    break;
                case "ad_group.status":
                    commands.Add((row) => row?.AdGroup?.Status.ToString());
                    break;
                case "ad_group.target_cpa_micros":
                    commands.Add((row) => row?.AdGroup?.TargetCpaMicros.ToString());
                    break;
                case "ad_group.target_cpm_micros":
                    commands.Add((row) => row?.AdGroup?.TargetCpmMicros.ToString());
                    break;
                case "ad_group.target_roas":
                    commands.Add((row) => row?.AdGroup?.TargetRoas.ToString());
                    break;
                case "ad_group.targeting_setting.target_restrictions":
                    commands.Add((row) => row?.AdGroup?.TargetingSetting?.TargetRestrictions?.ToString());
                    break;
                case "ad_group.tracking_url_template":
                    commands.Add((row) => row?.AdGroup?.TrackingUrlTemplate?.ToString());
                    break;
                case "ad_group.type":
                    commands.Add((row) => row?.AdGroup?.Type.ToString());
                    break;
                case "ad_group.url_custom_parameters":
                    commands.Add((row) => row?.AdGroup?.UrlCustomParameters?.ToString());
                    break;
                case "ad_group_ad.ad.id":
                    commands.Add((row) => row?.AdGroupAd?.Ad?.Id.ToString());
                    break;
                case "ad_group_ad.ad.device_preference":
                    commands.Add((row) => row?.AdGroupAd?.Ad?.DevicePreference.ToString());
                    break;
                case "ad_group_ad.ad.expanded_text_ad.headline_part1":
                    commands.Add((row) => row?.AdGroupAd?.Ad?.ExpandedTextAd?.HeadlinePart1?.ToString());
                    break;
                case "ad_group_ad.ad.expanded_text_ad.headline_part2":
                    commands.Add((row) => row?.AdGroupAd?.Ad?.ExpandedTextAd?.HeadlinePart2?.ToString());
                    break;
                case "ad_group_ad.ad.expanded_text_ad.headline_part3":
                    commands.Add((row) => row?.AdGroupAd?.Ad?.ExpandedTextAd?.HeadlinePart3?.ToString());
                    break;
                case "ad_group_ad.ad.final_url_suffix":
                    commands.Add((row) => row?.AdGroupAd?.Ad?.FinalUrlSuffix?.ToString());
                    break;
                case "ad_group_ad.ad.final_urls":
                    commands.Add((row) => row?.AdGroupAd?.Ad?.FinalUrls?.ToString());
                    break;
                case "ad_group_ad.ad.name":
                    commands.Add((row) => row?.AdGroupAd?.Ad?.Name?.ToString());
                    break;
                case "ad_group_ad.ad.text_ad.headline":
                    commands.Add((row) => row?.AdGroupAd?.Ad?.TextAd?.Headline?.ToString());
                    break;
                case "ad_group_ad.ad.tracking_url_template":
                    commands.Add((row) => row?.AdGroupAd?.Ad?.TrackingUrlTemplate?.ToString());
                    break;
                case "ad_group_ad.ad.type":
                    commands.Add((row) => row?.AdGroupAd?.Ad?.Type.ToString());
                    break;
                case "ad_group_ad.ad.url_custom_parameters":
                    commands.Add((row) => row?.AdGroupAd?.Ad?.UrlCustomParameters?.ToString());
                    break;
                case "ad_group_ad.ad.video_ad.in_stream.action_button_label":
                    commands.Add((row) => row?.AdGroupAd?.Ad?.VideoAd?.InStream?.ActionButtonLabel?.ToString());
                    break;
                case "ad_group_ad.labels":
                    commands.Add((row) => row?.AdGroupAd?.Labels?.ToString());
                    break;
                case "ad_group_ad_label.label":
                    commands.Add((row) => row?.AdGroupAdLabel?.Label?.ToString());
                    break;
                case "ad_group_ad.policy_summary.approval_status":
                    commands.Add((row) => row?.AdGroupAd?.PolicySummary?.ApprovalStatus.ToString());
                    break;
                case "ad_group_ad.policy_summary.review_status":
                    commands.Add((row) => row?.AdGroupAd?.PolicySummary?.ReviewStatus.ToString());
                    break;
                case "ad_group_ad.status":
                    commands.Add((row) => row?.AdGroupAd?.Status.ToString());
                    break;
                case "ad_group_ad.ad_group":
                    commands.Add((row) => row?.AdGroupAd?.AdGroup?.ToString());
                    break;
                case "segments.conversion_action_name":
                    commands.Add((row) => row?.Segments?.ConversionActionName?.ToString());
                    break;
                case "segments.external_conversion_source":
                    commands.Add((row) => row?.Segments?.ExternalConversionSource.ToString());
                    break;
                case "segments.conversion_action_category":
                    commands.Add((row) => row?.Segments?.ConversionActionCategory.ToString());
                    break;
                case "segments.geo_target_state":
                    commands.Add((row) => row?.Segments?.GeoTargetState.ToString());
                    break;
                case "segments.geo_target_metro":
                    commands.Add((row) => row?.Segments?.GeoTargetMetro.ToString());
                    break;
                case "label.id":
                    commands.Add((row) => row?.Label?.Id.ToString());
                    break;
                case "label.name":
                    commands.Add((row) => row?.Label?.Name.ToString());
                    break;
                case "label.resource_name":
                    commands.Add((row) => row?.Label?.ResourceName.ToString());
                    break;
                case "label.status":
                    commands.Add((row) => row?.Label?.Status.ToString());
                    break;
                case "geo_target_constant.canonical_name":
                    commands.Add((row) => row?.GeoTargetConstant?.CanonicalName.ToString());
                    break;
                case "geo_target_constant.country_code":
                    commands.Add((row) => row?.GeoTargetConstant?.CountryCode.ToString());
                    break;
                case "geo_target_constant.id":
                    commands.Add((row) => row?.GeoTargetConstant?.Id.ToString());
                    break;
                case "geo_target_constant.name":
                    commands.Add((row) => row?.GeoTargetConstant?.Name.ToString());
                    break;
                case "geo_target_constant.parent_geo_target":
                    commands.Add((row) => row?.GeoTargetConstant?.ParentGeoTarget.ToString());
                    break;
                case "geo_target_constant.resource_name":
                    commands.Add((row) => row?.GeoTargetConstant?.ResourceName.ToString());
                    break;
                case "geo_target_constant.status":
                    commands.Add((row) => row?.GeoTargetConstant?.Status.ToString());
                    break;
                case "geo_target_constant.target_type":
                    commands.Add((row) => row?.GeoTargetConstant?.TargetType.ToString());
                    break;
                case "geographic_view.country_criterion_id":
                    commands.Add((row) => row?.GeographicView?.CountryCriterionId.ToString());
                    break;
                case "user_location_view.country_criterion_id":
                    commands.Add((row) => row?.UserLocationView?.CountryCriterionId.ToString());
                    break;
                case "segments.ad_network_type":
                    commands.Add((row) => row?.Segments?.AdNetworkType.ToString());
                    break;
                case "user_location_view.resource_name":
                    commands.Add((row) => row?.UserLocationView?.ResourceName.ToString());
                    break;
                case "user_location_view.targeting_location":
                    commands.Add((row) => row?.UserLocationView?.TargetingLocation.ToString());
                    break;

                default:
                    throw (new APIReportException($"ERROR: Column '{column.APIReportFieldName}' has no corresponding column defined"));
            }
        }

        return commands;
    }
}
