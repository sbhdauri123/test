using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using Queue = Greenhouse.Data.Model.Core.Queue;

namespace Greenhouse.Data.DataSource.Pinterest
{
    public static class PinterestService
    {
        public static FileCollectionItem StageDelivery(List<DeliveryMetricReport> deliveryReport, Queue queue, Func<JArray, string, DateTime, string, FileCollectionItem> writeObjectToFile, string fileName, APIEntity entity)
        {
            var pinPromotionDeliveryReport = deliveryReport
                .Where(y => y.DeliveryMetrics != null)
                .SelectMany(x => x.DeliveryMetrics
                    .Select(xx => new
                    {
                        account_id = queue.EntityID,
                        account_name = entity.APIEntityName,
                        date = xx.DATE,
                        campaign_id = xx.PINPROMOTIONCAMPAIGNID,
                        campaign_name = xx.PINPROMOTIONCAMPAIGNNAME,
                        campaign_status = xx.PINPROMOTIONCAMPAIGNSTATUS,
                        ad_group_id = xx.PINPROMOTIONADGROUPID,
                        ad_group_name = xx.PINPROMOTIONADGROUPNAME,
                        ad_group_status = xx.PINPROMOTIONADGROUPSTATUS,
                        pin_promotion_id = xx.PINPROMOTIONID,
                        pin_promotion_name = xx.PINPROMOTIONNAME,
                        pin_promotion_status = xx.PINPROMOTIONSTATUS,
                        pin_id = xx.PINID,
                        spend_in_micro_dollar = xx.SPENDINMICRODOLLAR,
                        impression_1 = xx.IMPRESSION1,
                        impression_2 = xx.IMPRESSION2,
                        clickthrough_1 = xx.CLICKTHROUGH1,
                        clickthrough_2 = xx.CLICKTHROUGH2,
                        repin_1 = xx.REPIN1,
                        repin_2 = xx.REPIN2,
                        engagement_1 = xx.ENGAGEMENT1,
                        engagement_2 = xx.ENGAGEMENT2,
                        video_mrc_views_1 = xx.VIDEOMRCVIEWS1,
                        video_mrc_views_2 = xx.VIDEOMRCVIEWS2,
                        video_p0_combined_1 = xx.VIDEOP0COMBINED1,
                        video_p0_combined_2 = xx.VIDEOP0COMBINED2,
                        video_p100_complete_1 = xx.VIDEOP100COMPLETE1,
                        cpv_in_micro_dollar = xx.CPVINMICRODOLLAR,
                        cpcv_in_micro_dollar = xx.CPCVINMICRODOLLAR,
                        cpcv_p95_in_micro_dollar = xx.CPCVP95INMICRODOLLAR,
                        total_click_add_to_cart = xx.TOTALCLICKADDTOCART,
                        total_click_add_to_cart_value_in_micro_dollar = xx.TOTALCLICKADDTOCARTVALUEINMICRODOLLAR,
                        total_engagement_add_to_cart = xx.TOTALENGAGEMENTADDTOCART,
                        total_engagement_add_to_cart_value_in_micro_dollar = xx.TOTALENGAGEMENTADDTOCARTVALUEINMICRODOLLAR,
                        total_view_add_to_cart = xx.TOTALVIEWADDTOCART,
                        total_view_add_to_cart_value_in_micro_dollar = xx.TOTALVIEWADDTOCARTVALUEINMICRODOLLAR,
                        total_click_checkout = xx.TOTALCLICKCHECKOUT,
                        total_click_checkout_value_in_micro_dollar = xx.TOTALCLICKCHECKOUTVALUEINMICRODOLLAR,
                        total_engagement_checkout = xx.TOTALENGAGEMENTCHECKOUT,
                        total_engagement_checkout_value_in_micro_dollar = xx.TOTALENGAGEMENTCHECKOUTVALUEINMICRODOLLAR,
                        total_view_checkout = xx.TOTALVIEWCHECKOUT,
                        total_view_checkout_value_in_micro_dollar = xx.TOTALVIEWCHECKOUTVALUEINMICRODOLLAR,
                        total_click_page_visit = xx.TOTALCLICKPAGEVISIT,
                        total_click_page_visit_value_in_micro_dollar = xx.TOTALCLICKPAGEVISITVALUEINMICRODOLLAR,
                        total_engagement_page_visit = xx.TOTALENGAGEMENTPAGEVISIT,
                        total_engagement_page_visit_value_in_micro_dollar = xx.TOTALENGAGEMENTPAGEVISITVALUEINMICRODOLLAR,
                        total_view_page_visit = xx.TOTALVIEWPAGEVISIT,
                        total_view_page_visit_value_in_micro_dollar = xx.TOTALVIEWPAGEVISITVALUEINMICRODOLLAR,
                        total_click_signup = xx.TOTALCLICKSIGNUP,
                        total_click_signup_value_in_micro_dollar = xx.TOTALCLICKSIGNUPVALUEINMICRODOLLAR,
                        total_engagement_signup = xx.TOTALENGAGEMENTSIGNUP,
                        total_engagement_signup_value_in_micro_dollar = xx.TOTALENGAGEMENTSIGNUPVALUEINMICRODOLLAR,
                        total_view_signup = xx.TOTALVIEWSIGNUP,
                        total_view_signup_value_in_micro_dollar = xx.TOTALVIEWSIGNUPVALUEINMICRODOLLAR,
                        total_click_custom = xx.TOTALCLICKCUSTOM,
                        total_click_custom_value_in_micro_dollar = xx.TOTALCLICKCUSTOMVALUEINMICRODOLLAR,
                        total_engagement_custom = xx.TOTALENGAGEMENTCUSTOM,
                        total_engagement_custom_value_in_micro_dollar = xx.TOTALENGAGEMENTCUSTOMVALUEINMICRODOLLAR,
                        total_view_custom = xx.TOTALVIEWCUSTOM,
                        total_view_custom_value_in_micro_dollar = xx.TOTALVIEWCUSTOMVALUEINMICRODOLLAR,
                        video_p25_combined_1 = xx.VIDEOP25COMBINED1,
                        video_p50_combined_1 = xx.VIDEOP50COMBINED1,
                        video_p75_combined_1 = xx.VIDEOP75COMBINED1,
                        video_p95_combined_1 = xx.VIDEOP95COMBINED1,
                        total_conversions = xx.TOTALCONVERSIONS,
                        outbound_click_1 = xx.OUTBOUND_CLICK_1,
                        outbound_click_2 = xx.OUTBOUND_CLICK_2,
                        video_3sec_views_1 = xx.VIDEO3SECVIEWS1,
                        video_3sec_views_2 = xx.VIDEO3SECVIEWS2,
                        video_p25_combined_2 = xx.VIDEOP25COMBINED2,
                        video_p50_combined_2 = xx.VIDEOP50COMBINED2,
                        video_p75_combined_2 = xx.VIDEOP75COMBINED2,
                        video_p95_combined_2 = xx.VIDEOP95COMBINED2,
                        video_p100_complete_2 = xx.VIDEOP100COMPLETE2,
                        total_web_sessions = xx.TOTALWEBSESSIONS,
                        web_sessions_1 = xx.WEBSESSIONS1,
                        web_sessions_2 = xx.WEBSESSIONS2,
                        total_click_lead = xx.TOTALCLICKLEAD,
                        total_click_lead_value_in_micro_dollar = xx.TOTALCLICKLEADVALUEINMICRODOLLAR,
                        total_engagement_lead = xx.TOTALENGAGEMENTLEAD,
                        total_engagement_lead_value_in_micro_dollar = xx.TOTALENGAGEMENTLEADVALUEINMICRODOLLAR,
                        total_view_lead = xx.TOTALVIEWLEAD,
                        total_view_lead_value_in_micro_dollar = xx.TOTALVIEWLEADVALUEINMICRODOLLAR
                    }));

            var deliveryMetricObjectToSerialize = JArray.FromObject(pinPromotionDeliveryReport);
            return writeObjectToFile(deliveryMetricObjectToSerialize, queue.EntityID, queue.FileDate, fileName);
        }

        public static FileCollectionItem StageDeliveryWithTarget(List<DeliveryMetricReport> deliveryReport, Queue queue, Func<JArray, string, DateTime, string, FileCollectionItem> writeObjectToFile, string fileName, APIEntity entity)
        {
            var pinPromotionDeliveryReport = deliveryReport
                .Where(y => y.DeliveryMetrics != null)
                .SelectMany(x => x.DeliveryMetrics
                    .Select(xx => new
                    {
                        account_id = queue.EntityID,
                        account_name = entity.APIEntityName,
                        date = xx.DATE,
                        campaign_id = xx.PINPROMOTIONCAMPAIGNID,
                        campaign_name = xx.PINPROMOTIONCAMPAIGNNAME,
                        campaign_status = xx.PINPROMOTIONCAMPAIGNSTATUS,
                        ad_group_id = xx.PINPROMOTIONADGROUPID,
                        ad_group_name = xx.PINPROMOTIONADGROUPNAME,
                        ad_group_status = xx.PINPROMOTIONADGROUPSTATUS,
                        pin_promotion_id = xx.PINPROMOTIONID,
                        pin_promotion_name = xx.PINPROMOTIONNAME,
                        pin_promotion_status = xx.PINPROMOTIONSTATUS,
                        pin_id = xx.PINID,
                        spend_in_micro_dollar = xx.SPENDINMICRODOLLAR,
                        impression_1 = xx.IMPRESSION1,
                        impression_2 = xx.IMPRESSION2,
                        clickthrough_1 = xx.CLICKTHROUGH1,
                        clickthrough_2 = xx.CLICKTHROUGH2,
                        repin_1 = xx.REPIN1,
                        repin_2 = xx.REPIN2,
                        engagement_1 = xx.ENGAGEMENT1,
                        engagement_2 = xx.ENGAGEMENT2,
                        video_mrc_views_1 = xx.VIDEOMRCVIEWS1,
                        video_mrc_views_2 = xx.VIDEOMRCVIEWS2,
                        video_p0_combined_1 = xx.VIDEOP0COMBINED1,
                        video_p0_combined_2 = xx.VIDEOP0COMBINED2,
                        video_p100_complete_1 = xx.VIDEOP100COMPLETE1,
                        cpv_in_micro_dollar = xx.CPVINMICRODOLLAR,
                        cpcv_in_micro_dollar = xx.CPCVINMICRODOLLAR,
                        cpcv_p95_in_micro_dollar = xx.CPCVP95INMICRODOLLAR,
                        total_click_add_to_cart = xx.TOTALCLICKADDTOCART,
                        total_click_add_to_cart_value_in_micro_dollar = xx.TOTALCLICKADDTOCARTVALUEINMICRODOLLAR,
                        total_engagement_add_to_cart = xx.TOTALENGAGEMENTADDTOCART,
                        total_engagement_add_to_cart_value_in_micro_dollar = xx.TOTALENGAGEMENTADDTOCARTVALUEINMICRODOLLAR,
                        total_view_add_to_cart = xx.TOTALVIEWADDTOCART,
                        total_view_add_to_cart_value_in_micro_dollar = xx.TOTALVIEWADDTOCARTVALUEINMICRODOLLAR,
                        total_click_checkout = xx.TOTALCLICKCHECKOUT,
                        total_click_checkout_value_in_micro_dollar = xx.TOTALCLICKCHECKOUTVALUEINMICRODOLLAR,
                        total_engagement_checkout = xx.TOTALENGAGEMENTCHECKOUT,
                        total_engagement_checkout_value_in_micro_dollar = xx.TOTALENGAGEMENTCHECKOUTVALUEINMICRODOLLAR,
                        total_view_checkout = xx.TOTALVIEWCHECKOUT,
                        total_view_checkout_value_in_micro_dollar = xx.TOTALVIEWCHECKOUTVALUEINMICRODOLLAR,
                        total_click_page_visit = xx.TOTALCLICKPAGEVISIT,
                        total_click_page_visit_value_in_micro_dollar = xx.TOTALCLICKPAGEVISITVALUEINMICRODOLLAR,
                        total_engagement_page_visit = xx.TOTALENGAGEMENTPAGEVISIT,
                        total_engagement_page_visit_value_in_micro_dollar = xx.TOTALENGAGEMENTPAGEVISITVALUEINMICRODOLLAR,
                        total_view_page_visit = xx.TOTALVIEWPAGEVISIT,
                        total_view_page_visit_value_in_micro_dollar = xx.TOTALVIEWPAGEVISITVALUEINMICRODOLLAR,
                        total_click_signup = xx.TOTALCLICKSIGNUP,
                        total_click_signup_value_in_micro_dollar = xx.TOTALCLICKSIGNUPVALUEINMICRODOLLAR,
                        total_engagement_signup = xx.TOTALENGAGEMENTSIGNUP,
                        total_engagement_signup_value_in_micro_dollar = xx.TOTALENGAGEMENTSIGNUPVALUEINMICRODOLLAR,
                        total_view_signup = xx.TOTALVIEWSIGNUP,
                        total_view_signup_value_in_micro_dollar = xx.TOTALVIEWSIGNUPVALUEINMICRODOLLAR,
                        total_click_custom = xx.TOTALCLICKCUSTOM,
                        total_click_custom_value_in_micro_dollar = xx.TOTALCLICKCUSTOMVALUEINMICRODOLLAR,
                        total_engagement_custom = xx.TOTALENGAGEMENTCUSTOM,
                        total_engagement_custom_value_in_micro_dollar = xx.TOTALENGAGEMENTCUSTOMVALUEINMICRODOLLAR,
                        total_view_custom = xx.TOTALVIEWCUSTOM,
                        total_view_custom_value_in_micro_dollar = xx.TOTALVIEWCUSTOMVALUEINMICRODOLLAR,
                        video_p25_combined_1 = xx.VIDEOP25COMBINED1,
                        video_p50_combined_1 = xx.VIDEOP50COMBINED1,
                        video_p75_combined_1 = xx.VIDEOP75COMBINED1,
                        video_p95_combined_1 = xx.VIDEOP95COMBINED1,
                        total_conversions = xx.TOTALCONVERSIONS,
                        outbound_click_1 = xx.OUTBOUND_CLICK_1,
                        outbound_click_2 = xx.OUTBOUND_CLICK_2,
                        video_3sec_views_1 = xx.VIDEO3SECVIEWS1,
                        video_3sec_views_2 = xx.VIDEO3SECVIEWS2,
                        video_p25_combined_2 = xx.VIDEOP25COMBINED2,
                        video_p50_combined_2 = xx.VIDEOP50COMBINED2,
                        video_p75_combined_2 = xx.VIDEOP75COMBINED2,
                        video_p95_combined_2 = xx.VIDEOP95COMBINED2,
                        video_p100_complete_2 = xx.VIDEOP100COMPLETE2,
                        total_web_sessions = xx.TOTALWEBSESSIONS,
                        web_sessions_1 = xx.WEBSESSIONS1,
                        web_sessions_2 = xx.WEBSESSIONS2,
                        total_click_lead = xx.TOTALCLICKLEAD,
                        total_click_lead_value_in_micro_dollar = xx.TOTALCLICKLEADVALUEINMICRODOLLAR,
                        total_engagement_lead = xx.TOTALENGAGEMENTLEAD,
                        total_engagement_lead_value_in_micro_dollar = xx.TOTALENGAGEMENTLEADVALUEINMICRODOLLAR,
                        total_view_lead = xx.TOTALVIEWLEAD,
                        total_view_lead_value_in_micro_dollar = xx.TOTALVIEWLEADVALUEINMICRODOLLAR,
                        targeting_type = xx.TARGETINGTYPE,
                        targeting_value = xx.TARGETINGVALUE
                    }));

            var deliveryMetricObjectToSerialize = JArray.FromObject(pinPromotionDeliveryReport);
            return writeObjectToFile(deliveryMetricObjectToSerialize, queue.EntityID, queue.FileDate, fileName);
        }

        public static FileCollectionItem StageDeliveryCatalogSales(List<DeliveryCatalogSalesMetricReport> deliveryReport, Queue queue, Func<JArray, string, DateTime, string, FileCollectionItem> writeObjectToFile, string fileName, APIEntity entity)
        {
            var pinPromotionDeliveryReport = deliveryReport
                .Where(y => y.DeliveryMetrics != null)
                .SelectMany(x => x.DeliveryMetrics
                    .Select(xx => new
                    {
                        account_id = queue.EntityID,
                        account_name = entity.APIEntityName,
                        date = xx.DATE,
                        campaign_id = xx.CAMPAIGNID,
                        campaign_name = xx.CAMPAIGNNAME,
                        campaign_status = xx.CAMPAIGNSTATUS,
                        ad_group_id = xx.ADGROUPID,
                        ad_group_name = xx.ADGROUPNAME,
                        ad_group_status = xx.ADGROUPSTATUS,
                        clickthrough_1 = xx.CLICKTHROUGH1,
                        clickthrough_2 = xx.CLICKTHROUGH2,
                        engagement_1 = xx.ENGAGEMENT1,
                        engagement_2 = xx.ENGAGEMENT2,
                        impression_1 = xx.IMPRESSION1,
                        impression_2 = xx.IMPRESSION2,
                        outbound_click_1 = xx.OUTBOUNDCLICK1,
                        outbound_click_2 = xx.OUTBOUNDCLICK2,
                        product_group_id = xx.PRODUCTGROUPID,
                        product_group_name = xx.PROMOTEDCATALOGPRODUCTGROUPREFERENCENAME,
                        repin_1 = xx.REPIN1,
                        repin_2 = xx.REPIN2,
                        spend_in_micro_dollar = xx.SPENDINMICRODOLLAR,
                        total_click_add_to_cart = xx.TOTALCLICKADDTOCART,
                        total_click_add_to_cart_value_in_micro_dollar = xx.TOTALCLICKADDTOCARTVALUEINMICRODOLLAR,
                        total_click_checkout = xx.TOTALCLICKCHECKOUT,
                        total_click_checkout_value_in_micro_dollar = xx.TOTALCLICKCHECKOUTVALUEINMICRODOLLAR,
                        total_click_custom = xx.TOTALCLICKCUSTOM,
                        total_click_custom_value_in_micro_dollar = xx.TOTALCLICKCUSTOMVALUEINMICRODOLLAR,
                        total_click_page_visit = xx.TOTALCLICKPAGEVISIT,
                        total_click_page_visit_value_in_micro_dollar = xx.TOTALCLICKPAGEVISITVALUEINMICRODOLLAR,
                        total_click_signup = xx.TOTALCLICKSIGNUP,
                        total_click_signup_value_in_micro_dollar = xx.TOTALCLICKSIGNUPVALUEINMICRODOLLAR,
                        total_conversions = xx.TOTALCONVERSIONS,
                        total_engagement_add_to_cart = xx.TOTALENGAGEMENTADDTOCART,
                        total_engagement_add_to_cart_value_in_micro_dollar = xx.TOTALENGAGEMENTADDTOCARTVALUEINMICRODOLLAR,
                        total_engagement_checkout = xx.TOTALENGAGEMENTCHECKOUT,
                        total_engagement_checkout_value_in_micro_dollar = xx.TOTALENGAGEMENTCHECKOUTVALUEINMICRODOLLAR,
                        total_engagement_custom = xx.TOTALENGAGEMENTCUSTOM,
                        total_engagement_custom_value_in_micro_dollar = xx.TOTALENGAGEMENTCUSTOMVALUEINMICRODOLLAR,
                        total_engagement_page_visit = xx.TOTALENGAGEMENTPAGEVISIT,
                        total_engagement_page_visit_value_in_micro_dollar = xx.TOTALENGAGEMENTPAGEVISITVALUEINMICRODOLLAR,
                        total_engagement_signup = xx.TOTALENGAGEMENTSIGNUP,
                        total_engagement_signup_value_in_micro_dollar = xx.TOTALENGAGEMENTSIGNUPVALUEINMICRODOLLAR,
                        total_view_add_to_cart = xx.TOTALVIEWADDTOCART,
                        total_view_add_to_cart_value_in_micro_dollar = xx.TOTALVIEWADDTOCARTVALUEINMICRODOLLAR,
                        total_view_checkout = xx.TOTALVIEWCHECKOUT,
                        total_view_checkout_value_in_micro_dollar = xx.TOTALVIEWCHECKOUTVALUEINMICRODOLLAR,
                        total_view_custom = xx.TOTALVIEWCUSTOM,
                        total_view_custom_value_in_micro_dollar = xx.TOTALVIEWCUSTOMVALUEINMICRODOLLAR,
                        total_view_page_visit = xx.TOTALVIEWPAGEVISIT,
                        total_view_page_visit_value_in_micro_dollar = xx.TOTALVIEWPAGEVISITVALUEINMICRODOLLAR,
                        total_view_signup = xx.TOTALVIEWSIGNUP,
                        total_view_signup_value_in_micro_dollar = xx.TOTALVIEWSIGNUPVALUEINMICRODOLLAR,
                        total_web_sessions = xx.TOTALWEBSESSIONS,
                        web_sessions_1 = xx.WEBSESSIONS1,
                        web_sessions_2 = xx.WEBSESSIONS2,
                        video_p0_combined_1 = xx.VIDEOP0COMBINED1,
                        video_p0_combined_2 = xx.VIDEOP0COMBINED2,
                        video_3sec_views_1 = xx.VIDEO3SECVIEWS1,
                        video_3sec_views_2 = xx.VIDEO3SECVIEWS2,
                        video_p95_combined_1 = xx.VIDEOP95COMBINED1,
                        video_p95_combined_2 = xx.VIDEOP95COMBINED2,
                        total_click_lead = xx.TOTALCLICKLEAD,
                        total_click_lead_value_in_micro_dollar = xx.TOTALCLICKLEADVALUEINMICRODOLLAR,
                        total_engagement_lead = xx.TOTALENGAGEMENTLEAD,
                        total_engagement_lead_value_in_micro_dollar = xx.TOTALENGAGEMENTLEADVALUEINMICRODOLLAR,
                        total_view_lead = xx.TOTALVIEWLEAD,
                        total_view_lead_value_in_micro_dollar = xx.TOTALVIEWLEADVALUEINMICRODOLLAR
                    }));

            var deliveryMetricObjectToSerialize = JArray.FromObject(pinPromotionDeliveryReport);
            return writeObjectToFile(deliveryMetricObjectToSerialize, queue.EntityID, queue.FileDate, fileName);
        }

        public static FileCollectionItem StageConversion(List<DeliveryMetricReport> deliveryReport, Queue queue, Func<JArray, string, DateTime, string, FileCollectionItem> writeObjectToFile, string fileName)
        {
            var pinPromotionDeliveryReport = deliveryReport
                .Where(y => y.DeliveryMetrics != null)
                .SelectMany(x => x.DeliveryMetrics
                    .Select(xx => new
                    {
                        account_id = queue.EntityID,
                        date = xx.DATE,
                        campaign_id = xx.PINPROMOTIONCAMPAIGNID,
                        campaign_name = xx.PINPROMOTIONCAMPAIGNNAME,
                        campaign_status = xx.PINPROMOTIONCAMPAIGNSTATUS,
                        ad_group_id = xx.PINPROMOTIONADGROUPID,
                        ad_group_name = xx.PINPROMOTIONADGROUPNAME,
                        ad_group_status = xx.PINPROMOTIONADGROUPSTATUS,
                        pin_promotion_id = xx.PINPROMOTIONID,
                        pin_promotion_name = xx.PINPROMOTIONNAME,
                        pin_promotion_status = xx.PINPROMOTIONSTATUS,
                        pin_id = xx.PINID,
                        total_click_add_to_cart = xx.TOTALCLICKADDTOCART,
                        total_engagement_add_to_cart = xx.TOTALENGAGEMENTADDTOCART,
                        total_view_add_to_cart = xx.TOTALVIEWADDTOCART,
                        total_click_checkout = xx.TOTALCLICKCHECKOUT,
                        total_engagement_checkout = xx.TOTALENGAGEMENTCHECKOUT,
                        total_view_checkout = xx.TOTALVIEWCHECKOUT,
                        total_click_page_visit = xx.TOTALCLICKPAGEVISIT,
                        total_engagement_page_visit = xx.TOTALENGAGEMENTPAGEVISIT,
                        total_view_page_visit = xx.TOTALVIEWPAGEVISIT,
                        total_click_signup = xx.TOTALCLICKSIGNUP,
                        total_engagement_signup = xx.TOTALENGAGEMENTSIGNUP,
                        total_view_signup = xx.TOTALVIEWSIGNUP,
                        total_click_custom = xx.TOTALCLICKCUSTOM,
                        total_engagement_custom = xx.TOTALENGAGEMENTCUSTOM,
                        total_view_custom = xx.TOTALVIEWCUSTOM,
                        total_conversions = xx.TOTALCONVERSIONS,
                        total_click_lead = xx.TOTALCLICKLEAD,
                        total_engagement_lead = xx.TOTALENGAGEMENTLEAD,
                        total_view_lead = xx.TOTALVIEWLEAD,
                        click_through_value_lead = xx.TOTALCLICKLEADVALUEINMICRODOLLAR,
                        click_through_order_value_add_to_cart = xx.TOTALCLICKADDTOCARTVALUEINMICRODOLLAR,
                        click_through_order_value_checkout = xx.TOTALCLICKCHECKOUTVALUEINMICRODOLLAR,
                        click_through_value_page_visit = xx.TOTALCLICKPAGEVISITVALUEINMICRODOLLAR,
                        click_through_value_signup = xx.TOTALCLICKSIGNUPVALUEINMICRODOLLAR,
                        click_through_value_custom = xx.TOTALCLICKCUSTOMVALUEINMICRODOLLAR,
                        view_through_value_lead = xx.TOTALVIEWLEADVALUEINMICRODOLLAR,
                        view_through_order_value_add_to_cart = xx.TOTALVIEWADDTOCARTVALUEINMICRODOLLAR,
                        view_through_order_value_checkout = xx.TOTALVIEWCHECKOUTVALUEINMICRODOLLAR,
                        view_through_value_page_visit = xx.TOTALVIEWPAGEVISITVALUEINMICRODOLLAR,
                        view_through_value_signup = xx.TOTALVIEWSIGNUPVALUEINMICRODOLLAR,
                        view_through_value_custom = xx.TOTALVIEWCUSTOMVALUEINMICRODOLLAR,
                        engagement_value_lead = xx.TOTALENGAGEMENTLEADVALUEINMICRODOLLAR,
                        engagement_order_value_add_to_cart = xx.TOTALENGAGEMENTADDTOCARTVALUEINMICRODOLLAR,
                        engagement_order_value_checkout = xx.TOTALENGAGEMENTCHECKOUTVALUEINMICRODOLLAR,
                        engagement_value_page_visit = xx.TOTALENGAGEMENTPAGEVISITVALUEINMICRODOLLAR,
                        engagement_value_signup = xx.TOTALENGAGEMENTSIGNUPVALUEINMICRODOLLAR,
                        engagement_value_custom = xx.TOTALENGAGEMENTCUSTOMVALUEINMICRODOLLAR
                    }));

            var deliveryMetricObjectToSerialize = JArray.FromObject(pinPromotionDeliveryReport);
            return writeObjectToFile(deliveryMetricObjectToSerialize, queue.EntityID, queue.FileDate, fileName);
        }

        public static FileCollectionItem StageAdGroupDim(List<AdGroupDimReport.AdGroupDim> deliveryReport, Queue queue, Func<JArray, string, DateTime, string, FileCollectionItem> writeObjectToFile, string fileName)
        {
            var report = deliveryReport
                    .Select(x => new
                    {
                        start_time = x.StartTime,
                        name = x.Name,
                        placement_group = x.PlacementGroup,
                        end_time = x.EndTime,
                        pacing_delivery_type = x.PacingDeliveryType,
                        campaign_id = x.CampaignId,
                        budget_type = x.BudgetType,
                        auto_targeting_enabled = x.AutoTargetingEnabled,
                        status = x.Status,
                        budget_in_micro_currency = x.BudgetInMicroCurrency,
                        tracking_urls = x.TrackingUrls,
                        billable_event = x.BillableEvent,
                        bid_in_micro_currency = x.BidInMicroCurrency,
                        dca_assets = x.DcaAssets,
                        id = x.Id,
                        summary_status = x.SummaryStatus,
                        feed_profile_id = x.FeedProfileId,
                        updated_time = x.UpdatedTime,
                        targeting_spec = x.TargetingSpec,
                        created_time = x.CreatedTime,
                        type = x.Type,
                        bid_strategy_type = x.BidStrategyType,
                        advertiser_id = x.AdAccountId,
                        conversion_learning_mode_type = x.ConversionLearningModeType,
                        optimization_goal_metadata = x.OptimizationGoalMetadata
                    });

            var objectToSerialize = JArray.FromObject(report);
            return writeObjectToFile(objectToSerialize, queue.EntityID, queue.FileDate, fileName);
        }

        public static FileCollectionItem StageAdsDim(List<AdsDimReport.AdsDim> deliveryReport, Queue queue, Func<JArray, string, DateTime, string, FileCollectionItem> writeObjectToFile, string fileName)
        {
            var report = deliveryReport
                    .Select(x => new
                    {
                        id = x.Id,
                        ad_group_id = x.AdGroupId,
                        ad_account_id = x.AdAccountId,
                        android_deep_link = x.AndroidDeepLink,
                        campaign_id = x.CampaignId,
                        carousel_android_deep_links = x.CarouselAndroidDeepLinks,
                        carousel_destination_urls = x.CarouselDestinationUrls,
                        carousel_ios_deep_links = x.CarouselIosDeepLinks,
                        click_tracking_url = x.ClickTrackingUrl,
                        collection_items_destination_url_template = x.CollectionItemsDestinationUrlTemplate,
                        created_time = x.CreatedTimeEpoch,
                        creative_type = x.CreativeType,
                        destination_url = x.DestinationUrl,
                        ios_deep_link = x.IosDeepLink,
                        is_pin_deleted = x.IsPinDeleted,
                        is_removable = x.IsRemovable,
                        name = x.Name,
                        pin_id = x.PinId,
                        rejected_reasons = x.RejectedReasons,
                        rejection_labels = x.RejectionLabels,
                        review_status = x.ReviewStatus,
                        status = x.Status,
                        summary_status = x.SummaryStatus,
                        tracking_urls = x.TrackingUrls,
                        type = x.Type,
                        updated_time = x.UpdatedTimeEpoch,
                        view_tracking_url = x.ViewTrackingUrl,
                        lead_form_id = x.LeadFormId,
                        quiz_pin_data = x.QuizPinData,
                        grid_click_type = x.GridClickType,
                        customizable_cta_type = x.CustomizableCtaType
                    });


            var objectToSerialize = JArray.FromObject(report);
            return writeObjectToFile(objectToSerialize, queue.EntityID, queue.FileDate, fileName);
        }
        public static FileCollectionItem StageCampaignDim(List<CampaignDimReport.Campaign> deliveryReport, Queue queue, Func<JArray, string, DateTime, string, FileCollectionItem> writeObjectToFile, string fileName)
        {
            var report = deliveryReport
                    .Select(x => new
                    {
                        order_line_id = x.OrderLineId,
                        objective_type = x.ObjectiveType,
                        updated_time = x.UpdatedTime,
                        created_time = x.CreatedTime,
                        name = x.Name,
                        type = x.Type,
                        advertiser_id = x.AdAccountId,
                        id = x.Id,
                        start_time = x.StartTime,
                        flexible_daily_budgets_enabled = x.IsFlexibleDailyBudgets,
                        lifetime_spend_cap = x.LifetimeSpendCap,
                        end_time = x.EndTime,
                        summary_status = x.SummaryStatus,
                        daily_spend_cap = x.DailySpendCap,
                        status = x.Status,
                        tracking_urls = x.TrackingUrls,
                        campaign_budget_optimization_enabled = x.IsCampaignBudgetOptimization
                    });

            var objectToSerialize = JArray.FromObject(report);
            return writeObjectToFile(objectToSerialize, queue.EntityID, queue.FileDate, fileName);
        }
    }
}
