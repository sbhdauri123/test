using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Pinterest
{
    public class DeliveryCatalogSalesMetricReport : IMetricReport<DeliveryCatalogSalesMetrics>
    {
        public string EntityID { get; set; }

        public List<DeliveryCatalogSalesMetrics> DeliveryMetrics { get; set; }
    }

    public class DeliveryCatalogSalesMetrics
    {
        [JsonProperty("DATE")]
        public string DATE { get; set; }
        [JsonProperty("CLICKTHROUGH_1")]
        public string CLICKTHROUGH1 { get; set; }
        [JsonProperty("CLICKTHROUGH_2")]
        public string CLICKTHROUGH2 { get; set; }
        [JsonProperty("ENGAGEMENT_1")]
        public string ENGAGEMENT1 { get; set; }
        [JsonProperty("ENGAGEMENT_2")]
        public string ENGAGEMENT2 { get; set; }
        [JsonProperty("IMPRESSION_1")]
        public string IMPRESSION1 { get; set; }
        [JsonProperty("IMPRESSION_2")]
        public string IMPRESSION2 { get; set; }
        [JsonProperty("OUTBOUND_CLICK_1")]
        public string OUTBOUNDCLICK1 { get; set; }
        [JsonProperty("OUTBOUND_CLICK_2")]
        public string OUTBOUNDCLICK2 { get; set; }
        [JsonProperty("REPIN_1")]
        public string REPIN1 { get; set; }
        [JsonProperty("REPIN_2")]
        public string REPIN2 { get; set; }
        [JsonProperty("TOTAL_CLICK_LEAD")]
        public string TOTALCLICKLEAD { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTLEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKLEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("SPEND_IN_MICRO_DOLLAR")]
        public string SPENDINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_ADD_TO_CART")]
        public string TOTALCLICKADDTOCART { get; set; }
        [JsonProperty("TOTAL_CLICK_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_CHECKOUT")]
        public string TOTALCLICKCHECKOUT { get; set; }
        [JsonProperty("TOTAL_CLICK_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_CUSTOM")]
        public string TOTALCLICKCUSTOM { get; set; }
        [JsonProperty("TOTAL_CLICK_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKCUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_PAGE_VISIT")]
        public string TOTALCLICKPAGEVISIT { get; set; }
        [JsonProperty("TOTAL_CLICK_PAGE_VISIT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKPAGEVISITVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_SIGNUP")]
        public string TOTALCLICKSIGNUP { get; set; }
        [JsonProperty("TOTAL_CLICK_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CONVERSIONS")]
        public string TOTALCONVERSIONS { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_ADD_TO_CART")]
        public string TOTALENGAGEMENTADDTOCART { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_CHECKOUT")]
        public string TOTALENGAGEMENTCHECKOUT { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_CUSTOM")]
        public string TOTALENGAGEMENTCUSTOM { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTCUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_LEAD")]
        public string TOTALENGAGEMENTLEAD { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_PAGE_VISIT")]
        public string TOTALENGAGEMENTPAGEVISIT { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_PAGE_VISIT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTPAGEVISITVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_SIGNUP")]
        public string TOTALENGAGEMENTSIGNUP { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_ADD_TO_CART")]
        public string TOTALVIEWADDTOCART { get; set; }
        [JsonProperty("TOTAL_VIEW_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_CHECKOUT")]
        public string TOTALVIEWCHECKOUT { get; set; }
        [JsonProperty("TOTAL_VIEW_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_CUSTOM")]
        public string TOTALVIEWCUSTOM { get; set; }
        [JsonProperty("TOTAL_VIEW_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWCUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_LEAD")]
        public string TOTALVIEWLEAD { get; set; }
        [JsonProperty("TOTAL_VIEW_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWLEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_PAGE_VISIT")]
        public string TOTALVIEWPAGEVISIT { get; set; }
        [JsonProperty("TOTAL_VIEW_PAGE_VISIT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWPAGEVISITVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_SIGNUP")]
        public string TOTALVIEWSIGNUP { get; set; }
        [JsonProperty("TOTAL_VIEW_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("AD_GROUP_ID")]
        public string ADGROUPID { get; set; }
        [JsonProperty("AD_GROUP_NAME")]
        public string ADGROUPNAME { get; set; }
        [JsonProperty("AD_GROUP_STATUS")]
        public string ADGROUPSTATUS { get; set; }
        [JsonProperty("CAMPAIGN_ID")]
        public string CAMPAIGNID { get; set; }
        [JsonProperty("CAMPAIGN_NAME")]
        public string CAMPAIGNNAME { get; set; }
        [JsonProperty("CAMPAIGN_STATUS")]
        public string CAMPAIGNSTATUS { get; set; }
        [JsonProperty("PRODUCT_GROUP_ID")]
        public string PRODUCTGROUPID { get; set; }
        [JsonProperty("PROMOTED_CATALOG_PRODUCT_GROUP_REFERENCE_NAME")]
        public string PROMOTEDCATALOGPRODUCTGROUPREFERENCENAME { get; set; }
        [JsonProperty("VIDEO_P0_COMBINED_1")]
        public string VIDEOP0COMBINED1 { get; set; }
        [JsonProperty("VIDEO_P0_COMBINED_2")]
        public string VIDEOP0COMBINED2 { get; set; }
        [JsonProperty("VIDEO_3SEC_VIEWS_1")]
        public string VIDEO3SECVIEWS1 { get; set; }
        [JsonProperty("VIDEO_3SEC_VIEWS_2")]
        public string VIDEO3SECVIEWS2 { get; set; }
        [JsonProperty("VIDEO_P95_COMBINED_1")]
        public string VIDEOP95COMBINED1 { get; set; }
        [JsonProperty("VIDEO_P95_COMBINED_2")]
        public string VIDEOP95COMBINED2 { get; set; }
        [JsonProperty("TOTAL_WEB_SESSIONS")]
        public string TOTALWEBSESSIONS { get; set; }
        [JsonProperty("WEB_SESSIONS_1")]
        public string WEBSESSIONS1 { get; set; }
        [JsonProperty("WEB_SESSIONS_2")]
        public string WEBSESSIONS2 { get; set; }
    }
}