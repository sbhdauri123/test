using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Pinterest
{
    public class DeliveryMetricReport : IMetricReport<DeliveryMetrics>
    {
        public string EntityID { get; set; }
        public List<DeliveryMetrics> DeliveryMetrics { get; set; }
    }

    public class DeliveryMetrics
    {
        [JsonProperty("DATE")]
        public string DATE { get; set; }
        [JsonProperty("AD_GROUP_CAMPAIGN_STATUS")]
        public string ADGROUPCAMPAIGNSTATUS { get; set; }
        [JsonProperty("AD_GROUP_CAMPAIGN_NAME")]
        public string ADGROUPCAMPAIGNNAME { get; set; }
        [JsonProperty("AD_GROUP_CAMPAIGN_ID")]
        public string ADGROUPCAMPAIGNID { get; set; }
        [JsonProperty("AD_GROUP_NAME")]
        public string PINPROMOTIONADGROUPNAME { get; set; }
        [JsonProperty("CAMPAIGN_STATUS")]
        public string PINPROMOTIONCAMPAIGNSTATUS { get; set; }
        [JsonProperty("CAMPAIGN_ID")]
        public string PINPROMOTIONCAMPAIGNID { get; set; }
        [JsonProperty("PIN_PROMOTION_ID")]
        public string PINPROMOTIONID { get; set; }
        [JsonProperty("PIN_ID")]
        public string PINID { get; set; }
        [JsonProperty("AD_GROUP_STATUS")]
        public string PINPROMOTIONADGROUPSTATUS { get; set; }
        [JsonProperty("CAMPAIGN_NAME")]
        public string PINPROMOTIONCAMPAIGNNAME { get; set; }
        [JsonProperty("AD_GROUP_ID")]
        public string PINPROMOTIONADGROUPID { get; set; }
        [JsonProperty("PIN_PROMOTION_NAME")]
        public string PINPROMOTIONNAME { get; set; }
        [JsonProperty("PIN_PROMOTION_STATUS")]
        public string PINPROMOTIONSTATUS { get; set; }
        [JsonProperty("TARGETING_VALUE")]
        public string TARGETINGVALUE { get; set; }
        [JsonProperty("TARGETING_TYPE")]
        public string TARGETINGTYPE { get; set; }

        #region metrics
        [JsonProperty("APP_INSTALL_COST_PER_ACTION")]
        public string APPINSTALLCOSTPERACTION { get; set; }
        [JsonProperty("CLICKTHROUGH_1")]
        public string CLICKTHROUGH1 { get; set; }
        [JsonProperty("CLICKTHROUGH_1_GROSS")]
        public string CLICKTHROUGH1GROSS { get; set; }
        [JsonProperty("CLICKTHROUGH_2")]
        public string CLICKTHROUGH2 { get; set; }
        [JsonProperty("CLOSEUP_1")]
        public string CLOSEUP1 { get; set; }
        [JsonProperty("CLOSEUP_2")]
        public string CLOSEUP2 { get; set; }
        [JsonProperty("CPCV_IN_MICRO_DOLLAR")]
        public string CPCVINMICRODOLLAR { get; set; }
        [JsonProperty("CPCV_P95_IN_MICRO_DOLLAR")]
        public string CPCVP95INMICRODOLLAR { get; set; }
        [JsonProperty("CPV_IN_MICRO_DOLLAR")]
        public string CPVINMICRODOLLAR { get; set; }
        [JsonProperty("ENGAGEMENT_1")]
        public string ENGAGEMENT1 { get; set; }
        [JsonProperty("ENGAGEMENT_2")]
        public string ENGAGEMENT2 { get; set; }
        [JsonProperty("IMPRESSION_1")]
        public string IMPRESSION1 { get; set; }
        [JsonProperty("IMPRESSION_1_GROSS")]
        public string IMPRESSION1GROSS { get; set; }
        [JsonProperty("IMPRESSION_2")]
        public string IMPRESSION2 { get; set; }
        [JsonProperty("INAPP_ADD_TO_CART_COST_PER_ACTION")]
        public string INAPPADDTOCARTCOSTPERACTION { get; set; }
        [JsonProperty("INAPP_ADD_TO_CART_ROAS")]
        public string INAPPADDTOCARTROAS { get; set; }
        [JsonProperty("INAPP_APP_INSTALL_COST_PER_ACTION")]
        public string INAPPAPPINSTALLCOSTPERACTION { get; set; }
        [JsonProperty("INAPP_APP_INSTALL_ROAS")]
        public string INAPPAPPINSTALLROAS { get; set; }
        [JsonProperty("INAPP_CHECKOUT_COST_PER_ACTION")]
        public string INAPPCHECKOUTCOSTPERACTION { get; set; }
        [JsonProperty("INAPP_CHECKOUT_ROAS")]
        public string INAPPCHECKOUTROAS { get; set; }
        [JsonProperty("INAPP_SEARCH_COST_PER_ACTION")]
        public string INAPPSEARCHCOSTPERACTION { get; set; }
        [JsonProperty("INAPP_SEARCH_ROAS")]
        public string INAPPSEARCHROAS { get; set; }
        [JsonProperty("INAPP_SIGNUP_COST_PER_ACTION")]
        public string INAPPSIGNUPCOSTPERACTION { get; set; }
        [JsonProperty("INAPP_SIGNUP_ROAS")]
        public string INAPPSIGNUPROAS { get; set; }
        [JsonProperty("INAPP_UNKNOWN_COST_PER_ACTION")]
        public string INAPPUNKNOWNCOSTPERACTION { get; set; }
        [JsonProperty("INAPP_UNKNOWN_ROAS")]
        public string INAPPUNKNOWNROAS { get; set; }
        [JsonProperty("OFFLINE_CHECKOUT_COST_PER_ACTION")]
        public string OFFLINECHECKOUTCOSTPERACTION { get; set; }
        [JsonProperty("OFFLINE_CHECKOUT_ROAS")]
        public string OFFLINECHECKOUTROAS { get; set; }
        [JsonProperty("OFFLINE_CUSTOM_COST_PER_ACTION")]
        public string OFFLINECUSTOMCOSTPERACTION { get; set; }
        [JsonProperty("OFFLINE_CUSTOM_ROAS")]
        public string OFFLINECUSTOMROAS { get; set; }
        [JsonProperty("OFFLINE_LEAD_COST_PER_ACTION")]
        public string OFFLINELEADCOSTPERACTION { get; set; }
        [JsonProperty("OFFLINE_LEAD_ROAS")]
        public string OFFLINELEADROAS { get; set; }
        [JsonProperty("OFFLINE_SIGNUP_COST_PER_ACTION")]
        public string OFFLINESIGNUPCOSTPERACTION { get; set; }
        [JsonProperty("OFFLINE_SIGNUP_ROAS")]
        public string OFFLINESIGNUPROAS { get; set; }
        [JsonProperty("OFFLINE_UNKNOWN_COST_PER_ACTION")]
        public string OFFLINEUNKNOWNCOSTPERACTION { get; set; }
        [JsonProperty("OFFLINE_UNKNOWN_ROAS")]
        public string OFFLINEUNKNOWNROAS { get; set; }
        [JsonProperty("ONSITE_CHECKOUTS")]
        public string ONSITECHECKOUTS { get; set; }
        [JsonProperty("ONSITE_CHECKOUTS_CPA_BILLABLE")]
        public string ONSITECHECKOUTSCPABILLABLE { get; set; }
        [JsonProperty("ONSITE_CHECKOUTS_VALUE")]
        public string ONSITECHECKOUTSVALUE { get; set; }
        [JsonProperty("REPIN_1")]
        public string REPIN1 { get; set; }
        [JsonProperty("REPIN_2")]
        public string REPIN2 { get; set; }
        [JsonProperty("SPEND_IN_MICRO_DOLLAR")]
        public string SPENDINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ADD_TO_CART_DESKTOP_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALADDTOCARTDESKTOPACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_ADD_TO_CART_DESKTOP_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALADDTOCARTDESKTOPACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_ADD_TO_CART_DESKTOP_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALADDTOCARTDESKTOPACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_ADD_TO_CART_MOBILE_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALADDTOCARTMOBILEACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_ADD_TO_CART_MOBILE_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALADDTOCARTMOBILEACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_ADD_TO_CART_MOBILE_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALADDTOCARTMOBILEACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_ADD_TO_CART_TABLET_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALADDTOCARTTABLETACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_ADD_TO_CART_TABLET_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALADDTOCARTTABLETACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_ADD_TO_CART_TABLET_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALADDTOCARTTABLETACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_APP_INSTALL")]
        public string TOTALAPPINSTALL { get; set; }
        [JsonProperty("TOTAL_APP_INSTALL_DESKTOP_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALAPPINSTALLDESKTOPACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_APP_INSTALL_DESKTOP_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALAPPINSTALLDESKTOPACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_APP_INSTALL_DESKTOP_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALAPPINSTALLDESKTOPACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_APP_INSTALL_MOBILE_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALAPPINSTALLMOBILEACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_APP_INSTALL_MOBILE_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALAPPINSTALLMOBILEACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_APP_INSTALL_MOBILE_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALAPPINSTALLMOBILEACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_APP_INSTALL_TABLET_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALAPPINSTALLTABLETACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_APP_INSTALL_TABLET_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALAPPINSTALLTABLETACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_APP_INSTALL_TABLET_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALAPPINSTALLTABLETACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_APP_INSTALL_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALAPPINSTALLVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CHECKOUT_DESKTOP_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALCHECKOUTDESKTOPACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_CHECKOUT_DESKTOP_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALCHECKOUTDESKTOPACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_CHECKOUT_DESKTOP_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALCHECKOUTDESKTOPACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_CHECKOUT_MOBILE_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALCHECKOUTMOBILEACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_CHECKOUT_MOBILE_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALCHECKOUTMOBILEACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_CHECKOUT_MOBILE_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALCHECKOUTMOBILEACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_CHECKOUT_TABLET_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALCHECKOUTTABLETACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_CHECKOUT_TABLET_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALCHECKOUTTABLETACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_CHECKOUT_TABLET_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALCHECKOUTTABLETACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_CLICK_ADD_TO_CART")]
        public string TOTALCLICKADDTOCART { get; set; }
        [JsonProperty("TOTAL_CLICK_ADD_TO_CART_QUANTITY")]
        public string TOTALCLICKADDTOCARTQUANTITY { get; set; }
        [JsonProperty("TOTAL_CLICK_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_APP_INSTALL")]
        public string TOTALCLICKAPPINSTALL { get; set; }
        [JsonProperty("TOTAL_CLICK_APP_INSTALL_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKAPPINSTALLVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_CHECKOUT")]
        public string TOTALCLICKCHECKOUT { get; set; }
        [JsonProperty("TOTAL_CLICK_CHECKOUT_QUANTITY")]
        public string TOTALCLICKCHECKOUTQUANTITY { get; set; }
        [JsonProperty("TOTAL_CLICK_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_CUSTOM")]
        public string TOTALCLICKCUSTOM { get; set; }
        [JsonProperty("TOTAL_CLICK_CUSTOM_QUANTITY")]
        public string TOTALCLICKCUSTOMQUANTITY { get; set; }
        [JsonProperty("TOTAL_CLICK_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKCUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_LEAD")]
        public string TOTALCLICKLEAD { get; set; }
        [JsonProperty("TOTAL_CLICK_LEAD_QUANTITY")]
        public string TOTALCLICKLEADQUANTITY { get; set; }
        [JsonProperty("TOTAL_CLICK_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKLEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_PAGE_VISIT")]
        public string TOTALCLICKPAGEVISIT { get; set; }
        [JsonProperty("TOTAL_CLICK_PAGE_VISIT_QUANTITY")]
        public string TOTALCLICKPAGEVISITQUANTITY { get; set; }
        [JsonProperty("TOTAL_CLICK_PAGE_VISIT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKPAGEVISITVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_SEARCH")]
        public string TOTALCLICKSEARCH { get; set; }
        [JsonProperty("TOTAL_CLICK_SEARCH_QUANTITY")]
        public string TOTALCLICKSEARCHQUANTITY { get; set; }
        [JsonProperty("TOTAL_CLICK_SEARCH_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKSEARCHVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_SIGNUP")]
        public string TOTALCLICKSIGNUP { get; set; }
        [JsonProperty("TOTAL_CLICK_SIGNUP_QUANTITY")]
        public string TOTALCLICKSIGNUPQUANTITY { get; set; }
        [JsonProperty("TOTAL_CLICK_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_UNKNOWN")]
        public string TOTALCLICKUNKNOWN { get; set; }
        [JsonProperty("TOTAL_CLICK_UNKNOWN_QUANTITY")]
        public string TOTALCLICKUNKNOWNQUANTITY { get; set; }
        [JsonProperty("TOTAL_CLICK_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_VIEW_CATEGORY")]
        public string TOTALCLICKVIEWCATEGORY { get; set; }
        [JsonProperty("TOTAL_CLICK_VIEW_CATEGORY_QUANTITY")]
        public string TOTALCLICKVIEWCATEGORYQUANTITY { get; set; }
        [JsonProperty("TOTAL_CLICK_VIEW_CATEGORY_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKVIEWCATEGORYVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CLICK_WATCH_VIDEO")]
        public string TOTALCLICKWATCHVIDEO { get; set; }
        [JsonProperty("TOTAL_CLICK_WATCH_VIDEO_QUANTITY")]
        public string TOTALCLICKWATCHVIDEOQUANTITY { get; set; }
        [JsonProperty("TOTAL_CLICK_WATCH_VIDEO_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCLICKWATCHVIDEOVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CONVERSIONS")]
        public string TOTALCONVERSIONS { get; set; }
        [JsonProperty("TOTAL_CONVERSIONS_QUANTITY")]
        public string TOTALCONVERSIONSQUANTITY { get; set; }
        [JsonProperty("TOTAL_CONVERSIONS_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALCONVERSIONSVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_CUSTOM_DESKTOP_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALCUSTOMDESKTOPACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_CUSTOM_DESKTOP_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALCUSTOMDESKTOPACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_CUSTOM_DESKTOP_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALCUSTOMDESKTOPACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_CUSTOM_MOBILE_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALCUSTOMMOBILEACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_CUSTOM_MOBILE_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALCUSTOMMOBILEACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_CUSTOM_MOBILE_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALCUSTOMMOBILEACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_CUSTOM_TABLET_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALCUSTOMTABLETACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_CUSTOM_TABLET_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALCUSTOMTABLETACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_CUSTOM_TABLET_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALCUSTOMTABLETACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_ADD_TO_CART")]
        public string TOTALENGAGEMENTADDTOCART { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_ADD_TO_CART_QUANTITY")]
        public string TOTALENGAGEMENTADDTOCARTQUANTITY { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_APP_INSTALL")]
        public string TOTALENGAGEMENTAPPINSTALL { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_APP_INSTALL_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTAPPINSTALLVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_CHECKOUT")]
        public string TOTALENGAGEMENTCHECKOUT { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_CHECKOUT_QUANTITY")]
        public string TOTALENGAGEMENTCHECKOUTQUANTITY { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_CUSTOM")]
        public string TOTALENGAGEMENTCUSTOM { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_CUSTOM_QUANTITY")]
        public string TOTALENGAGEMENTCUSTOMQUANTITY { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTCUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_LEAD")]
        public string TOTALENGAGEMENTLEAD { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_LEAD_QUANTITY")]
        public string TOTALENGAGEMENTLEADQUANTITY { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTLEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_PAGE_VISIT")]
        public string TOTALENGAGEMENTPAGEVISIT { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_PAGE_VISIT_QUANTITY")]
        public string TOTALENGAGEMENTPAGEVISITQUANTITY { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_PAGE_VISIT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTPAGEVISITVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_SEARCH")]
        public string TOTALENGAGEMENTSEARCH { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_SEARCH_QUANTITY")]
        public string TOTALENGAGEMENTSEARCHQUANTITY { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_SEARCH_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTSEARCHVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_SIGNUP")]
        public string TOTALENGAGEMENTSIGNUP { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_SIGNUP_QUANTITY")]
        public string TOTALENGAGEMENTSIGNUPQUANTITY { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_UNKNOWN")]
        public string TOTALENGAGEMENTUNKNOWN { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_UNKNOWN_QUANTITY")]
        public string TOTALENGAGEMENTUNKNOWNQUANTITY { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_VIEW_CATEGORY")]
        public string TOTALENGAGEMENTVIEWCATEGORY { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_VIEW_CATEGORY_QUANTITY")]
        public string TOTALENGAGEMENTVIEWCATEGORYQUANTITY { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_VIEW_CATEGORY_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTVIEWCATEGORYVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_WATCH_VIDEO")]
        public string TOTALENGAGEMENTWATCHVIDEO { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_WATCH_VIDEO_QUANTITY")]
        public string TOTALENGAGEMENTWATCHVIDEOQUANTITY { get; set; }
        [JsonProperty("TOTAL_ENGAGEMENT_WATCH_VIDEO_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALENGAGEMENTWATCHVIDEOVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_ADD_TO_CART")]
        public string TOTALINAPPADDTOCART { get; set; }
        [JsonProperty("TOTAL_INAPP_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_APP_INSTALL")]
        public string TOTALINAPPAPPINSTALL { get; set; }
        [JsonProperty("TOTAL_INAPP_APP_INSTALL_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPAPPINSTALLVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_CHECKOUT")]
        public string TOTALINAPPCHECKOUT { get; set; }
        [JsonProperty("TOTAL_INAPP_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_CLICK_ADD_TO_CART")]
        public string TOTALINAPPCLICKADDTOCART { get; set; }
        [JsonProperty("TOTAL_INAPP_CLICK_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPCLICKADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_CLICK_APP_INSTALL")]
        public string TOTALINAPPCLICKAPPINSTALL { get; set; }
        [JsonProperty("TOTAL_INAPP_CLICK_APP_INSTALL_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPCLICKAPPINSTALLVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_CLICK_CHECKOUT")]
        public string TOTALINAPPCLICKCHECKOUT { get; set; }
        [JsonProperty("TOTAL_INAPP_CLICK_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPCLICKCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_CLICK_SEARCH")]
        public string TOTALINAPPCLICKSEARCH { get; set; }
        [JsonProperty("TOTAL_INAPP_CLICK_SEARCH_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPCLICKSEARCHVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_CLICK_SIGNUP")]
        public string TOTALINAPPCLICKSIGNUP { get; set; }
        [JsonProperty("TOTAL_INAPP_CLICK_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPCLICKSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_CLICK_UNKNOWN")]
        public string TOTALINAPPCLICKUNKNOWN { get; set; }
        [JsonProperty("TOTAL_INAPP_CLICK_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPCLICKUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_ENGAGEMENT_ADD_TO_CART")]
        public string TOTALINAPPENGAGEMENTADDTOCART { get; set; }
        [JsonProperty("TOTAL_INAPP_ENGAGEMENT_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPENGAGEMENTADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_ENGAGEMENT_APP_INSTALL")]
        public string TOTALINAPPENGAGEMENTAPPINSTALL { get; set; }
        [JsonProperty("TOTAL_INAPP_ENGAGEMENT_APP_INSTALL_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPENGAGEMENTAPPINSTALLVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_ENGAGEMENT_CHECKOUT")]
        public string TOTALINAPPENGAGEMENTCHECKOUT { get; set; }
        [JsonProperty("TOTAL_INAPP_ENGAGEMENT_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPENGAGEMENTCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_ENGAGEMENT_SEARCH")]
        public string TOTALINAPPENGAGEMENTSEARCH { get; set; }
        [JsonProperty("TOTAL_INAPP_ENGAGEMENT_SEARCH_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPENGAGEMENTSEARCHVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_ENGAGEMENT_SIGNUP")]
        public string TOTALINAPPENGAGEMENTSIGNUP { get; set; }
        [JsonProperty("TOTAL_INAPP_ENGAGEMENT_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPENGAGEMENTSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_ENGAGEMENT_UNKNOWN")]
        public string TOTALINAPPENGAGEMENTUNKNOWN { get; set; }
        [JsonProperty("TOTAL_INAPP_ENGAGEMENT_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPENGAGEMENTUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_SEARCH")]
        public string TOTALINAPPSEARCH { get; set; }
        [JsonProperty("TOTAL_INAPP_SEARCH_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPSEARCHVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_SIGNUP")]
        public string TOTALINAPPSIGNUP { get; set; }
        [JsonProperty("TOTAL_INAPP_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_UNKNOWN")]
        public string TOTALINAPPUNKNOWN { get; set; }
        [JsonProperty("TOTAL_INAPP_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_VIEW_ADD_TO_CART")]
        public string TOTALINAPPVIEWADDTOCART { get; set; }
        [JsonProperty("TOTAL_INAPP_VIEW_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPVIEWADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_VIEW_APP_INSTALL")]
        public string TOTALINAPPVIEWAPPINSTALL { get; set; }
        [JsonProperty("TOTAL_INAPP_VIEW_APP_INSTALL_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPVIEWAPPINSTALLVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_VIEW_CHECKOUT")]
        public string TOTALINAPPVIEWCHECKOUT { get; set; }
        [JsonProperty("TOTAL_INAPP_VIEW_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPVIEWCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_VIEW_SEARCH")]
        public string TOTALINAPPVIEWSEARCH { get; set; }
        [JsonProperty("TOTAL_INAPP_VIEW_SEARCH_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPVIEWSEARCHVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_VIEW_SIGNUP")]
        public string TOTALINAPPVIEWSIGNUP { get; set; }
        [JsonProperty("TOTAL_INAPP_VIEW_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPVIEWSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_INAPP_VIEW_UNKNOWN")]
        public string TOTALINAPPVIEWUNKNOWN { get; set; }
        [JsonProperty("TOTAL_INAPP_VIEW_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALINAPPVIEWUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_LEAD_DESKTOP_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALLEADDESKTOPACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_LEAD_DESKTOP_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALLEADDESKTOPACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_LEAD_DESKTOP_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALLEADDESKTOPACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_LEAD_MOBILE_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALLEADMOBILEACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_LEAD_MOBILE_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALLEADMOBILEACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_LEAD_MOBILE_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALLEADMOBILEACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_LEAD_TABLET_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALLEADTABLETACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_LEAD_TABLET_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALLEADTABLETACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_LEAD_TABLET_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALLEADTABLETACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CHECKOUT")]
        public string TOTALOFFLINECHECKOUT { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINECHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CLICK_CHECKOUT")]
        public string TOTALOFFLINECLICKCHECKOUT { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CLICK_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINECLICKCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CLICK_CUSTOM")]
        public string TOTALOFFLINECLICKCUSTOM { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CLICK_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINECLICKCUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CLICK_LEAD")]
        public string TOTALOFFLINECLICKLEAD { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CLICK_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINECLICKLEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CLICK_SIGNUP")]
        public string TOTALOFFLINECLICKSIGNUP { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CLICK_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINECLICKSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CLICK_UNKNOWN")]
        public string TOTALOFFLINECLICKUNKNOWN { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CLICK_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINECLICKUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CUSTOM")]
        public string TOTALOFFLINECUSTOM { get; set; }
        [JsonProperty("TOTAL_OFFLINE_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINECUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_ENGAGEMENT_CHECKOUT")]
        public string TOTALOFFLINEENGAGEMENTCHECKOUT { get; set; }
        [JsonProperty("TOTAL_OFFLINE_ENGAGEMENT_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINEENGAGEMENTCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_ENGAGEMENT_CUSTOM")]
        public string TOTALOFFLINEENGAGEMENTCUSTOM { get; set; }
        [JsonProperty("TOTAL_OFFLINE_ENGAGEMENT_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINEENGAGEMENTCUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_ENGAGEMENT_LEAD")]
        public string TOTALOFFLINEENGAGEMENTLEAD { get; set; }
        [JsonProperty("TOTAL_OFFLINE_ENGAGEMENT_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINEENGAGEMENTLEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_ENGAGEMENT_SIGNUP")]
        public string TOTALOFFLINEENGAGEMENTSIGNUP { get; set; }
        [JsonProperty("TOTAL_OFFLINE_ENGAGEMENT_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINEENGAGEMENTSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_ENGAGEMENT_UNKNOWN")]
        public string TOTALOFFLINEENGAGEMENTUNKNOWN { get; set; }
        [JsonProperty("TOTAL_OFFLINE_ENGAGEMENT_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINEENGAGEMENTUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_LEAD")]
        public string TOTALOFFLINELEAD { get; set; }
        [JsonProperty("TOTAL_OFFLINE_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINELEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_SIGNUP")]
        public string TOTALOFFLINESIGNUP { get; set; }
        [JsonProperty("TOTAL_OFFLINE_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINESIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_UNKNOWN")]
        public string TOTALOFFLINEUNKNOWN { get; set; }
        [JsonProperty("TOTAL_OFFLINE_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINEUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_VIEW_CHECKOUT")]
        public string TOTALOFFLINEVIEWCHECKOUT { get; set; }
        [JsonProperty("TOTAL_OFFLINE_VIEW_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINEVIEWCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_VIEW_CUSTOM")]
        public string TOTALOFFLINEVIEWCUSTOM { get; set; }
        [JsonProperty("TOTAL_OFFLINE_VIEW_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINEVIEWCUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_VIEW_LEAD")]
        public string TOTALOFFLINEVIEWLEAD { get; set; }
        [JsonProperty("TOTAL_OFFLINE_VIEW_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINEVIEWLEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_VIEW_SIGNUP")]
        public string TOTALOFFLINEVIEWSIGNUP { get; set; }
        [JsonProperty("TOTAL_OFFLINE_VIEW_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINEVIEWSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_OFFLINE_VIEW_UNKNOWN")]
        public string TOTALOFFLINEVIEWUNKNOWN { get; set; }
        [JsonProperty("TOTAL_OFFLINE_VIEW_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALOFFLINEVIEWUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_PAGE_VISIT_DESKTOP_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALPAGEVISITDESKTOPACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_PAGE_VISIT_DESKTOP_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALPAGEVISITDESKTOPACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_PAGE_VISIT_DESKTOP_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALPAGEVISITDESKTOPACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_PAGE_VISIT_MOBILE_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALPAGEVISITMOBILEACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_PAGE_VISIT_MOBILE_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALPAGEVISITMOBILEACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_PAGE_VISIT_MOBILE_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALPAGEVISITMOBILEACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_PAGE_VISIT_TABLET_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALPAGEVISITTABLETACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_PAGE_VISIT_TABLET_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALPAGEVISITTABLETACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_PAGE_VISIT_TABLET_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALPAGEVISITTABLETACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_SEARCH_DESKTOP_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALSEARCHDESKTOPACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_SEARCH_DESKTOP_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALSEARCHDESKTOPACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_SEARCH_DESKTOP_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALSEARCHDESKTOPACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_SEARCH_MOBILE_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALSEARCHMOBILEACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_SEARCH_MOBILE_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALSEARCHMOBILEACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_SEARCH_MOBILE_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALSEARCHMOBILEACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_SEARCH_TABLET_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALSEARCHTABLETACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_SEARCH_TABLET_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALSEARCHTABLETACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_SEARCH_TABLET_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALSEARCHTABLETACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_SIGNUP_DESKTOP_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALSIGNUPDESKTOPACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_SIGNUP_DESKTOP_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALSIGNUPDESKTOPACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_SIGNUP_DESKTOP_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALSIGNUPDESKTOPACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_SIGNUP_MOBILE_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALSIGNUPMOBILEACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_SIGNUP_MOBILE_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALSIGNUPMOBILEACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_SIGNUP_MOBILE_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALSIGNUPMOBILEACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_SIGNUP_TABLET_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALSIGNUPTABLETACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_SIGNUP_TABLET_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALSIGNUPTABLETACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_SIGNUP_TABLET_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALSIGNUPTABLETACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_UNKNOWN_DESKTOP_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALUNKNOWNDESKTOPACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_UNKNOWN_DESKTOP_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALUNKNOWNDESKTOPACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_UNKNOWN_DESKTOP_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALUNKNOWNDESKTOPACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_UNKNOWN_MOBILE_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALUNKNOWNMOBILEACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_UNKNOWN_MOBILE_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALUNKNOWNMOBILEACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_UNKNOWN_MOBILE_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALUNKNOWNMOBILEACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_UNKNOWN_TABLET_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALUNKNOWNTABLETACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_UNKNOWN_TABLET_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALUNKNOWNTABLETACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_UNKNOWN_TABLET_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALUNKNOWNTABLETACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_VIEW_ADD_TO_CART")]
        public string TOTALVIEWADDTOCART { get; set; }
        [JsonProperty("TOTAL_VIEW_ADD_TO_CART_QUANTITY")]
        public string TOTALVIEWADDTOCARTQUANTITY { get; set; }
        [JsonProperty("TOTAL_VIEW_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_APP_INSTALL")]
        public string TOTALVIEWAPPINSTALL { get; set; }
        [JsonProperty("TOTAL_VIEW_APP_INSTALL_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWAPPINSTALLVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_CATEGORY_DESKTOP_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALVIEWCATEGORYDESKTOPACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_VIEW_CATEGORY_DESKTOP_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALVIEWCATEGORYDESKTOPACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_VIEW_CATEGORY_DESKTOP_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALVIEWCATEGORYDESKTOPACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_VIEW_CATEGORY_MOBILE_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALVIEWCATEGORYMOBILEACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_VIEW_CATEGORY_MOBILE_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALVIEWCATEGORYMOBILEACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_VIEW_CATEGORY_MOBILE_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALVIEWCATEGORYMOBILEACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_VIEW_CATEGORY_TABLET_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALVIEWCATEGORYTABLETACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_VIEW_CATEGORY_TABLET_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALVIEWCATEGORYTABLETACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_VIEW_CATEGORY_TABLET_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALVIEWCATEGORYTABLETACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_VIEW_CHECKOUT")]
        public string TOTALVIEWCHECKOUT { get; set; }
        [JsonProperty("TOTAL_VIEW_CHECKOUT_QUANTITY")]
        public string TOTALVIEWCHECKOUTQUANTITY { get; set; }
        [JsonProperty("TOTAL_VIEW_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_CUSTOM")]
        public string TOTALVIEWCUSTOM { get; set; }
        [JsonProperty("TOTAL_VIEW_CUSTOM_QUANTITY")]
        public string TOTALVIEWCUSTOMQUANTITY { get; set; }
        [JsonProperty("TOTAL_VIEW_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWCUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_LEAD")]
        public string TOTALVIEWLEAD { get; set; }
        [JsonProperty("TOTAL_VIEW_LEAD_QUANTITY")]
        public string TOTALVIEWLEADQUANTITY { get; set; }
        [JsonProperty("TOTAL_VIEW_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWLEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_PAGE_VISIT")]
        public string TOTALVIEWPAGEVISIT { get; set; }
        [JsonProperty("TOTAL_VIEW_PAGE_VISIT_QUANTITY")]
        public string TOTALVIEWPAGEVISITQUANTITY { get; set; }
        [JsonProperty("TOTAL_VIEW_PAGE_VISIT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWPAGEVISITVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_SEARCH")]
        public string TOTALVIEWSEARCH { get; set; }
        [JsonProperty("TOTAL_VIEW_SEARCH_QUANTITY")]
        public string TOTALVIEWSEARCHQUANTITY { get; set; }
        [JsonProperty("TOTAL_VIEW_SEARCH_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWSEARCHVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_SIGNUP")]
        public string TOTALVIEWSIGNUP { get; set; }
        [JsonProperty("TOTAL_VIEW_SIGNUP_QUANTITY")]
        public string TOTALVIEWSIGNUPQUANTITY { get; set; }
        [JsonProperty("TOTAL_VIEW_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_UNKNOWN")]
        public string TOTALVIEWUNKNOWN { get; set; }
        [JsonProperty("TOTAL_VIEW_UNKNOWN_QUANTITY")]
        public string TOTALVIEWUNKNOWNQUANTITY { get; set; }
        [JsonProperty("TOTAL_VIEW_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_VIEW_CATEGORY")]
        public string TOTALVIEWVIEWCATEGORY { get; set; }
        [JsonProperty("TOTAL_VIEW_VIEW_CATEGORY_QUANTITY")]
        public string TOTALVIEWVIEWCATEGORYQUANTITY { get; set; }
        [JsonProperty("TOTAL_VIEW_VIEW_CATEGORY_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWVIEWCATEGORYVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_VIEW_WATCH_VIDEO")]
        public string TOTALVIEWWATCHVIDEO { get; set; }
        [JsonProperty("TOTAL_VIEW_WATCH_VIDEO_QUANTITY")]
        public string TOTALVIEWWATCHVIDEOQUANTITY { get; set; }
        [JsonProperty("TOTAL_VIEW_WATCH_VIDEO_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALVIEWWATCHVIDEOVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WATCH_VIDEO_DESKTOP_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALWATCHVIDEODESKTOPACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_WATCH_VIDEO_DESKTOP_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALWATCHVIDEODESKTOPACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_WATCH_VIDEO_DESKTOP_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALWATCHVIDEODESKTOPACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_WATCH_VIDEO_MOBILE_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALWATCHVIDEOMOBILEACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_WATCH_VIDEO_MOBILE_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALWATCHVIDEOMOBILEACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_WATCH_VIDEO_MOBILE_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALWATCHVIDEOMOBILEACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_WATCH_VIDEO_TABLET_ACTION_TO_DESKTOP_CONVERSION")]
        public string TOTALWATCHVIDEOTABLETACTIONTODESKTOPCONVERSION { get; set; }
        [JsonProperty("TOTAL_WATCH_VIDEO_TABLET_ACTION_TO_MOBILE_CONVERSION")]
        public string TOTALWATCHVIDEOTABLETACTIONTOMOBILECONVERSION { get; set; }
        [JsonProperty("TOTAL_WATCH_VIDEO_TABLET_ACTION_TO_TABLET_CONVERSION")]
        public string TOTALWATCHVIDEOTABLETACTIONTOTABLETCONVERSION { get; set; }
        [JsonProperty("TOTAL_WEB_ADD_TO_CART")]
        public string TOTALWEBADDTOCART { get; set; }
        [JsonProperty("TOTAL_WEB_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_CHECKOUT")]
        public string TOTALWEBCHECKOUT { get; set; }
        [JsonProperty("TOTAL_WEB_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_ADD_TO_CART")]
        public string TOTALWEBCLICKADDTOCART { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBCLICKADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_CHECKOUT")]
        public string TOTALWEBCLICKCHECKOUT { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBCLICKCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_CUSTOM")]
        public string TOTALWEBCLICKCUSTOM { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBCLICKCUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_LEAD")]
        public string TOTALWEBCLICKLEAD { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBCLICKLEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_PAGE_VISIT")]
        public string TOTALWEBCLICKPAGEVISIT { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_PAGE_VISIT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBCLICKPAGEVISITVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_SEARCH")]
        public string TOTALWEBCLICKSEARCH { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_SEARCH_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBCLICKSEARCHVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_SIGNUP")]
        public string TOTALWEBCLICKSIGNUP { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBCLICKSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_UNKNOWN")]
        public string TOTALWEBCLICKUNKNOWN { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBCLICKUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_VIEW_CATEGORY")]
        public string TOTALWEBCLICKVIEWCATEGORY { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_VIEW_CATEGORY_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBCLICKVIEWCATEGORYVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_WATCH_VIDEO")]
        public string TOTALWEBCLICKWATCHVIDEO { get; set; }
        [JsonProperty("TOTAL_WEB_CLICK_WATCH_VIDEO_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBCLICKWATCHVIDEOVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_CUSTOM")]
        public string TOTALWEBCUSTOM { get; set; }
        [JsonProperty("TOTAL_WEB_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBCUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_ADD_TO_CART")]
        public string TOTALWEBENGAGEMENTADDTOCART { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBENGAGEMENTADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_CHECKOUT")]
        public string TOTALWEBENGAGEMENTCHECKOUT { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBENGAGEMENTCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_CUSTOM")]
        public string TOTALWEBENGAGEMENTCUSTOM { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBENGAGEMENTCUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_LEAD")]
        public string TOTALWEBENGAGEMENTLEAD { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBENGAGEMENTLEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_PAGE_VISIT")]
        public string TOTALWEBENGAGEMENTPAGEVISIT { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_PAGE_VISIT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBENGAGEMENTPAGEVISITVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_SEARCH")]
        public string TOTALWEBENGAGEMENTSEARCH { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_SEARCH_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBENGAGEMENTSEARCHVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_SIGNUP")]
        public string TOTALWEBENGAGEMENTSIGNUP { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBENGAGEMENTSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_UNKNOWN")]
        public string TOTALWEBENGAGEMENTUNKNOWN { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBENGAGEMENTUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_VIEW_CATEGORY")]
        public string TOTALWEBENGAGEMENTVIEWCATEGORY { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_VIEW_CATEGORY_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBENGAGEMENTVIEWCATEGORYVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_WATCH_VIDEO")]
        public string TOTALWEBENGAGEMENTWATCHVIDEO { get; set; }
        [JsonProperty("TOTAL_WEB_ENGAGEMENT_WATCH_VIDEO_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBENGAGEMENTWATCHVIDEOVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_LEAD")]
        public string TOTALWEBLEAD { get; set; }
        [JsonProperty("TOTAL_WEB_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBLEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_PAGE_VISIT")]
        public string TOTALWEBPAGEVISIT { get; set; }
        [JsonProperty("TOTAL_WEB_PAGE_VISIT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBPAGEVISITVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_SEARCH")]
        public string TOTALWEBSEARCH { get; set; }
        [JsonProperty("TOTAL_WEB_SEARCH_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBSEARCHVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_SIGNUP")]
        public string TOTALWEBSIGNUP { get; set; }
        [JsonProperty("TOTAL_WEB_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_UNKNOWN")]
        public string TOTALWEBUNKNOWN { get; set; }
        [JsonProperty("TOTAL_WEB_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_ADD_TO_CART")]
        public string TOTALWEBVIEWADDTOCART { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBVIEWADDTOCARTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_CATEGORY")]
        public string TOTALWEBVIEWCATEGORY { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_CATEGORY_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBVIEWCATEGORYVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_CHECKOUT")]
        public string TOTALWEBVIEWCHECKOUT { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_CHECKOUT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBVIEWCHECKOUTVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_CUSTOM")]
        public string TOTALWEBVIEWCUSTOM { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_CUSTOM_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBVIEWCUSTOMVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_LEAD")]
        public string TOTALWEBVIEWLEAD { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_LEAD_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBVIEWLEADVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_PAGE_VISIT")]
        public string TOTALWEBVIEWPAGEVISIT { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_PAGE_VISIT_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBVIEWPAGEVISITVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_SEARCH")]
        public string TOTALWEBVIEWSEARCH { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_SEARCH_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBVIEWSEARCHVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_SIGNUP")]
        public string TOTALWEBVIEWSIGNUP { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_SIGNUP_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBVIEWSIGNUPVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_UNKNOWN")]
        public string TOTALWEBVIEWUNKNOWN { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_UNKNOWN_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBVIEWUNKNOWNVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_VIEW_CATEGORY")]
        public string TOTALWEBVIEWVIEWCATEGORY { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_VIEW_CATEGORY_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBVIEWVIEWCATEGORYVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_WATCH_VIDEO")]
        public string TOTALWEBVIEWWATCHVIDEO { get; set; }
        [JsonProperty("TOTAL_WEB_VIEW_WATCH_VIDEO_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBVIEWWATCHVIDEOVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("TOTAL_WEB_WATCH_VIDEO")]
        public string TOTALWEBWATCHVIDEO { get; set; }
        [JsonProperty("TOTAL_WEB_WATCH_VIDEO_VALUE_IN_MICRO_DOLLAR")]
        public string TOTALWEBWATCHVIDEOVALUEINMICRODOLLAR { get; set; }
        [JsonProperty("VIDEO_3SEC_VIEWS_1")]
        public string VIDEO3SECVIEWS1 { get; set; }
        [JsonProperty("VIDEO_3SEC_VIEWS_2")]
        public string VIDEO3SECVIEWS2 { get; set; }
        [JsonProperty("VIDEO_AVG_WATCHTIME_IN_SECOND_1")]
        public string VIDEOAVGWATCHTIMEINSECOND1 { get; set; }
        [JsonProperty("VIDEO_AVG_WATCHTIME_IN_SECOND_2")]
        public string VIDEOAVGWATCHTIMEINSECOND2 { get; set; }
        [JsonProperty("VIDEO_MRC_VIEWS_1")]
        public string VIDEOMRCVIEWS1 { get; set; }
        [JsonProperty("VIDEO_MRC_VIEWS_2")]
        public string VIDEOMRCVIEWS2 { get; set; }
        [JsonProperty("VIDEO_P0_COMBINED_1")]
        public string VIDEOP0COMBINED1 { get; set; }
        [JsonProperty("VIDEO_P0_COMBINED_2")]
        public string VIDEOP0COMBINED2 { get; set; }
        [JsonProperty("VIDEO_P100_COMPLETE_1")]
        public string VIDEOP100COMPLETE1 { get; set; }
        [JsonProperty("VIDEO_P100_COMPLETE_2")]
        public string VIDEOP100COMPLETE2 { get; set; }
        [JsonProperty("VIDEO_P25_COMBINED_1")]
        public string VIDEOP25COMBINED1 { get; set; }
        [JsonProperty("VIDEO_P25_COMBINED_2")]
        public string VIDEOP25COMBINED2 { get; set; }
        [JsonProperty("VIDEO_P50_COMBINED_1")]
        public string VIDEOP50COMBINED1 { get; set; }
        [JsonProperty("VIDEO_P50_COMBINED_2")]
        public string VIDEOP50COMBINED2 { get; set; }
        [JsonProperty("VIDEO_P75_COMBINED_1")]
        public string VIDEOP75COMBINED1 { get; set; }
        [JsonProperty("VIDEO_P75_COMBINED_2")]
        public string VIDEOP75COMBINED2 { get; set; }
        [JsonProperty("VIDEO_P95_COMBINED_1")]
        public string VIDEOP95COMBINED1 { get; set; }
        [JsonProperty("VIDEO_P95_COMBINED_2")]
        public string VIDEOP95COMBINED2 { get; set; }
        [JsonProperty("WEB_ADD_TO_CART_COST_PER_ACTION")]
        public string WEBADDTOCARTCOSTPERACTION { get; set; }
        [JsonProperty("WEB_ADD_TO_CART_ROAS")]
        public string WEBADDTOCARTROAS { get; set; }
        [JsonProperty("WEB_CHECKOUT_COST_PER_ACTION")]
        public string WEBCHECKOUTCOSTPERACTION { get; set; }
        [JsonProperty("WEB_CHECKOUT_ROAS")]
        public string WEBCHECKOUTROAS { get; set; }
        [JsonProperty("WEB_CUSTOM_COST_PER_ACTION")]
        public string WEBCUSTOMCOSTPERACTION { get; set; }
        [JsonProperty("WEB_CUSTOM_ROAS")]
        public string WEBCUSTOMROAS { get; set; }
        [JsonProperty("WEB_LEAD_COST_PER_ACTION")]
        public string WEBLEADCOSTPERACTION { get; set; }
        [JsonProperty("WEB_LEAD_ROAS")]
        public string WEBLEADROAS { get; set; }
        [JsonProperty("WEB_PAGE_VISIT_COST_PER_ACTION")]
        public string WEBPAGEVISITCOSTPERACTION { get; set; }
        [JsonProperty("WEB_PAGE_VISIT_ROAS")]
        public string WEBPAGEVISITROAS { get; set; }
        [JsonProperty("WEB_SEARCH_COST_PER_ACTION")]
        public string WEBSEARCHCOSTPERACTION { get; set; }
        [JsonProperty("WEB_SEARCH_ROAS")]
        public string WEBSEARCHROAS { get; set; }
        [JsonProperty("WEB_SIGNUP_COST_PER_ACTION")]
        public string WEBSIGNUPCOSTPERACTION { get; set; }
        [JsonProperty("WEB_SIGNUP_ROAS")]
        public string WEBSIGNUPROAS { get; set; }
        [JsonProperty("WEB_UNKNOWN_COST_PER_ACTION")]
        public string WEBUNKNOWNCOSTPERACTION { get; set; }
        [JsonProperty("WEB_UNKNOWN_ROAS")]
        public string WEBUNKNOWNROAS { get; set; }
        [JsonProperty("WEB_VIEW_CATEGORY_COST_PER_ACTION")]
        public string WEBVIEWCATEGORYCOSTPERACTION { get; set; }
        [JsonProperty("WEB_VIEW_CATEGORY_ROAS")]
        public string WEBVIEWCATEGORYROAS { get; set; }
        [JsonProperty("WEB_WATCH_VIDEO_COST_PER_ACTION")]
        public string WEBWATCHVIDEOCOSTPERACTION { get; set; }
        [JsonProperty("WEB_WATCH_VIDEO_ROAS")]
        public string WEBWATCHVIDEOROAS { get; set; }
        [JsonProperty("OUTBOUND_CLICK_1")]
        public string OUTBOUND_CLICK_1 { get; set; }
        [JsonProperty("OUTBOUND_CLICK_2")]
        public string OUTBOUND_CLICK_2 { get; set; }
        [JsonProperty("TOTAL_WEB_SESSIONS")]
        public string TOTALWEBSESSIONS { get; set; }
        [JsonProperty("WEB_SESSIONS_1")]
        public string WEBSESSIONS1 { get; set; }
        [JsonProperty("WEB_SESSIONS_2")]
        public string WEBSESSIONS2 { get; set; }

        #endregion
    }
}