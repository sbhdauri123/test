using System;

namespace Greenhouse.Data.DataSource.DBM.API
{
    [Serializable]
    public enum ReportType
    {
        // DIAT-14939 - Report Types deprecated in v2 - https://developers.google.com/bid-manager/how-tos/v2-migration
        // following commented-out value is v1.1 for reference

        STANDARD,//TYPE_GENERAL,
        INVENTORY_AVAILABILITY,//TYPE_INVENTORY_AVAILABILITY,
        AUDIENCE_COMPOSITION,//TYPE_AUDIENCE_COMPOSITION,
        FLOODLIGHT,//TYPE_ORDER_ID,
        YOUTUBE,//TYPE_TRUEVIEW,
        GRP,//TYPE_NIELSEN_SITE,
        REACH,//TYPE_REACH_AND_FREQUENCY,
        YOUTUBE_PROGRAMMATIC_GUARANTEED,//TYPE_PETRA_NIELSEN_AUDIENCE_PROFILE,
        REPORT_TYPE_UNSPECIFIED,//TYPE_NOT_SUPPORTED,
        UNIQUE_REACH_AUDIENCE,//TYPE_REACH_AUDIENCE,
        FULL_PATH,//TYPE_PATH,
        PATH_ATTRIBUTION,//TYPE_PATH_ATTRIBUTION,
        BROWSER,
        DEVICE_CRITERIA,
        GEOLOCATION,
        ISP,
        LANGUAGE,
        SUPPORTED_EXCHANGE,
        ADVERTISERS,
        INSERTION_ORDER,
        INVENTORY_SOURCE,
        LINE_ITEM,
        PARTNER,
        UNIVERSAL_CHANNEL
    }

    public enum ReportFrequency
    {
        DAILY,
        MONTHLY,
        ONE_TIME,
        QUARTERLY,
        SEMI_MONTHLY,
        WEEKLY,
        FREQUENCY_UNSPECIFIED
    }

    public enum DataRange
    {
        CURRENT_DAY,
        CUSTOM_DATES,
        LAST_14_DAYS,
        LAST_30_DAYS,
        LAST_365_DAYS,
        LAST_7_DAYS,
        LAST_90_DAYS,
        MONTH_TO_DATE,
        PREVIOUS_DAY,
        RANGE_UNSPECIFIED
    }

    public enum FilterType
    {
        FILTER_ORDER_ID,
        FILTER_OS,
        FILTER_PAGE_CATEGORY,
        FILTER_PAGE_LAYOUT,
        FILTER_PARTNER,
        FILTER_LINE_ITEM,
        FILTER_LINE_ITEM_TYPE
    }

    public enum ReportFormat
    {
        CSV,
        XLSX,
        FORMAT_UNSPECIFIED
    }

    public enum LineItemType
    {
        RTB = 1,
        RMX_NON_RTB = 2,
        RTB_VIDEO = 5,
        ADWORDS_FOR_VIDEO = 6,
        RTB_AUDIO = 8,
        LINE_ITEM_TYPE_GMAIL = 9
    }
}