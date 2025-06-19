namespace Greenhouse.Data.DataSource.NetBase.Core
{
    public static class NetBaseEnums
    {
        public enum SentimentsEnum
        {
            Negative,
            Neutral,
            Positive
        }

        public enum GendersEnum
        {
            Female,
            Male,
            Unknown
        }

        public enum SourcesEnum
        {
            Blogs,
            Comments,
            ConsumerReviews,
            Facebook,
            Forums,
            Instagram,
            Microblogs,
            News,
            Other,
            ProfReviews,
            SocialNetworks,
            Tumblr,
            Twitter,
            YouTube
        }

        public enum DateRangeEnum
        {
            LAST_HOUR,
            LAST_DAY,
            LAST_WEEK,
            LAST_MONTH,
            LAST_QUARTER,
            LAST_SIX_MONTHS,
            LAST_YEAR,
            LAST_2YEARS,
            LAST_27MONTHS
        }
    }
}
