using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.DataSource.DCM
{
    public static class DcmService
    {
        public static void LoadDcmAdDimension(List<Ad> adsDimCollection,
            string entityId, DateTime fileDate, string jsonFileName, Action<JArray, string, DateTime, string> writeObjectToFile)
        {
            var ads = adsDimCollection.Select(x => new
            {
                account_id = x.AccountId,
                advertiser_id = x.AdvertiserId,
                campaign_id = x.CampaignId,
                ad_id = x.AdId,
                ad_name = x.AdName,
                ad_type = x.Type,
                compatibility = x.Compatibility,
                start_time = x.StartTime,
                end_time = x.EndTime
            });

            JArray adsObjecToSerialize = JArray.FromObject(ads);
            writeObjectToFile(adsObjecToSerialize, entityId, fileDate, jsonFileName);

            var adSize = adsDimCollection.Select(x => new { x.AdId, x.Size }).Where(xx => xx.Size != null).Select(xxx => new
            {
                ad_id = xxx.AdId,
                size_id = xxx.Size.SizeId,
                height = xxx.Size.Height,
                width = xxx.Size.Width
            });

            JArray adSizesObjecToSerialize = JArray.FromObject(adSize);
            writeObjectToFile(adSizesObjecToSerialize, entityId, fileDate, "adsize.json");
        }

        public static void LoadDcmAdvertiserDimension(List<Advertiser> advertisersDimCollection,
            string entityId, DateTime fileDate, string jsonFileName, Action<JArray, string, DateTime, string> writeObjectToFile)
        {
            var advertisers = advertisersDimCollection.Select(x => new
            {
                advertiser_id = x.AdvertiserId,
                advertiser_name = x.AdvertiserName,
                account_id = x.AccountId,
                floodlight_configuration_id = x.FloodlightConfigurationId,
                advertiser_group_id = x.AdvertiserGroupId
            });

            JArray advertisersObjectToSerialize = JArray.FromObject(advertisers);
            writeObjectToFile(advertisersObjectToSerialize, entityId, fileDate, jsonFileName);
        }

        public static void LoadDcmCampaignDimension(List<Campaign> campaignsDimCollection,
            string entityId, DateTime fileDate, string jsonFileName, Action<JArray, string, DateTime, string> writeObjectToFile)
        {
            var campaigns = campaignsDimCollection.Select(x => new
            {
                campaign_id = x.CampaignId,
                campaign_name = x.CampaignName,
                account_id = x.AccountId,
                advertiser_id = x.AdvertiserId,
                start_time = x.StartDate,
                end_time = x.EndDate,
                advertiser_group_id = x.AdvertiserGroupId
            });

            JArray campaignsObjectToSerialize = JArray.FromObject(campaigns);
            writeObjectToFile(campaignsObjectToSerialize, entityId, fileDate, jsonFileName);
        }

        public static void LoadDcmCreativeDimension(List<Creative> creativesDimCollection,
            string entityId, DateTime fileDate, string jsonFileName, Action<JArray, string, DateTime, string> writeObjectToFile)
        {
            var creatives = creativesDimCollection.Select(x => new
            {
                creative_id = x.CreativeId,
                creative_name = x.CreativeName,
                account_id = x.AccountId,
                advertiser_id = x.AdvertiserId,
                rendering_id = x.RenderingId,
                creative_type = x.Type
            });

            JArray creativesObjectToSerialize = JArray.FromObject(creatives);
            writeObjectToFile(creativesObjectToSerialize, entityId, fileDate, jsonFileName);

            var creativeSize = creativesDimCollection.Select(x => new { x.CreativeId, x.Size }).Where(xx => xx.Size != null).Select(xxx => new
            {
                creative_id = xxx.CreativeId,
                size_id = xxx.Size.SizeId,
                height = xxx.Size.Height,
                width = xxx.Size.Width
            });

            JArray creativeSizesObjecToSerialize = JArray.FromObject(creativeSize);
            writeObjectToFile(creativeSizesObjecToSerialize, entityId, fileDate, "creativesize.json");
        }

        public static void LoadDcmPlacementGroupDimension(List<PlacementGroup> placementGroupsDimCollection,
            string entityId, DateTime fileDate, string jsonFileName, Action<JArray, string, DateTime, string> writeObjectToFile)
        {
            var placementGroups = placementGroupsDimCollection.Select(x => new
            {
                placement_group_id = x.PlacementGroupId,
                placement_group_name = x.PlacementGroupName,
                account_id = x.AccountId,
                advertiser_id = x.AdvertiserId,
                campaign_id = x.CampaignId,
                site_id = x.SiteId,
                placement_strategy_id = x.PlacementStrategyId,
                placement_group_type = x.PlacementGroupType
            });

            JArray placementsObjecToSerialize = JArray.FromObject(placementGroups);
            writeObjectToFile(placementsObjecToSerialize, entityId, fileDate, jsonFileName);

            var placementGroupPricingSchedule = placementGroupsDimCollection.Select(x => new { x.PlacementGroupId, x.PricingSchedule }).Where(xx => xx.PricingSchedule != null).Select(xxx => new
            {
                placement_group_id = xxx.PlacementGroupId,
                start_date = xxx.PricingSchedule.StartDate,
                end_date = xxx.PricingSchedule.EndDate
            });

            JArray placementGroupPricingSchedulesObjecToSerialize = JArray.FromObject(placementGroupPricingSchedule);
            writeObjectToFile(placementGroupPricingSchedulesObjecToSerialize, entityId, fileDate, "placementgrouppricingschedule.json");
        }

        public static void LoadDcmPlacementDimension(List<Placement> placementsDimCollection,
            string entityId, DateTime fileDate, string jsonFileName, Action<JArray, string, DateTime, string> writeObjectToFile)
        {
            var placements = placementsDimCollection.Select(x => new
            {
                placement_id = x.PlacementId,
                placement_name = x.PlacementName,
                account_id = x.AccountId,
                advertiser_id = x.AdvertiserId,
                campaign_id = x.CampaignId,
                site_id = x.SiteId,
                placement_group_id = x.PlacementGroupId,
                placement_strategy_id = x.PlacementStrategyId
            });

            JArray placementsObjecToSerialize = JArray.FromObject(placements);
            writeObjectToFile(placementsObjecToSerialize, entityId, fileDate, jsonFileName);

            var placementSize = placementsDimCollection.Select(x => new { x.PlacementId, x.Size }).Where(xx => xx.Size != null).Select(xxx => new
            {
                placement_id = xxx.PlacementId,
                size_id = xxx.Size.SizeId,
                height = xxx.Size.Height,
                width = xxx.Size.Width
            });

            JArray adSizesObjecToSerialize = JArray.FromObject(placementSize);
            writeObjectToFile(adSizesObjecToSerialize, entityId, fileDate, "placementsize.json");

            var placementPricingSchedule = placementsDimCollection.Select(x => new { x.PlacementId, x.PricingSchedule }).Where(xx => xx.PricingSchedule != null).Select(xxx => new
            {
                placement_id = xxx.PlacementId,
                start_date = xxx.PricingSchedule.StartDate,
                end_date = xxx.PricingSchedule.EndDate
            });

            JArray placementPricingSchedulesObjecToSerialize = JArray.FromObject(placementPricingSchedule);
            writeObjectToFile(placementPricingSchedulesObjecToSerialize, entityId, fileDate, "placementpricingschedule.json");
        }
    }
}