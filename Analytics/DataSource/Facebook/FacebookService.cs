using Greenhouse.Data.DataSource.Facebook.Action;
using Greenhouse.Data.DataSource.Facebook.Dimension.AdCreative;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Services;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.DataSource.Facebook;

public static class FacebookService
{
    public static void LoadFacebookAdDimension(List<DataAdDimension> adDimensionList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        var redshiftMaxLength = int.Parse(SetupService.GetById<Model.Setup.Lookup>(Common.Constants.LOOKUP_REDSHIFT_MAX_STRING_LENGTH).Value);

        #region [Ad]
        var ads = adDimensionList
            .Select(x => new
            {
                account_id = x.AccountId,
                effective_status = x.EffectiveStatus,
                bid_type = x.BidType,
                campaign_id = x.CampaignId,
                adset_id = x.AdSetId,
                created_time = x.CreatedTime,
                id = x.AdId,
                last_updated_by_app_id = x.LastUpdatedByAppId,
                name = x.Name,
                updated_time = x.UpdatedTime,
                status = x.Status
            });

        StageData(ads, queue, WriteObjectToFile, stageFileCollection, "Ad");
        #endregion

        #region [Bid Info]
        var bidInfo = adDimensionList
            .Where(y => y.BidInfo != null)
            .Select(x => new
            {
                ad_id = x.AdId,
                adset_id = x.AdSetId,
                campaign_id = x.CampaignId,
                account_id = x.AccountId,
                actions = x.BidInfo.Actions,
                clicks = x.BidInfo.Clicks,
                impressions = x.BidInfo.Impressions,
                reach = x.BidInfo.Reach,
                social = x.BidInfo.Social
            });

        StageData(bidInfo, queue, WriteObjectToFile, stageFileCollection, "BidInfo");
        #endregion

        #region [Tracking Specs]
        var trackingSpecs = adDimensionList
            .Where(y => y.TrackingSpecs != null)
            .Select(x => x.TrackingSpecs
                .Select(xx => new
                {
                    ad_id = x.AdId,
                    adset_id = x.AdSetId,
                    campaign_id = x.CampaignId,
                    account_id = x.AccountId,
                    action_type = TruncateField(xx.ActionType == null ? string.Empty : string.Join(",", xx.ActionType), redshiftMaxLength),
                    offsite_pixel = TruncateField(xx.OffsitePixel == null ? string.Empty : string.Join(",", xx.OffsitePixel), redshiftMaxLength),
                    page = TruncateField(xx.Page == null ? string.Empty : string.Join(",", xx.Page), redshiftMaxLength),
                    application = TruncateField(xx.Application == null ? string.Empty : string.Join(",", xx.Application), redshiftMaxLength),
                    ts_event = TruncateField(xx.Event == null ? string.Empty : string.Join(",", xx.Event), redshiftMaxLength),
                    ts_object = TruncateField(xx.Object == null ? string.Empty : string.Join(",", xx.Object), redshiftMaxLength),
                    offer = TruncateField(xx.Offer == null ? string.Empty : string.Join(",", xx.Offer), redshiftMaxLength),
                    offer_creator = TruncateField(xx.OfferCreator == null ? string.Empty : string.Join(",", xx.OfferCreator), redshiftMaxLength),
                    post = TruncateField(xx.Post == null ? string.Empty : string.Join(",", xx.Post), redshiftMaxLength),
                    post_wall = TruncateField(xx.PostWall == null ? string.Empty : string.Join(",", xx.PostWall), redshiftMaxLength),
                    response = TruncateField(xx.Response == null ? string.Empty : string.Join(",", xx.Response), redshiftMaxLength),
                    creative = TruncateField(xx.Creative == null ? string.Empty : string.Join(",", xx.Creative), redshiftMaxLength),
                    fb_pixel = TruncateField(xx.FbPixel == null ? string.Empty : string.Join(",", xx.FbPixel), redshiftMaxLength)
                })).SelectMany(xxx => xxx);

        StageData(trackingSpecs, queue, WriteObjectToFile, stageFileCollection, "TrackingSpecs");
        #endregion

        #region [Conversion Specs]
        //https://developers.facebook.com/docs/marketing-api/reference/conversion-action-query/
        var conversionSpecs = adDimensionList
            .Where(y => y.ConversionSpecs != null)
            .Select(x => x.ConversionSpecs
                .Select(xx => new
                {
                    ad_id = x.AdId,
                    adset_id = x.AdSetId,
                    campaign_id = x.CampaignId,
                    account_id = x.AccountId,
                    action_type = TruncateField(xx.ActionType == null ? string.Empty : string.Join(",", xx.ActionType), redshiftMaxLength),
                    page = TruncateField(xx.Page == null ? string.Empty : string.Join(",", xx.Page), redshiftMaxLength),
                    application = TruncateField(xx.Application == null ? string.Empty : string.Join(",", xx.Application), redshiftMaxLength),
                    cs_event = TruncateField(xx.Event == null ? string.Empty : string.Join(",", xx.Event), redshiftMaxLength),
                    cs_object = TruncateField(xx.Object == null ? string.Empty : string.Join(",", xx.Object), redshiftMaxLength),
                    offer = TruncateField(xx.Offer == null ? string.Empty : string.Join(",", xx.Offer), redshiftMaxLength),
                    offer_creator = TruncateField(xx.OfferCreator == null ? string.Empty : string.Join(",", xx.OfferCreator), redshiftMaxLength),
                    post = TruncateField(xx.Post == null ? string.Empty : string.Join(",", xx.Post), redshiftMaxLength),
                    post_wall = TruncateField(xx.PostWall == null ? string.Empty : string.Join(",", xx.PostWall), redshiftMaxLength),
                    response = TruncateField(xx.Response == null ? string.Empty : string.Join(",", xx.Response), redshiftMaxLength),
                    conversion_id = TruncateField(xx.ConversionId == null ? string.Empty : string.Join(",", xx.ConversionId), redshiftMaxLength)
                })).SelectMany(xxx => xxx);

        StageData(conversionSpecs, queue, WriteObjectToFile, stageFileCollection, "ConversionSpecs");
        #endregion

        #region [Review Feedback]

        var adReviewFeedback = adDimensionList.Where(t => t.AdReviewFeedback != null)
            .Select(a => new
            {
                ad_id = a.AdId,
                adset_id = a.AdSetId,
                campaign_id = a.CampaignId,
                account_id = a.AccountId,
                global = TruncateField(a.AdReviewFeedback.Global == null ? string.Empty : a.AdReviewFeedback.Global.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                account_admin = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.AccountAdmin == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.AccountAdmin.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                ad = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.Ad == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.Ad.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                b2c = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.B2C == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.B2C.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                bsg = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.BSG == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.BSG.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                city_community = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.CityCommunity == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.CityCommunity.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                daily_deals = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.DailyDeals == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.DailyDeals.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                daily_deals_legacy = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.DailyDealsLegacy == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.DailyDealsLegacy.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                dpa = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.DPA == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.DPA.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                facebook = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.Facebook == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.Facebook.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                instagram = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.Instagram == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.Instagram.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                instagram_shop = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.InstagramShop == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.InstagramShop.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                marketplace = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.Marketplace == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.Marketplace.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                marketplace_home_rentals = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.MarketplaceHomeRentals == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.MarketplaceHomeRentals.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                marketplace_home_sales = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.MarketplaceHomeSales == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.MarketplaceHomeSales.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                marketplace_motors = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.MarketplaceMotors == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.MarketplaceMotors.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                page_admin = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.PageAdmin == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.PageAdmin.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                product = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.Product == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.Product.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                product_service = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.ProductService == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.ProductService.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                profile = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.Profile == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.Profile.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                seller = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.Seller == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.Seller.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                shops = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.Shops == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.Shops.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength),
                whatsapp = TruncateField(a.AdReviewFeedback.PlacementSpecific == null || a.AdReviewFeedback.PlacementSpecific.Whatsapp == null ? string.Empty : a.AdReviewFeedback.PlacementSpecific.Whatsapp.Select(x => x.Key + "=" + x.Value).Aggregate((s1, s2) => s1 + ";" + s2), redshiftMaxLength)
            });

        StageData(adReviewFeedback, queue, WriteObjectToFile, stageFileCollection, "AdReviewFeedback");
        #endregion

        #region targeting
        var targetingData = adDimensionList
            .Where(y => y.Targeting != null)
            .Select(x => new
            {
                x.AdId,
                x.AdSetId,
                CampaignId = x.CampaignId,
                AccountId = x.AccountId,
                x.Targeting
            }).Select(xx => new
            {
                ad_id = xx.AdId,
                adset_id = xx.AdSetId,
                campaign_id = xx.CampaignId,
                account_id = xx.AccountId,
                genders = TruncateField(xx.Targeting.Genders == null ? null : string.Join(",", xx.Targeting.Genders), redshiftMaxLength),
                age_max = xx.Targeting.AgeMax,
                age_min = xx.Targeting.AgeMin,
                countries = TruncateField(xx.Targeting.Countries == null ? null : string.Join(",", xx.Targeting.Countries), redshiftMaxLength),
                page_types = TruncateField(xx.Targeting.PageTypes == null ? null : string.Join(",", xx.Targeting.PageTypes), redshiftMaxLength),
                radius = xx.Targeting.Radius,
                user_os = TruncateField(xx.Targeting.UserOs == null ? null : string.Join(",", xx.Targeting.UserOs), redshiftMaxLength),
                user_device = TruncateField(xx.Targeting.UserDevice == null ? null : string.Join(",", xx.Targeting.UserDevice), redshiftMaxLength),
                wireless_carrier = TruncateField(xx.Targeting.WirelessCarrier == null ? null : string.Join(",", xx.Targeting.WirelessCarrier), redshiftMaxLength),
                education_statuses = TruncateField(xx.Targeting.EducationStatuses == null ? null : string.Join(",", xx.Targeting.EducationStatuses), redshiftMaxLength),
                college_years = TruncateField(xx.Targeting.CollegeYears == null ? null : string.Join(",", xx.Targeting.CollegeYears), redshiftMaxLength),
                relationship_statuses = TruncateField(xx.Targeting.RelationshipStatuses == null ? null : string.Join(",", xx.Targeting.RelationshipStatuses), redshiftMaxLength),
                locales = TruncateField(xx.Targeting.Locales == null ? null : string.Join(",", xx.Targeting.Locales), redshiftMaxLength),
                dynamic_audience_ids = TruncateField(xx.Targeting.DynamicAudienceIds == null ? null : string.Join(",", xx.Targeting.DynamicAudienceIds), redshiftMaxLength),
                excluded_user_device = TruncateField(xx.Targeting.ExcludedUserDevice == null ? null : string.Join(",", xx.Targeting.ExcludedUserDevice), redshiftMaxLength),
                device_platforms = TruncateField(xx.Targeting.DevicePlatforms == null ? null : string.Join(",", xx.Targeting.DevicePlatforms), redshiftMaxLength),
                publisher_platforms = TruncateField(xx.Targeting.PublisherPlatforms == null ? null : string.Join(",", xx.Targeting.PublisherPlatforms), redshiftMaxLength),
                facebook_positions = TruncateField(xx.Targeting.FacebookPositions == null ? null : string.Join(",", xx.Targeting.FacebookPositions), redshiftMaxLength),
                instagram_positions = TruncateField(xx.Targeting.InstagramPositions == null ? null : string.Join(",", xx.Targeting.InstagramPositions), redshiftMaxLength),
                audience_network_positions = TruncateField(xx.Targeting.AudienceNetworkPositions == null ? null : string.Join(",", xx.Targeting.AudienceNetworkPositions), redshiftMaxLength),
                messenger_positions = TruncateField(xx.Targeting.MessengerPositions == null ? null : string.Join(",", xx.Targeting.MessengerPositions), redshiftMaxLength),
                excluded_publisher_categories = TruncateField(xx.Targeting.ExcludedPublisherCategories == null ? null : string.Join(",", xx.Targeting.ExcludedPublisherCategories), redshiftMaxLength),
                app_install_state = xx.Targeting.AppInstallState,
                targeting_optimization = xx.Targeting.TargetingOptimization
            });

        StageData(targetingData, queue, WriteObjectToFile, stageFileCollection, "AdTargeting");
        #endregion

        #region [GeoLocations]
        var geoLocations = adDimensionList
            .Where(a => a.Targeting != null)
            .Select(x => new
            {
                x.AdId,
                x.AdSetId,
                x.CampaignId,
                x.AccountId,
                x.Targeting
            })
            .Where(q =>
                q.Targeting.GeoLocations != null
             )
            .Select(xx => new
            {
                ad_id = xx.AdId,
                adset_id = xx.AdSetId,
                campaign_id = xx.CampaignId,
                account_id = xx.AccountId,
                countries = TruncateField(xx.Targeting.GeoLocations.Countries == null ? string.Empty : string.Join(",", xx.Targeting.GeoLocations.Countries), redshiftMaxLength),
                location_types = TruncateField(xx.Targeting.GeoLocations.LocationTypes == null ? string.Empty : string.Join(",", xx.Targeting.GeoLocations.LocationTypes), redshiftMaxLength),
                country_groups = TruncateField(xx.Targeting.GeoLocations.CountryGroups == null ? string.Empty : string.Join(",", xx.Targeting.GeoLocations.CountryGroups), redshiftMaxLength)
            });

        StageData(geoLocations, queue, WriteObjectToFile, stageFileCollection, "AdTargetingGeoLocation");
        #endregion

        #region[Cities]
        var cities = adDimensionList
            .Where(a => a.Targeting != null)
            .Select(x => new
            {
                x.AdId,
                x.AdSetId,
                x.CampaignId,
                x.AccountId,
                x.Targeting
            })
            .Where(q => q.Targeting.GeoLocations != null &&
                        q.Targeting.GeoLocations?.Cities != null)
            .Select(xx => xx.Targeting.GeoLocations.Cities
                .Select(xxx => new
                {
                    ad_id = xx.AdId,
                    adset_id = xx.AdSetId,
                    campaign_id = xx.CampaignId,
                    account_id = xx.AccountId,
                    city_key = xxx.CityKey,
                    radius = xxx.Radius,
                    distance_unit = xxx.DistanceUnit,
                    country = xxx.Country,
                    city_name = xxx.CityName,
                    region = xxx.Region,
                    region_id = xxx.RegionId
                })).SelectMany(xxxx => xxxx);

        StageData(cities, queue, WriteObjectToFile, stageFileCollection, "AdTargetingCityGeoLocation");
        #endregion

        #region [Regions]
        var regions = adDimensionList
            .Where(a => a.Targeting != null)
            .Select(x => new
            {
                x.AdId,
                x.AdSetId,
                x.CampaignId,
                x.AccountId,
                x.Targeting
            })
            .Where(q => q.Targeting.GeoLocations != null &&
                        q.Targeting.GeoLocations?.Regions != null)
            .Select(xx => xx.Targeting.GeoLocations.Regions
                .Select(xxx => new
                {
                    ad_id = xx.AdId,
                    adset_id = xx.AdSetId,
                    campaign_id = xx.CampaignId,
                    account_id = xx.AccountId,
                    region_key = xxx.RegionKey,
                    region_name = xxx.RegionName,
                    region_country = xxx.RegionCountry
                })).SelectMany(xxxx => xxxx);

        StageData(regions, queue, WriteObjectToFile, stageFileCollection, "AdTargetingRegionGeoLocation");
        #endregion

        #region [Zips]
        var zips = adDimensionList
            .Where(a => a.Targeting != null)
            .Select(x => new
            {
                x.AdId,
                x.AdSetId,
                x.CampaignId,
                x.AccountId,
                x.Targeting
            })
            .Where(q => q.Targeting.GeoLocations != null &&
                        q.Targeting.GeoLocations?.Zips != null)
            .Select(xx => xx.Targeting.GeoLocations.Zips
                .Select(xxx => new
                {
                    ad_id = xx.AdId,
                    adset_id = xx.AdSetId,
                    campaign_id = xx.CampaignId,
                    account_id = xx.AccountId,
                    zip_key = xxx.ZipKey,
                    zip_name = xxx.ZipName,
                    primary_city_id = xxx.PrimaryCityId,
                    region_id = xxx.RegionId,
                    country = xxx.Country
                })).SelectMany(xxxx => xxxx);

        StageData(zips, queue, WriteObjectToFile, stageFileCollection, "AdTargetingZipGeoLocation");
        #endregion

        List<object> allCategoryTargeting = new List<object>();

        #region [ExcludedConnections]
        var p = adDimensionList.Select(x => new
        {
            x.AdId,
            x.AdSetId,
            x.CampaignId,
            x.AccountId,
            x.Targeting
        })
        .Where(q => q.Targeting?.ExcludedConnections != null)
        .Select(xx => xx.Targeting.ExcludedConnections
            .Select(xxx => new
            {
                ad_id = xx.AdId,
                adset_id = xx.AdSetId,
                campaign_id = xx.CampaignId,
                account_id = xx.AccountId,
                category_targeting_id = xxx.CategoryTargetingId,
                name = xxx.Name,
                parent_category = "ExcludedConnections"
            })).SelectMany(xxxx => xxxx);

        if (p.Any())
        {
            allCategoryTargeting.AddRange(p);
        }
        #endregion

        #region [FriendsOfConnections]
        p = adDimensionList.Select(x => new
        {
            x.AdId,
            x.AdSetId,
            x.CampaignId,
            x.AccountId,
            x.Targeting
        })
        .Where(q => q.Targeting?.FriendsOfConnections != null)
        .Select(xx => xx.Targeting.FriendsOfConnections
            .Select(xxx => new
            {
                ad_id = xx.AdId,
                adset_id = xx.AdSetId,
                campaign_id = xx.CampaignId,
                account_id = xx.AccountId,
                category_targeting_id = xxx.CategoryTargetingId,
                name = xxx.Name,
                parent_category = "FriendsOfConnections"
            })).SelectMany(xxxx => xxxx);

        if (p.Any())
        {
            allCategoryTargeting.AddRange(p);
        }
        #endregion

        #region [Connections]
        p = adDimensionList.Select(x => new
        {
            x.AdId,
            x.AdSetId,
            x.CampaignId,
            x.AccountId,
            x.Targeting
        })
        .Where(q => q.Targeting?.Connections != null)
        .Select(xx => xx.Targeting.Connections
            .Select(xxx => new
            {
                ad_id = xx.AdId,
                adset_id = xx.AdSetId,
                campaign_id = xx.CampaignId,
                account_id = xx.AccountId,
                category_targeting_id = xxx.CategoryTargetingId,
                name = xxx.Name,
                parent_category = "Connections"
            })).SelectMany(xxxx => xxxx);

        if (p.Any())
        {
            allCategoryTargeting.AddRange(p);
        }
        #endregion

        #region [CustomAudiences]
        p = adDimensionList.Select(x => new
        {
            x.AdId,
            x.AdSetId,
            x.CampaignId,
            x.AccountId,
            x.Targeting
        })
        .Where(q => q.Targeting?.CustomAudiences != null)
        .Select(xx => xx.Targeting.CustomAudiences
            .Select(xxx => new
            {
                ad_id = xx.AdId,
                adset_id = xx.AdSetId,
                campaign_id = xx.CampaignId,
                account_id = xx.AccountId,
                category_targeting_id = xxx.CategoryTargetingId,
                name = xxx.Name,
                parent_category = "CustomAudiences"
            })).SelectMany(xxxx => xxxx);

        if (p.Any())
        {
            allCategoryTargeting.AddRange(p);
        }
        #endregion

        #region [UserAdclusters]
        p = adDimensionList.Select(x => new
        {
            x.AdId,
            x.AdSetId,
            x.CampaignId,
            x.AccountId,
            x.Targeting
        })
        .Where(q => q.Targeting?.UserAdClusters != null)
        .Select(xx => xx.Targeting.UserAdClusters
            .Select(xxx => new
            {
                ad_id = xx.AdId,
                adset_id = xx.AdSetId,
                campaign_id = xx.CampaignId,
                account_id = xx.AccountId,
                category_targeting_id = xxx.CategoryTargetingId,
                name = xxx.Name,
                parent_category = "UserAdClusters"
            })).SelectMany(xxxx => xxxx);

        if (p.Any())
        {
            allCategoryTargeting.AddRange(p);
        }
        #endregion

        #region [Interests]
        p = adDimensionList.Select(x => new
        {
            x.AdId,
            x.AdSetId,
            x.CampaignId,
            x.AccountId,
            x.Targeting
        })
        .Where(q => q.Targeting?.Interests != null)
        .Select(xx => xx.Targeting.Interests
            .Select(xxx => new
            {
                ad_id = xx.AdId,
                adset_id = xx.AdSetId,
                campaign_id = xx.CampaignId,
                account_id = xx.AccountId,
                category_targeting_id = xxx.CategoryTargetingId,
                name = xxx.Name,
                parent_category = "Interests"
            })).SelectMany(xxxx => xxxx);

        if (p.Any())
        {
            allCategoryTargeting.AddRange(p);
        }
        #endregion

        #region [EducationSchools]
        p = adDimensionList.Select(x => new
        {
            x.AdId,
            x.AdSetId,
            x.CampaignId,
            x.AccountId,
            x.Targeting
        })
            .Where(q => q.Targeting?.EducationSchools != null)
            .Select(xx => xx.Targeting.EducationSchools
                .Select(xxx => new
                {
                    ad_id = xx.AdId,
                    adset_id = xx.AdSetId,
                    campaign_id = xx.CampaignId,
                    account_id = xx.AccountId,
                    category_targeting_id = xxx.CategoryTargetingId,
                    name = xxx.Name,
                    parent_category = "EducationSchools"
                })).SelectMany(xxxx => xxxx);

        if (p.Any())
        {
            allCategoryTargeting.AddRange(p);
        }
        #endregion

        #region [EducationMajors]
        p = adDimensionList.Select(x => new
        {
            x.AdId,
            x.AdSetId,
            x.CampaignId,
            x.AccountId,
            x.Targeting
        })
            .Where(q => q.Targeting?.EducationMajors != null)
            .Select(xx => xx.Targeting.EducationMajors
                .Select(xxx => new
                {
                    ad_id = xx.AdId,
                    adset_id = xx.AdSetId,
                    campaign_id = xx.CampaignId,
                    account_id = xx.AccountId,
                    category_targeting_id = xxx.CategoryTargetingId,
                    name = xxx.Name,
                    parent_category = "EducationMajors"
                })).SelectMany(xxxx => xxxx);

        if (p.Any())
        {
            allCategoryTargeting.AddRange(p);
        }
        #endregion

        #region [WorkEmployers]
        p = adDimensionList.Select(x => new
        {
            x.AdId,
            x.AdSetId,
            x.CampaignId,
            x.AccountId,
            x.Targeting
        })
            .Where(q => q.Targeting?.WorkEmployers != null)
            .Select(xx => xx.Targeting.WorkEmployers
                .Select(xxx => new
                {
                    ad_id = xx.AdId,
                    adset_id = xx.AdSetId,
                    campaign_id = xx.CampaignId,
                    account_id = xx.AccountId,
                    category_targeting_id = xxx.CategoryTargetingId,
                    name = xxx.Name,
                    parent_category = "WorkEmployers"
                })).SelectMany(xxxx => xxxx);

        if (p.Any())
        {
            allCategoryTargeting.AddRange(p);
        }
        #endregion

        #region [ExcludedCustomAudiences]
        p = adDimensionList.Select(x => new
        {
            x.AdId,
            x.AdSetId,
            x.CampaignId,
            x.AccountId,
            x.Targeting
        })
            .Where(q => q.Targeting?.ExcludedCustomAudiences != null)
            .Select(xx => xx.Targeting.ExcludedCustomAudiences
                .Select(xxx => new
                {
                    ad_id = xx.AdId,
                    adset_id = xx.AdSetId,
                    campaign_id = xx.CampaignId,
                    account_id = xx.AccountId,
                    category_targeting_id = xxx.CategoryTargetingId,
                    name = xxx.Name,
                    parent_category = "ExcludedCustomAudiences"
                })).SelectMany(xxxx => xxxx);

        if (p.Any())
        {
            allCategoryTargeting.AddRange(p);
        }
        #endregion

        StageData(allCategoryTargeting, queue, WriteObjectToFile, stageFileCollection, "AdTargetingAllCategory");
    }

    private static void StageData(IEnumerable<object> transformedData, Queue queue, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile, List<FileCollectionItem> fileCollectionItems, string reportType)
    {
        FileCollectionItem fileCollectionItem = new FileCollectionItem { FilePath = $"{reportType}_{queue.FileGUID}_{queue.FileDate:yyyy-MM-dd}.csv", SourceFileName = reportType };

        // File size of the file-collection-item will be updated when the file is written
        WriteObjectToFile(transformedData, queue, fileCollectionItem);

        fileCollectionItems.Add(fileCollectionItem);
    }

    public static void LoadFacebookAdSetDimension(List<DataAdSetDimension> adSetDimensionList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        var adSets = adSetDimensionList
            .Select(x => new
            {
                id = x.Id,
                account_id = x.AccountId,
                campaign_id = x.CampaignId,
                name = x.Name,
                start_time = x.StartTime,
                end_time = x.EndTime,
                daily_budget = x.DailyBudget,
                effective_status = x.EffectiveStatus,
                lifetime_budget = x.LifetimeBudget,
                budget_remaining = x.BudgetRemaining,
                status = x.Status
            });

        StageData(adSets, queue, WriteObjectToFile, stageFileCollection, "AdSet");
    }

    public static void LoadFacebookAdAccountDimension(List<AdAccountDimension> adAccountDimensionList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        var redshiftMaxLength = int.Parse(SetupService.GetById<Model.Setup.Lookup>(Common.Constants.LOOKUP_REDSHIFT_MAX_STRING_LENGTH).Value);

        #region [Ad Account]

        var adAccount = adAccountDimensionList
            .Select(x => new
            {
                account_id = x.AccountId,
                account_status = x.AccountStatus,
                age = x.Age,
                amount_spent = x.AmountSpent,
                business_city = x.BusinessCity,
                business_country_code = x.BusinessCountryCode,
                business_name = x.BusinessName,
                business_state = x.BusinessState,
                business_street2 = x.BusinessStreet2,
                business_street = x.BusinessStreet,
                business_zip = x.BusinessZip,
                capabilities = TruncateField(x.Capabilities == null ? null : string.Join(",", x.Capabilities), redshiftMaxLength),
                currency = x.Currency,
                id = x.Id,
                is_personal = x.IsPersonal,
                name = x.Name,
                spend_cap = x.SpendCap,
                timezone_id = x.TimezoneId,
                timezone_name = x.TimezoneName,
                timezone_offset_hours_utc = x.TimezoneOffsetHoursUtc,
                tax_id_status = x.TaxIdStatus,
                balance = x.Balance
            });

        StageData(adAccount, queue, WriteObjectToFile, stageFileCollection, "AdAccount");
        #endregion

        #region [Users]

        var users = adAccountDimensionList
            .Where(y => y.Users != null)
            .Select(x => x.Users.Data
                .Select(xx => new
                {
                    account_id = x.AccountId,
                    name = xx.Name,
                    permission = TruncateField(xx.Permissions == null ? string.Empty : string.Join(",", xx.Permissions), redshiftMaxLength),
                    role = xx.Role,
                    id = xx.Id,
                })).SelectMany(xxx => xxx);

        StageData(users, queue, WriteObjectToFile, stageFileCollection, "Users");
        #endregion
    }

    public static void LoadFacebookAdStatReport(List<StatsReportData> adStatsReportList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        #region [Ad Stats Report]

        var adStats = adStatsReportList.Select(x => new
        {
            account_id = x.AccountId,
            account_name = x.AccountName,
            campaign_id = x.CampaignId,
            campaign_name = x.CampaignName,
            call_to_action_clicks = x.CallToActionClicks,
            impressions = x.Impressions,
            inline_link_clicks = x.InlineLinkClicks,
            inline_post_engagement = x.InlinePostEngagement,
            reach = x.Reach,
            relevance_score = x.RelevanceScore,
            social_clicks = x.SocialClicks,
            social_impressions = x.SocialImpressions,
            social_reach = x.SocialReach,
            social_spend = x.SocialSpend,
            spend = x.Spend,
            total_action_value = x.TotalActionValue,
            total_actions = x.TotalActions,
            total_unique_actions = x.TotalUniqueActions,
            unique_clicks = x.UniqueClicks,
            unique_social_clicks = x.UniqueSocialClicks,
            date_start = x.DateStart,
            date_stop = x.DateStop,
            adset_id = x.AdSetId,
            adset_name = x.AdSetName,
            ad_id = x.AdId,
            ad_name = x.AdName,
            clicks = x.Clicks,
            buying_type = x.BuyingType,
            frequency = x.Frequency,
            estimated_ad_recallers = x.EstimatedAdRecallers,
            publisher_platform = x.PublisherPlatform,
            platform_position = x.PlatformPosition,
            device_platform = x.DevicePlatform,
            impression_device = x.ImpressionDevice
        });

        StageData(adStats, queue, WriteObjectToFile, stageFileCollection, "AdStatsReport");
        #endregion

        #region [Actions]

        var facebookActions = GetFacebookActions<AdAction>(adStatsReportList);
        StageData(facebookActions, queue, WriteObjectToFile, stageFileCollection, "AdStatsReportActions");
        #endregion
    }

    public static void LoadFacebookAdStatActionReactions(List<StatsReportData> adStatsReportList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        #region [Actions]

        var facebookActions = GetFacebookActions<AdAction>(adStatsReportList);
        StageData(facebookActions, queue, WriteObjectToFile, stageFileCollection, "AdStatsActionReactions");
        #endregion
    }

    public static void LoadFacebookAdCreativeDimension(List<AdCreativeDimensionWithRequestAdId> adCreativeDimensionList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        var adCreative = adCreativeDimensionList
            .Where(y => y.CreativeData?.data != null)
            .SelectMany(c => c.CreativeData.data)
            .Select(x => new
            {
                id = x.Id,
                account_id = x.AccountId,
                name = x.Name,
                body = x.Body,
                object_type = x.ObjectType,
                effective_object_story_id = x.EffectiveObjectStoryId
            });

        StageData(adCreative, queue, WriteObjectToFile, stageFileCollection, "AdCreative");

        List<CreativeLinkUrl> creativeLinkUrls = new();

        #region [Asset Feed Specs]
        var assetFeedLinks = adCreativeDimensionList
            .Where(x => x.CreativeData?.data != null)
            .SelectMany(allData => allData.CreativeData.data
            .Where(y => y.AssetFeedSpec?.link_urls != null)
            .SelectMany(data => data.AssetFeedSpec.link_urls
            .Select(linkData => new CreativeLinkUrl
            {
                CreativeID = data.Id,
                AccountID = data.AccountId,
                AdID = allData.AdIdInRequest,
                LinkUrl = linkData.website_url,
                LinkDataParentObject = "AssetFeedSpec"
            })));

        if (assetFeedLinks != null && assetFeedLinks.Any())
        {
            creativeLinkUrls.AddRange(assetFeedLinks);
        }
        #endregion

        #region [Object Story Specs - link data]

        var objectStoryLinks = adCreativeDimensionList
            .Where(x => x.CreativeData?.data != null)
            .SelectMany(allData => allData.CreativeData.data
            .Where(y => y.ObjectStorySpec?.link_data?.link != null)
            .Select(linkData => new CreativeLinkUrl
            {
                CreativeID = linkData.Id,
                AccountID = linkData.AccountId,
                AdID = allData.AdIdInRequest,
                LinkUrl = linkData.ObjectStorySpec?.link_data?.link,
                LinkDataParentObject = "LinkData"
            }));

        if (objectStoryLinks != null && objectStoryLinks.Any())
        {
            creativeLinkUrls.AddRange(objectStoryLinks);
        }

        #endregion

        #region [Object Story Specs - link data child attachments]
        var objectStoryLinkChildUrls = adCreativeDimensionList
            .Where(x => x.CreativeData?.data != null)
            .SelectMany(allData => allData.CreativeData.data
            .Where(y => y.ObjectStorySpec?.link_data?.child_attachments != null)
            .SelectMany(data => data.ObjectStorySpec.link_data.child_attachments
            .Where(z => z.link != null)
            .Select(linkData => new CreativeLinkUrl
            {
                CreativeID = data.Id,
                AccountID = data.AccountId,
                AdID = allData.AdIdInRequest,
                LinkUrl = linkData.link,
                LinkDataParentObject = "LinkDataChildAttachments"
            })));

        if (objectStoryLinkChildUrls != null && objectStoryLinkChildUrls.Any())
        {
            creativeLinkUrls.AddRange(objectStoryLinkChildUrls);
        }
        #endregion

        #region [Object Story Specs - Video data]

        var objectStoryVideoLinks = adCreativeDimensionList
            .Where(x => x.CreativeData?.data != null)
            .SelectMany(allData => allData.CreativeData.data
            .Where(y => y.ObjectStorySpec?.video_data?.call_to_action?.value?.link != null)
            .Select(linkData => new CreativeLinkUrl
            {
                CreativeID = linkData.Id,
                AccountID = linkData.AccountId,
                AdID = allData.AdIdInRequest,
                LinkUrl = linkData.ObjectStorySpec?.video_data?.call_to_action?.value?.link,
                LinkDataParentObject = "VideoData"
            }));

        if (objectStoryVideoLinks != null && objectStoryVideoLinks.Any())
        {
            creativeLinkUrls.AddRange(objectStoryVideoLinks);
        }

        #endregion

        #region [Object Story Specs - Template data]

        var objectStoryTemplateLinks = adCreativeDimensionList
            .Where(x => x.CreativeData?.data != null)
            .SelectMany(allData => allData.CreativeData.data
            .Where(y => y.ObjectStorySpec?.template_data?.link != null)
            .Select(linkData => new CreativeLinkUrl
            {
                CreativeID = linkData.Id,
                AccountID = linkData.AccountId,
                AdID = allData.AdIdInRequest,
                LinkUrl = linkData.ObjectStorySpec?.template_data?.link,
                LinkDataParentObject = "TemplateData"
            }));

        if (objectStoryTemplateLinks != null && objectStoryTemplateLinks.Any())
        {
            creativeLinkUrls.AddRange(objectStoryTemplateLinks);
        }

        #endregion

        StageData(creativeLinkUrls, queue, WriteObjectToFile, stageFileCollection, "CreativeLinkData");
    }

    public static void LoadFacebookAdCampaignDimension(List<DataAdCampaignDimension> adCampaignDimensionList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        var adCampaign = adCampaignDimensionList
            .Select(x => new
            {
                id = x.Id,
                account_id = x.AccountId,
                name = x.Name,
                objective = x.Objective,
                effective_status = x.EffectiveStatus,
                daily_budget = x.DailyBudget,
                lifetime_budget = x.LifetimeBudget,
                status = x.Status
            });

        StageData(adCampaign, queue, WriteObjectToFile, stageFileCollection, "AdCampaign");
    }
    public static void LoadFacebookAdCampaignStatsReport(List<StatsReportData> adCampaignStatsReportList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        #region [Ad Campaign Stats Report]

        var adCampaignStats = adCampaignStatsReportList.Select(x => new
        {
            account_id = x.AccountId,
            account_name = x.AccountName,
            campaign_id = x.CampaignId,
            campaign_name = x.CampaignName,
            call_to_action_clicks = x.CallToActionClicks,
            impressions = x.Impressions,
            inline_link_clicks = x.InlineLinkClicks,
            inline_post_engagement = x.InlinePostEngagement,
            reach = x.Reach,
            relevance_score = x.RelevanceScore,
            social_clicks = x.SocialClicks,
            social_impressions = x.SocialImpressions,
            social_reach = x.SocialReach,
            social_spend = x.SocialSpend,
            spend = x.Spend,
            total_action_value = x.TotalActionValue,
            total_actions = x.TotalActions,
            total_unique_actions = x.TotalUniqueActions,
            unique_clicks = x.UniqueClicks,
            unique_social_clicks = x.UniqueSocialClicks,
            date_start = x.DateStart,
            date_stop = x.DateStop,
            clicks = x.Clicks,
            buying_type = x.BuyingType,
            frequency = x.Frequency,
            estimated_ad_recallers = x.EstimatedAdRecallers,
            publisher_platform = x.PublisherPlatform,
            platform_position = x.PlatformPosition,
            device_platform = x.DevicePlatform,
            impression_device = x.ImpressionDevice
        });

        StageData(adCampaignStats, queue, WriteObjectToFile, stageFileCollection, "AdCampaignStatsReport");
        #endregion

        #region [Actions]

        var facebookActions = GetFacebookActions<CampaignAction>(adCampaignStatsReportList);
        StageData(facebookActions, queue, WriteObjectToFile, stageFileCollection, "AdCampaignStatsReportActions");
        #endregion
    }

    public static void LoadFacebookAdCampaignReachReport(List<StatsReportData> adCampaignReachReportList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        #region [Ad Campaign Reach Report]

        var adCampaignStats = adCampaignReachReportList.Select(x => new
        {
            account_id = x.AccountId,
            account_name = x.AccountName,
            campaign_id = x.CampaignId,
            campaign_name = x.CampaignName,
            reach = x.Reach,
            date_start = x.DateStart,
            date_stop = x.DateStop,
            buying_type = x.BuyingType,
            frequency = x.Frequency,
            publisher_platform = x.PublisherPlatform,
            platform_position = x.PlatformPosition,
            device_platform = x.DevicePlatform,
            impression_device = x.ImpressionDevice
        });

        StageData(adCampaignStats, queue, WriteObjectToFile, stageFileCollection, "AdCampaignReachReport");
        #endregion
    }

    public static void LoadFacebookAdSetStatsReport(List<StatsReportData> adSetStatsReportList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        #region [Ad Set Stats Report]

        var adSetStats = adSetStatsReportList.Select(x => new
        {
            account_id = x.AccountId,
            account_name = x.AccountName,
            campaign_id = x.CampaignId,
            campaign_name = x.CampaignName,
            call_to_action_clicks = x.CallToActionClicks,
            impressions = x.Impressions,
            inline_link_clicks = x.InlineLinkClicks,
            inline_post_engagement = x.InlinePostEngagement,
            reach = x.Reach,
            relevance_score = x.RelevanceScore,
            social_clicks = x.SocialClicks,
            social_impressions = x.SocialImpressions,
            social_reach = x.SocialReach,
            social_spend = x.SocialSpend,
            spend = x.Spend,
            total_action_value = x.TotalActionValue,
            total_actions = x.TotalActions,
            total_unique_actions = x.TotalUniqueActions,
            unique_clicks = x.UniqueClicks,
            unique_social_clicks = x.UniqueSocialClicks,
            date_start = x.DateStart,
            date_stop = x.DateStop,
            adset_id = x.AdSetId,
            adset_name = x.AdSetName,
            clicks = x.Clicks,
            buying_type = x.BuyingType,
            frequency = x.Frequency,
            estimated_ad_recallers = x.EstimatedAdRecallers,
            publisher_platform = x.PublisherPlatform,
            platform_position = x.PlatformPosition,
            device_platform = x.DevicePlatform,
            impression_device = x.ImpressionDevice
        });

        StageData(adSetStats, queue, WriteObjectToFile, stageFileCollection, "AdSetStatsReport");
        #endregion

        #region [Actions]

        var facebookActions = GetFacebookActions<AdSetAction>(adSetStatsReportList);
        StageData(facebookActions, queue, WriteObjectToFile, stageFileCollection, "AdSetStatsReportActions");
        #endregion
    }

    public static void LoadFacebookAdSetReachReport(List<StatsReportData> adSetReachReportList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        #region [Ad Set Stats Report]

        var adSetStats = adSetReachReportList.Select(x => new
        {
            account_id = x.AccountId,
            account_name = x.AccountName,
            campaign_id = x.CampaignId,
            campaign_name = x.CampaignName,
            reach = x.Reach,
            date_start = x.DateStart,
            date_stop = x.DateStop,
            adset_id = x.AdSetId,
            adset_name = x.AdSetName,
            buying_type = x.BuyingType,
            frequency = x.Frequency,
            publisher_platform = x.PublisherPlatform,
            platform_position = x.PlatformPosition,
            device_platform = x.DevicePlatform,
            impression_device = x.ImpressionDevice
        });

        StageData(adSetStats, queue, WriteObjectToFile, stageFileCollection, "AdSetReachReport");
        #endregion
    }

    public static void LoadFacebookAdStatByDmaReport(List<StatsReportData> adStatsReportList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        #region [Ad Stats Report]

        var adStats = adStatsReportList.Select(x => new
        {
            account_id = x.AccountId,
            account_name = x.AccountName,
            campaign_id = x.CampaignId,
            campaign_name = x.CampaignName,
            call_to_action_clicks = x.CallToActionClicks,
            impressions = x.Impressions,
            inline_link_clicks = x.InlineLinkClicks,
            inline_post_engagement = x.InlinePostEngagement,
            reach = x.Reach,
            relevance_score = x.RelevanceScore,
            social_clicks = x.SocialClicks,
            social_impressions = x.SocialImpressions,
            social_reach = x.SocialReach,
            social_spend = x.SocialSpend,
            spend = x.Spend,
            total_action_value = x.TotalActionValue,
            total_actions = x.TotalActions,
            total_unique_actions = x.TotalUniqueActions,
            unique_clicks = x.UniqueClicks,
            unique_social_clicks = x.UniqueSocialClicks,
            date_start = x.DateStart,
            date_stop = x.DateStop,
            adset_id = x.AdSetId,
            adset_name = x.AdSetName,
            ad_id = x.AdId,
            ad_name = x.AdName,
            clicks = x.Clicks,
            buying_type = x.BuyingType,
            frequency = x.Frequency,
            estimated_ad_recallers = x.EstimatedAdRecallers,
            dma = x.DMA
        });

        StageData(adStats, queue, WriteObjectToFile, stageFileCollection, "AdStatsByDMAReport");
        #endregion

        #region [Actions]

        var facebookActions = GetFacebookActions<AdActionDMA>(adStatsReportList);
        StageData(facebookActions, queue, WriteObjectToFile, stageFileCollection, "AdStatsByDMAReportActions");
        #endregion
    }

    public static void LoadFacebookAdStatByCountryReport(List<StatsReportData> adStatsReportList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        #region [Ad Stats Report]

        var adStats = adStatsReportList.Select(x => new
        {
            account_id = x.AccountId,
            account_name = x.AccountName,
            campaign_id = x.CampaignId,
            campaign_name = x.CampaignName,
            call_to_action_clicks = x.CallToActionClicks,
            impressions = x.Impressions,
            inline_link_clicks = x.InlineLinkClicks,
            inline_post_engagement = x.InlinePostEngagement,
            reach = x.Reach,
            relevance_score = x.RelevanceScore,
            social_clicks = x.SocialClicks,
            social_impressions = x.SocialImpressions,
            social_reach = x.SocialReach,
            social_spend = x.SocialSpend,
            spend = x.Spend,
            total_action_value = x.TotalActionValue,
            total_actions = x.TotalActions,
            total_unique_actions = x.TotalUniqueActions,
            unique_clicks = x.UniqueClicks,
            unique_social_clicks = x.UniqueSocialClicks,
            date_start = x.DateStart,
            date_stop = x.DateStop,
            adset_id = x.AdSetId,
            adset_name = x.AdSetName,
            ad_id = x.AdId,
            ad_name = x.AdName,
            clicks = x.Clicks,
            buying_type = x.BuyingType,
            frequency = x.Frequency,
            estimated_ad_recallers = x.EstimatedAdRecallers,
            country = x.Country,
            region = x.Region
        });

        StageData(adStats, queue, WriteObjectToFile, stageFileCollection, "AdStatsByCountryReport");
        #endregion

        #region [Actions]

        var facebookActions = GetFacebookActions<AdActionCountry>(adStatsReportList);
        StageData(facebookActions, queue, WriteObjectToFile, stageFileCollection, "AdStatsByCountryReportActions");
        #endregion
    }

    private static List<T> GetFacebookActions<T>(List<StatsReportData> adStatsReportList)
    {
        var facebookActions = new List<T>();

        var props = typeof(StatsReportData).GetProperties().Where(p => p.PropertyType.ToString().Contains("StatsReportActions"));

        if (!props.Any()) return facebookActions;

        foreach (var prop in props)
        {
            foreach (var stats in adStatsReportList)
            {
                var actions = (List<StatsReportActions>)stats.GetType().GetProperty(prop.Name).GetValue(stats, null);
                if (actions == null || actions.Count == 0) continue;
                var actionList = CreateActionList<T>(stats, actions, prop.Name);

                facebookActions.AddRange(actionList);
            }
        }
        return facebookActions;
    }

    public static string TruncateField(string value, int maxLength)
    {
        if (string.IsNullOrEmpty(value)) return value;
        return value.Length <= maxLength ? value : value.Substring(0, maxLength);
    }

    public static List<T> CreateActionList<T>(StatsReportData statsReportData, List<StatsReportActions> statsReportActions, string actionCategory)
    {
        var actionList = new List<T>();

        actionList.AddRange(statsReportActions.Where(action => action != null).Select(action =>
            (T)Activator.CreateInstance(typeof(T), statsReportData,
                action,
                actionCategory)));

        return actionList;
    }

    public static void LoadCustomConversionsDimension(List<DataCustomConversionDimension> customConversionsDimensionList, Queue queue, List<FileCollectionItem> stageFileCollection, Action<IEnumerable<object>, Queue, FileCollectionItem> WriteObjectToFile)
    {
        var entityID = queue.EntityID;

        var customConversions = customConversionsDimensionList
            .Select(x => new
            {
                id = x.Id,
                account_id = x.AccountId,
                name = x.Name,
                custom_event_type = x.CustomEventType,
                aggregation_rule = x.AggregationRule,
                business_id = x?.Business?.Id,
                business_name = x?.Business?.Name,
                creation_time = x.CreationTime,
                default_conversion_value = x.DefaultConversionValue,
                description = x.Description,
                event_source_type = x.EventSourceType,
                first_fired_time = x.FirstFiredTime,
                is_archived = x.IsArchived,
                is_unavailable = x.IsUnavailable,
                last_fired_time = x.LastFiredTime,
                offline_conversion_data_set_id = x?.OfflineConversionDataSet?.Id,
                offline_conversion_data_set_name = x?.OfflineConversionDataSet?.Name,
                retention_days = x.RetentionDays,
                entity_id = entityID
            });

        StageData(customConversions, queue, WriteObjectToFile, stageFileCollection, "CustomConversions");
        #region Data Sources
        var dataSources = customConversionsDimensionList
            .Where(y => y.DataSources != null)
            .SelectMany(x => x.DataSources
                .Select(xx => new
                {
                    id = xx.Id,
                    name = xx.Name,
                    source_type = xx.SourceType,
                    account_id = x.AccountId,
                    custom_conversion_id = x.Id,
                    entity_id = entityID
                }));

        StageData(dataSources, queue, WriteObjectToFile, stageFileCollection, "CustomConversionsDataSources");
        #endregion
    }
}
