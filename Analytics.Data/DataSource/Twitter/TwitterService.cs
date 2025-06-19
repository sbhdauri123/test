using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.DataSource.Twitter
{
    public static class TwitterService
    {
        public static void StageEngagementMetric(string fileGuid, string reportName, ReportSettings reportTypeSettings,
            List<RootStatsJob> statsData, string entityId, DateTime fileDate,
            Action<JArray, string, DateTime, string> writeObjectToFile, string fileName, ApiReportItem report)
        {
            var allEngagementMetrics = new List<object>();

            foreach (var reportEntity in statsData)
            {
                for (var i = 0; i < reportEntity.time_series_length; i++)
                {
                    foreach (var entityData in reportEntity.data)
                    {
                        var hourIndex = i;
                        var engagementMetrics = entityData.id_data.Select(x =>
                            new
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                impressions = x.metrics?.impressions?[hourIndex],
                                tweets_send = x.metrics?.tweets_send?[hourIndex],
                                qualified_impressions = x.metrics?.qualified_impressions?[hourIndex],
                                follows = x.metrics?.follows?[hourIndex],
                                app_clicks = x.metrics?.app_clicks?[hourIndex],
                                retweets = x.metrics?.retweets?[hourIndex],
                                unfollows = x.metrics?.unfollows?[hourIndex],
                                likes = x.metrics?.likes?[hourIndex],
                                engagements = x.metrics?.engagements?[hourIndex],
                                clicks = x.metrics?.clicks?[hourIndex],
                                card_engagements = x.metrics?.card_engagements?[hourIndex],
                                poll_card_vote = x.metrics?.poll_card_vote?[hourIndex],
                                replies = x.metrics?.replies?[hourIndex],
                                url_clicks = x.metrics?.url_clicks?[hourIndex],
                                carousel_swipes = x.metrics?.carousel_swipes?[hourIndex],
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (engagementMetrics.Any())
                        {
                            allEngagementMetrics.AddRange(engagementMetrics);
                        }
                    }
                }
            }

            var engagementMetricObjectToSerialize = JArray.FromObject(allEngagementMetrics);
            writeObjectToFile(engagementMetricObjectToSerialize, entityId, fileDate,
                string.Format(fileName, "engagement"));
        }

        public static void StageBillingMetric(string fileGuid, string reportName, ReportSettings reportTypeSettings,
            List<RootStatsJob> statsData, string entityId, DateTime fileDate,
            Action<JArray, string, DateTime, string> writeObjectToFile, string fileName, ApiReportItem report)
        {
            var allBillingMetrics = new List<object>();

            foreach (var reportEntity in statsData)
            {
                for (var i = 0; i < reportEntity.time_series_length; i++)
                {
                    foreach (var entityData in reportEntity.data)
                    {
                        var hourIndex = i;
                        var billingMetrics = entityData.id_data.Select(x =>
                            new
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                billed_charge_local_micro = x.metrics?.billed_charge_local_micro?[hourIndex],
                                billed_engagements = x.metrics?.billed_engagements?[hourIndex],
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (billingMetrics.Any())
                        {
                            allBillingMetrics.AddRange(billingMetrics);
                        }
                    }
                }
            }

            var billingMetricObjectToSerialize = JArray.FromObject(allBillingMetrics);
            writeObjectToFile(billingMetricObjectToSerialize, entityId, fileDate,
                string.Format(fileName, "billing"));
        }

        public static void StageVideoMetric(string fileGuid, string reportName, ReportSettings reportTypeSettings,
            List<RootStatsJob> statsData, string entityId, DateTime fileDate,
            Action<JArray, string, DateTime, string> writeObjectToFile, string fileName, ApiReportItem report)
        {
            var allVideoMetrics = new List<object>();

            foreach (var reportEntity in statsData)
            {
                for (var i = 0; i < reportEntity.time_series_length; i++)
                {
                    foreach (var entityData in reportEntity.data)
                    {
                        var hourIndex = i;
                        var videoMetrics = entityData.id_data.Select(x =>
                            new
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                video_views_50 = x.metrics?.video_views_50?[hourIndex],
                                video_views_75 = x.metrics?.video_views_75?[hourIndex],
                                video_3s100pct_views = x.metrics?.video_3s100pct_views?[hourIndex],
                                video_cta_clicks = x.metrics?.video_cta_clicks?[hourIndex],
                                video_content_starts = x.metrics?.video_content_starts?[hourIndex],
                                video_views_25 = x.metrics?.video_views_25?[hourIndex],
                                video_views_100 = x.metrics?.video_views_100?[hourIndex],
                                video_6s_views = x.metrics?.video_6s_views?[hourIndex],
                                video_mrc_views = x.metrics?.video_mrc_views?[hourIndex],
                                video_total_views = x.metrics?.video_total_views?[hourIndex],
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (videoMetrics.Any())
                        {
                            allVideoMetrics.AddRange(videoMetrics);
                        }
                    }
                }
            }

            var videoMetricObjectToSerialize = JArray.FromObject(allVideoMetrics);
            writeObjectToFile(videoMetricObjectToSerialize, entityId, fileDate,
                string.Format(fileName, "video"));
        }

        public static void StageMediaMetric(string fileGuid, string reportName, ReportSettings reportTypeSettings,
            List<RootStatsJob> statsData, string entityId, DateTime fileDate,
            Action<JArray, string, DateTime, string> writeObjectToFile, string fileName, ApiReportItem report)
        {
            var allMediaMetrics = new List<object>();

            foreach (var reportEntity in statsData)
            {
                for (var i = 0; i < reportEntity.time_series_length; i++)
                {
                    foreach (var entityData in reportEntity.data)
                    {
                        var hourIndex = i;
                        var mediaMetrics = entityData.id_data.Select(x =>
                            new
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                media_views = x.metrics?.media_views?[hourIndex],
                                media_engagements = x.metrics?.media_engagements?[hourIndex],
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mediaMetrics.Any())
                        {
                            allMediaMetrics.AddRange(mediaMetrics);
                        }
                    }
                }
            }

            var mediaMetricObjectToSerialize = JArray.FromObject(allMediaMetrics);
            writeObjectToFile(mediaMetricObjectToSerialize, entityId, fileDate,
                string.Format(fileName, "media"));
        }

        public static void StageWebConversionMetric(string fileGuid, string reportName, ReportSettings reportTypeSettings,
            List<RootStatsJob> statsData, string entityId, DateTime fileDate,
            Action<JArray, string, DateTime, string> writeObjectToFile, string fileName, ApiReportItem report)
        {
            var allWebConversionMetrics = new List<ConversionMetricGroup>();

            foreach (var reportEntity in statsData)
            {
                for (var i = 0; i < reportEntity.time_series_length; i++)
                {
                    foreach (var entityData in reportEntity.data)
                    {
                        var hourIndex = i;
                        var webConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                order_quantity_engagement =
                                    x.metrics?.conversion_purchases?.order_quantity_engagement?[hourIndex],
                                sale_amount_engagement = x.metrics?.conversion_purchases?.sale_amount_engagement?[hourIndex],
                                sale_amount_view = x.metrics?.conversion_purchases?.sale_amount_view?[hourIndex],
                                post_view = x.metrics?.conversion_purchases?.post_view?[hourIndex],
                                order_quantity = x.metrics?.conversion_purchases?.order_quantity?[hourIndex],
                                assisted = x.metrics?.conversion_purchases?.assisted?[hourIndex],
                                post_engagement = x.metrics?.conversion_purchases?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.conversion_purchases?.sale_amount?[hourIndex],
                                metric = x.metrics?.conversion_purchases?.metric?[hourIndex],
                                order_quantity_view = x.metrics?.conversion_purchases?.order_quantity_view?[hourIndex],
                                conversion_type = "PURCHASE",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (webConversionMetrics.Any())
                        {
                            allWebConversionMetrics.AddRange(webConversionMetrics);
                        }

                        webConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                order_quantity_engagement =
                                    x.metrics?.conversion_sign_ups?.order_quantity_engagement?[hourIndex],
                                sale_amount_engagement = x.metrics?.conversion_sign_ups?.sale_amount_engagement?[hourIndex],
                                sale_amount_view = x.metrics?.conversion_sign_ups?.sale_amount_view?[hourIndex],
                                post_view = x.metrics?.conversion_sign_ups?.post_view?[hourIndex],
                                order_quantity = x.metrics?.conversion_sign_ups?.order_quantity?[hourIndex],
                                assisted = x.metrics?.conversion_sign_ups?.assisted?[hourIndex],
                                post_engagement = x.metrics?.conversion_sign_ups?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.conversion_sign_ups?.sale_amount?[hourIndex],
                                metric = x.metrics?.conversion_sign_ups?.metric?[hourIndex],
                                order_quantity_view = x.metrics?.conversion_sign_ups?.order_quantity_view?[hourIndex],
                                conversion_type = "SIGN_UP",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (webConversionMetrics.Any())
                        {
                            allWebConversionMetrics.AddRange(webConversionMetrics);
                        }

                        webConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                order_quantity_engagement =
                                    x.metrics?.conversion_site_visits?.order_quantity_engagement?[hourIndex],
                                sale_amount_engagement = x.metrics?.conversion_site_visits?.sale_amount_engagement?[hourIndex],
                                sale_amount_view = x.metrics?.conversion_site_visits?.sale_amount_view?[hourIndex],
                                post_view = x.metrics?.conversion_site_visits?.post_view?[hourIndex],
                                order_quantity = x.metrics?.conversion_site_visits?.order_quantity?[hourIndex],
                                //                            assisted = x.metrics?.conversion_site_visits?.assisted[hourIndex],
                                post_engagement = x.metrics?.conversion_site_visits?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.conversion_site_visits?.sale_amount?[hourIndex],
                                metric = x.metrics?.conversion_site_visits?.metric?[hourIndex],
                                order_quantity_view = x.metrics?.conversion_site_visits?.order_quantity_view?[hourIndex],
                                conversion_type = "SITE_VISIT",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (webConversionMetrics.Any())
                        {
                            allWebConversionMetrics.AddRange(webConversionMetrics);
                        }

                        webConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                order_quantity_engagement =
                                    x.metrics?.conversion_downloads?.order_quantity_engagement?[hourIndex],
                                sale_amount_engagement = x.metrics?.conversion_downloads?.sale_amount_engagement?[hourIndex],
                                sale_amount_view = x.metrics?.conversion_downloads?.sale_amount_view?[hourIndex],
                                post_view = x.metrics?.conversion_downloads?.post_view?[hourIndex],
                                order_quantity = x.metrics?.conversion_downloads?.order_quantity?[hourIndex],
                                //                            assisted = null,//x.metrics?.conversion_downloads?.assisted[hourIndex],
                                post_engagement = x.metrics?.conversion_downloads?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.conversion_downloads?.sale_amount?[hourIndex],
                                metric = x.metrics?.conversion_downloads?.metric?[hourIndex],
                                order_quantity_view = x.metrics?.conversion_downloads?.order_quantity_view?[hourIndex],
                                conversion_type = "DOWNLOAD",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (webConversionMetrics.Any())
                        {
                            allWebConversionMetrics.AddRange(webConversionMetrics);
                        }

                        webConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                order_quantity_engagement = x.metrics?.conversion_custom?.order_quantity_engagement?[hourIndex],
                                sale_amount_engagement = x.metrics?.conversion_custom?.sale_amount_engagement?[hourIndex],
                                sale_amount_view = x.metrics?.conversion_custom?.sale_amount_view?[hourIndex],
                                post_view = x.metrics?.conversion_custom?.post_view?[hourIndex],
                                order_quantity = x.metrics?.conversion_custom?.order_quantity?[hourIndex],
                                //                            assisted = x.metrics?.conversion_custom?.assisted[hourIndex],
                                post_engagement = x.metrics?.conversion_custom?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.conversion_custom?.sale_amount?[hourIndex],
                                metric = x.metrics?.conversion_custom?.metric?[hourIndex],
                                order_quantity_view = x.metrics?.conversion_custom?.order_quantity_view?[hourIndex],
                                conversion_type = "CUSTOM",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (webConversionMetrics.Any())
                        {
                            allWebConversionMetrics.AddRange(webConversionMetrics);
                        }
                    }
                }
            }

            var webConversionMetricObjectToSerialize = JArray.FromObject(allWebConversionMetrics);
            writeObjectToFile(webConversionMetricObjectToSerialize, entityId, fileDate,
                string.Format(fileName, "web_conversion"));
        }

        public static void StageMobileConversionMetric(string fileGuid, string reportName, ReportSettings reportTypeSettings,
            List<RootStatsJob> statsData, string entityId, DateTime fileDate,
            Action<JArray, string, DateTime, string> writeObjectToFile, string fileName, ApiReportItem report)
        {
            var allMobileConversionMetrics = new List<ConversionMetricGroup>();

            foreach (var reportEntity in statsData)
            {
                for (var i = 0; i < reportEntity.time_series_length; i++)
                {
                    foreach (var entityData in reportEntity.data)
                    {
                        var hourIndex = i;
                        var mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_spent_credits?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_spent_credits?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_spent_credits?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_spent_credits?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_spent_credits?.sale_amount?[hourIndex],
                                conversion_type = "SPENT_CREDIT",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_installs?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_installs?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_installs?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_installs?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_installs?.sale_amount?[hourIndex],
                                conversion_type = "INSTALL",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_content_views?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_content_views?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_content_views?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_content_views?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_content_views?.sale_amount?[hourIndex],
                                conversion_type = "CONTENT_VIEW",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_add_to_wishlists?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_add_to_wishlists?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_add_to_wishlists?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_add_to_wishlists?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_add_to_wishlists?.sale_amount?[hourIndex],
                                conversion_type = "ADD_TO_WISHLIST",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_checkouts_initiated?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_checkouts_initiated?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_checkouts_initiated?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_checkouts_initiated?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_checkouts_initiated?.sale_amount?[hourIndex],
                                conversion_type = "CHECKOUT_INITIATED",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_reservations?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_reservations?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_reservations?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_reservations?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_reservations?.sale_amount?[hourIndex],
                                conversion_type = "RESERVATION",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_tutorials_completed?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_tutorials_completed?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_tutorials_completed?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_tutorials_completed?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_tutorials_completed?.sale_amount?[hourIndex],
                                conversion_type = "TUTORIAL_COMPLETED",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_achievements_unlocked?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_achievements_unlocked?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_achievements_unlocked?.assisted?[hourIndex],
                                post_engagement =
                                    x.metrics?.mobile_conversion_achievements_unlocked?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_achievements_unlocked?.sale_amount?[hourIndex],
                                conversion_type = "ACHIEVEMENT_UNLOCKED",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_searches?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_searches?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_searches?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_searches?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_searches?.sale_amount?[hourIndex],
                                conversion_type = "SEARCH",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_add_to_carts?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_add_to_carts?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_add_to_carts?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_add_to_carts?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_add_to_carts?.sale_amount?[hourIndex],
                                conversion_type = "ADD_TO_CART",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_payment_info_additions?.post_view?[hourIndex],
                                order_quantity =
                                    x.metrics?.mobile_conversion_payment_info_additions?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_payment_info_additions?.assisted?[hourIndex],
                                post_engagement =
                                    x.metrics?.mobile_conversion_payment_info_additions?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_payment_info_additions?.sale_amount?[hourIndex],
                                conversion_type = "PAYMENT_INFO_ADDITION",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_re_engages?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_re_engages?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_re_engages?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_re_engages?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_re_engages?.sale_amount?[hourIndex],
                                conversion_type = "RE_ENGAGE",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_shares?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_shares?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_shares?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_shares?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_shares?.sale_amount?[hourIndex],
                                conversion_type = "SHARE",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_rates?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_rates?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_rates?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_rates?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_rates?.sale_amount?[hourIndex],
                                conversion_type = "RATE",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_logins?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_logins?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_logins?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_logins?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_logins?.sale_amount?[hourIndex],
                                conversion_type = "LOGIN",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_updates?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_updates?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_updates?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_updates?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_updates?.sale_amount?[hourIndex],
                                conversion_type = "UPDATE",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_levels_achieved?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_levels_achieved?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_levels_achieved?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_levels_achieved?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_levels_achieved?.sale_amount?[hourIndex],
                                conversion_type = "LEVEL_ACHIEVED",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_invites?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_invites?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_invites?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_invites?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_invites?.sale_amount?[hourIndex],
                                conversion_type = "INVITE",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_key_page_views?.post_view?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_key_page_views?.post_engagement?[hourIndex],
                                conversion_type = "KEY_PAGE_VIEW",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_site_visits?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_site_visits?.order_quantity?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_site_visits?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_site_visits?.sale_amount?[hourIndex],
                                conversion_type = "SITE_VISIT",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_sign_ups?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_sign_ups?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_sign_ups?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_sign_ups?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_sign_ups?.sale_amount?[hourIndex],
                                conversion_type = "SIGN_UP",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_purchases?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_purchases?.order_quantity?[hourIndex],
                                assisted = x.metrics?.mobile_conversion_purchases?.assisted?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_purchases?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_purchases?.sale_amount?[hourIndex],
                                conversion_type = "PURCHASE",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }

                        mobileConversionMetrics = entityData.id_data.Select(x =>
                            new ConversionMetricGroup()
                            {
                                account_id = entityId,
                                entity_id = entityData.id,
                                data_date = reportEntity.request.@params.start_time,
                                data_hour = hourIndex,
                                placement = reportEntity.request.@params.placement,
                                post_view = x.metrics?.mobile_conversion_downloads?.post_view?[hourIndex],
                                order_quantity = x.metrics?.mobile_conversion_downloads?.order_quantity?[hourIndex],
                                post_engagement = x.metrics?.mobile_conversion_downloads?.post_engagement?[hourIndex],
                                sale_amount = x.metrics?.mobile_conversion_downloads?.sale_amount?[hourIndex],
                                conversion_type = "DOWNLOAD",
                                country = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_name : string.Empty,
                                country_id = string.IsNullOrEmpty(report.DMACountryID) ? x.segment?.segment_value : report.DMACountryID,
                                dma = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_name,
                                dma_id = string.IsNullOrEmpty(report.DMACountryID) ? null : x.segment?.segment_value
                            });
                        if (mobileConversionMetrics.Any())
                        {
                            allMobileConversionMetrics.AddRange(mobileConversionMetrics);
                        }
                    }
                }
            }

            var mobileConversionMetricObjectToSerialize = JArray.FromObject(allMobileConversionMetrics);
            writeObjectToFile(mobileConversionMetricObjectToSerialize, entityId, fileDate,
                string.Format(fileName, "mobile_conversion"));
        }

        public static void StageAccounts(string entityId, DateTime fileDate, List<DimensionReport<AccountDimensionReport>> fullData, string fileName, Action<JArray, string, DateTime, string> writeToFileSignature)
        {
            var flatData = fullData.SelectMany(d => d.Data, (d, data) => new
            {
                name = data.Name,
                business_name = data.BusinessName,
                timezone = data.TimeZone,
                timezone_switch_at = data.TimeZoneSwitchAt,
                id = data.Id,
                created_at = data.CreatedAt,
                salt = data.Salt,
                updated_at = data.UpdatedAt,
                business_id = data.BusinessId,
                approval_status = data.ApprovalStatus
            });

            writeToFileSignature(JArray.FromObject(flatData), entityId, fileDate, fileName);
        }

        public static void StageCampaigns(string entityId, DateTime fileDate, List<DimensionReport<CampaignDimensionReport>> fullData, string fileName, Action<JArray, string, DateTime, string> writeToFileSignature)
        {
            var flatData = fullData.SelectMany(d => d.Data, (d, data) => new
            {
                account_id = entityId,
                name = data.Name,
                start_time = data.StartTime,
                servable = data.Servable,
                daily_budget_amount_local_micro = data.DailyBudgetAmountLocalMicro,
                end_time = data.EndTime,
                funding_instrument_id = data.FundingInstrumentId,
                duration_in_days = data.DurationInDays,
                standard_delivery = data.StandardDelivery,
                total_budget_amount_local_micro = data.TotalBudgetAmountLocalMicro,
                id = data.Id,
                entity_status = data.EntityStatus,
                frequency_cap = data.FrequencyCap,
                currency = data.Currency,
                created_at = data.CreatedAt,
                updated_at = data.UpdatedAt,
                reason_not_servable = string.Join(",", data.ReasonsNotServable)
            });

            writeToFileSignature(JArray.FromObject(flatData), entityId, fileDate, fileName);
        }

        public static void StageLineItem(string entityId, DateTime fileDate, List<DimensionReport<LineItemDimensionReport>> fullData, string fileName, Action<JArray, string, DateTime, string> writeToFileSignature)
        {
            var flatData = fullData.SelectMany(d => d.Data, (d, data) => new
            {
                account_id = entityId,
                bid_type = data.BidType,
                advertiser_user_id = data.AdvertiserUserId,
                name = data.Name,
                start_time = data.StartTime,
                bid_amount_local_micro = data.BidAmountLocalMicro,
                automatically_select_bid = data.AutomaticallySelectBid,
                advertiser_domain = data.Advertiser_Domain,
                target_cpa_local_micro = data.TargetCpaLocalMicro,
                primary_web_event_tag = data.PrimaryWebEventTag,
                charge_by = data.ChargeBy,
                product_type = data.ProductType,
                end_time = data.EndTime,
                bid_unit = data.BidUnit,
                total_budget_amount_local_micro = data.TotalBudgetAmountLocalMicro,
                objective = data.Objective,
                id = data.Id,
                entity_status = data.EntityStatus,
                currency = data.Currency,
                created_at = data.CreatedAt,
                updated_at = data.UpdatedAt,
                include_sentiment = data.IncludeSentiment,
                campaign_id = data.CampaignId,
                creative_source = data.CreativeSource,
                placements = string.Join(",", data.Placements),
                categories = string.Join(",", data.Categories),
                tracking_tags = (data.TrackingTags != null && data.TrackingTags.Count != 0) ? string.Join(",", data.TrackingTags.Select(t => $"{t.TrackingPartner}-{t.TrackingTag}")) : null
            });

            writeToFileSignature(JArray.FromObject(flatData), entityId, fileDate, fileName);
        }

        public static void StagePromotedTweet(string entityId, DateTime fileDate, List<DimensionReport<PromotedTweetDimensionReport>> fullData, string fileName, Action<JArray, string, DateTime, string> writeToFileSignature)
        {
            var flatData = fullData.SelectMany(d => d.Data, (d, data) => new
            {
                account_id = entityId,
                line_item_id = data.LineItemId,
                id = data.Id,
                entity_status = data.EntityStatus,
                created_at = data.CreatedAt,
                updated_at = data.UpdatedAt,
                approval_status = data.ApprovalStatus,
                tweet_id = data.TweetId
            });

            writeToFileSignature(JArray.FromObject(flatData), entityId, fileDate, fileName);
        }

        public static void StageMediaCreative(string entityId, DateTime fileDate, List<DimensionReport<MediaCreativeDimensionReport>> fullData, string fileName, Action<JArray, string, DateTime, string> writeToFileSignature)
        {
            var flatData = fullData.SelectMany(d => d.Data, (d, data) => new
            {
                account_id = entityId,
                line_item_id = data.LineItemId,
                landing_url = data.LandingUrl,
                serving_status = data.ServingStatus,
                id = data.Id,
                created_at = data.CreatedAt,
                account_media_id = data.AccountMediaId,
                updated_at = data.UpdatedAt,
                approval_status = data.ApprovalStatus
            });

            writeToFileSignature(JArray.FromObject(flatData), entityId, fileDate, fileName);
        }
    }
}
