using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;

namespace Greenhouse.Data.DataSource.RUNDSP.RUN1
{
    public static class Run1Service
    {
        public static List<T> GetData<T>(Stream filesStream)
        {
            var dataList = new List<T>();
            using (var strm = new StreamReader(filesStream))
            {
                var line = "";
                while ((line = strm.ReadLine()) != null)
                {
                    var data = JsonConvert.DeserializeObject<T>(line);
                    dataList.Add(data);
                }
            }

            return dataList;
        }

        public static DataTable CreateDataTable<T>(IEnumerable<T> items, Guid guid)
        {
            DataTable dt = null;
            var dtExt = new Greenhouse.Utilities.DataTables.ObjectShredder<T>();
            var dataTable = dtExt.Shred(items, dt, null);

            dataTable.Columns.Add(
                new DataColumn("FileGUID", typeof(string)) { DefaultValue = guid });
            return dataTable;
        }

        public static Dictionary<string, DataTable> GetRun1AccountData(IEnumerable<dynamic> metadataConfig, Stream fileStream, Guid guid)
        {
            var metadataToProcess = new Dictionary<string, DataTable>();
            var accountList = GetData<Account>(fileStream);

            foreach (var config in metadataConfig)
            {
                if (config.TableName.Equals("rundsp_match_run1_account"))
                {
                    var accountData = accountList.Select(x => new
                    {
                        id = x.Id,
                        account_id = x.AccountId,
                        account_type = x.AccountType,
                        ad_serving_fee = x.AdServingFee,
                        name = x.Name,
                        platform_fee = x.PlatformFee
                    });
                    var accountDataTable = CreateDataTable(accountData, guid);
                    metadataToProcess.Add(config.TableName, accountDataTable);
                }
            }

            return metadataToProcess;
        }

        public static Dictionary<string, DataTable> GetRun1AdUnitData(IEnumerable<dynamic> metadataConfig, Stream fileStream, Guid guid)
        {
            var metadataToProcess = new Dictionary<string, DataTable>();
            var adUnitList = GetData<AdUnit>(fileStream);

            foreach (var config in metadataConfig)
            {
                if (config.TableName.Equals("rundsp_match_run1_adunit"))
                {
                    var adunitData = adUnitList.Select(x => new
                    {
                        id = x.Id,
                        adunit_id = x.AdUnitId,
                        campaign_id = x.CampaignId,
                        name = x.Name,
                    });
                    var adunitDataTable = CreateDataTable(adunitData, guid);
                    metadataToProcess.Add(config.TableName, adunitDataTable);
                }
                else if (config.TableName.Equals("rundsp_match_run1_adunitplacement"))
                {
                    var adunitPlacementData = adUnitList
                        .Where(y => y.PlacementIdList != null)
                        .Select(x => x.PlacementIdList
                            .Select(xx => new
                            {
                                id = x.Id,
                                adunit_id = x.AdUnitId,
                                placement_id = xx
                            })).SelectMany(xxx => xxx);
                    var adunitPlacementDataTable = CreateDataTable(adunitPlacementData, guid);
                    metadataToProcess.Add(config.TableName, adunitPlacementDataTable);
                }
            }

            return metadataToProcess;
        }

        public static Dictionary<string, DataTable> GetRun1AdvertiserData(IEnumerable<dynamic> metadataConfig, Stream fileStream, Guid guid)
        {
            var metadataToProcess = new Dictionary<string, DataTable>();
            var advertiserList = GetData<Advertiser>(fileStream);

            foreach (var config in metadataConfig)
            {
                if (config.TableName.Equals("rundsp_match_run1_advertiser"))
                {
                    var advertiserData = advertiserList.Select(x => new
                    {
                        id = x.Id,
                        account_id = x.AccountId,
                        adgear_id = x.AdgearId,
                        adv_id = x.AdvId,
                        name = x.Name
                    });
                    var advertiserDataTable = CreateDataTable(advertiserData, guid);
                    metadataToProcess.Add(config.TableName, advertiserDataTable);
                }
            }

            return metadataToProcess;
        }
        public static Dictionary<string, DataTable> GetRun1CampaignData(IEnumerable<dynamic> metadataConfig, Stream fileStream, Guid guid)
        {
            var metadataToProcess = new Dictionary<string, DataTable>();
            var campaignList = GetData<Campaign>(fileStream);

            foreach (var config in metadataConfig)
            {
                if (config.TableName.Equals("rundsp_match_run1_campaign"))
                {
                    var campaignData = campaignList.Select(x => new
                    {
                        id = x.Id,
                        account_id = x.AccountId,
                        advertiser_id = x.AdvertiserId,
                        campaign_id = x.CampaignId,
                        clearing_cost_enabled = x.ClearingCostEnabled,
                        data_cost_enabled = x.DataCostEnabled,
                        end_at = x.EndAt,
                        ias_cost_enabled = x.IasCostEnabled,
                        ias_qe_cost_enabled = x.IasQeCostEnabled,
                        name = x.Name,
                        media_spend = x.MediaSpend,
                        run_data_fee_amount = x.RunDataFeeAmount,
                        run_fee_amount = x.RunFeeAmount,
                        start_at = x.StartAt,
                        tms_type = x.TmsType,
                        vendor_cost_enabled = x.VendorCostEnabled
                    });
                    var campaignDataTable = CreateDataTable(campaignData, guid);
                    metadataToProcess.Add(config.TableName, campaignDataTable);
                }
            }

            return metadataToProcess;
        }

        public static Dictionary<string, DataTable> GetRun1GeoShapeData(IEnumerable<dynamic> metadataConfig, Stream fileStream, Guid guid)
        {
            var metadataToProcess = new Dictionary<string, DataTable>();
            var geoShapeList = GetData<GeoShape>(fileStream);

            foreach (var config in metadataConfig)
            {
                if (config.TableName.Equals("rundsp_match_run1_geoshape"))
                {
                    var geoShapeData = geoShapeList.Select(x => new
                    {
                        id = x.Id,
                        country = x.Country,
                        display_name = x.DisplayName,
                        geo_id = x.GeoId,
                        name = x.Name,
                        type = x.Type
                    });
                    var geoShapeDataTable = CreateDataTable(geoShapeData, guid);
                    metadataToProcess.Add(config.TableName, geoShapeDataTable);
                }
            }

            return metadataToProcess;
        }

        public static Dictionary<string, DataTable> GetRun1PlacementData(IEnumerable<dynamic> metadataConfig, Stream fileStream, Guid guid)
        {
            var metadataToProcess = new Dictionary<string, DataTable>();
            var placementList = GetData<Placement>(fileStream);

            foreach (var config in metadataConfig)
            {
                if (config.TableName.Equals("rundsp_match_run1_placement"))
                {
                    var placementData = placementList.Select(x => new
                    {
                        id = x.Id,
                        campaign_id = x.CampaignId,
                        delivery_type = x.DeliveryType,
                        name = x.Name,
                        placement_id = x.PlacementId,
                        placement_objective_start_at = x.PlacementObjective.StartAt,
                        placement_objective_end_at = x.PlacementObjective.EndAt
                    });
                    var placementDataTable = CreateDataTable(placementData, guid);
                    metadataToProcess.Add(config.TableName, placementDataTable);
                }
            }

            return metadataToProcess;
        }

        public static Dictionary<string, DataTable> GetRun1PmpDealData(IEnumerable<dynamic> metadataConfig, Stream fileStream, Guid guid)
        {
            var metadataToProcess = new Dictionary<string, DataTable>();
            var pmpDealList = GetData<PmpDeal>(fileStream);

            foreach (var config in metadataConfig)
            {
                if (config.TableName.Equals("rundsp_match_run1_pmpdeal"))
                {
                    var pmpDealData = pmpDealList.Select(x => new
                    {
                        id = x.Id,
                        type = x.Type,
                        deal_id = x.DealId,
                        name = x.Name,
                        run_deal_id = x.RunDealId
                    });
                    var pmpDealDataTable = CreateDataTable(pmpDealData, guid);
                    metadataToProcess.Add(config.TableName, pmpDealDataTable);
                }
            }

            return metadataToProcess;
        }

        public static Dictionary<string, DataTable> GetRun1Metadata(int sourceID, FileCollectionItem file, Stream fileStream,
            Guid guid)
        {
            var metadataToProcess = new Dictionary<string, DataTable>();
            var fileName = file.SourceFileName.ToLower();

            var stageMetadataTablesFieldsAll =
                SetupService.GetAll<MetadataStageConfiguration>().Where(x => x.SourceID == sourceID);
            var sourceFile = SetupService.GetAll<SourceFile>()
                .FirstOrDefault(x => x.SourceID == sourceID && x.SourceFileName.Equals(file.SourceFileName));
            var stageMetadataConfig = stageMetadataTablesFieldsAll
                .Where(x => sourceFile != null && x.SourceFileID.Equals(sourceFile.SourceFileID))
                .GroupBy(y => y.TableName).Select(grp => new
                {
                    TableName = grp.Key,
                    metadataColumns = grp.ToList()
                });

            switch (fileName)
            {
                case "run1-accounts":
                    metadataToProcess = GetRun1AccountData(stageMetadataConfig, fileStream, guid);
                    break;
                case "run1-ad_units":
                    metadataToProcess = GetRun1AdUnitData(stageMetadataConfig, fileStream, guid);
                    break;
                case "run1-advertisers":
                    metadataToProcess = GetRun1AdvertiserData(stageMetadataConfig, fileStream, guid);
                    break;
                case "run1-campaigns":
                    metadataToProcess = GetRun1CampaignData(stageMetadataConfig, fileStream, guid);
                    break;
                case "run1-geo_shapes":
                    metadataToProcess = GetRun1GeoShapeData(stageMetadataConfig, fileStream, guid);
                    break;
                case "run1-placements":
                    metadataToProcess = GetRun1PlacementData(stageMetadataConfig, fileStream, guid);
                    break;
                case "run1-pmp_deals":
                    metadataToProcess = GetRun1PmpDealData(stageMetadataConfig, fileStream, guid);
                    break;
            }

            return metadataToProcess;
        }
    }
}
