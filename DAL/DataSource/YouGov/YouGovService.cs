using Greenhouse.Data.DataSource.YouGov;
using Greenhouse.Data.DataSource.YouGov.Flat;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.DAL.DataSource.NetBase
{
    public static class YouGovService
    {
        public static void StageBrandReport(string entityId, DateTime fileDate, BrandDimResponse fullData, string fileName, Action<JArray, string, DateTime, string> writeToFile)
        {
            var flat = fullData?.Data?.Select(x => new BrandReport
            {
                id = x.Value?.ID.ToString(),
                is_active = x.Value?.IsActive.ToString(),
                label = x.Value?.Label,
                region = x.Value?.Region,
                sector_id = x.Value?.SectorId.ToString(),
                validity_period_start_date = x.Value?.ValidityPeriods?.FirstOrDefault()?.StartDate,
                validity_period_end_date = x.Value?.ValidityPeriods?.FirstOrDefault()?.EndDate
            });

            writeToFile(JArray.FromObject(flat), entityId, fileDate, fileName);
        }

        public static void StageSectorReport(string entityId, DateTime fileDate, SectorDimResponse fullData, string fileName, Action<JArray, string, DateTime, string> writeToFile)
        {
            var flat = fullData.Data.Select(x => new SectorReport
            {
                id = x.Value?.ID.ToString(),
                is_active = x.Value?.IsActive.ToString(),
                label = x.Value?.Label,
                region = x.Value?.Region,
                is_market_scanner = x.Value?.IsMarketScanner.ToString()
            });

            writeToFile(JArray.FromObject(flat), entityId, fileDate, fileName);
        }

        public static void StageFactReport(string entityId, DateTime fileDate, MetricsFactResponse fullData, string fileName, Action<JArray, string, DateTime, string> writeToFile, string region)
        {
            int volumeIndex,
                scoreIndex,
                positivesIndex,
                negativesIndex,
                neutralsIndex,
                positivesNeutralIndex,
                negativesNeutralIndex;

            var flat = fullData.Data.Queries.Select(x =>
            {
                var facts = new List<FactReport>();

                volumeIndex = x.Data.Coordinates.Perspective.IndexOf("volume");
                scoreIndex = x.Data.Coordinates.Perspective.IndexOf("score");
                positivesIndex = x.Data.Coordinates.Perspective.IndexOf("positives");
                negativesIndex = x.Data.Coordinates.Perspective.IndexOf("negatives");
                neutralsIndex = x.Data.Coordinates.Perspective.IndexOf("neutrals");
                positivesNeutralIndex = x.Data.Coordinates.Perspective.IndexOf("positives_neutrals");
                negativesNeutralIndex = x.Data.Coordinates.Perspective.IndexOf("negatives_neutrals");

                int size = x.Data.Coordinates.Metric.Count;
                for (int i = 0; i < size; i++)
                {
                    var data = x.Data.Values[i][0];// index 0 as we are retrieving 1 date at a time

                    var fact = new FactReport
                    {
                        region = region,
                        sector_id = x.Entity?.SectorID,
                        brand_id = x.Entity?.BrandID,
                        date = x.Data.Coordinates?.Date?.First(),
                        metric = x.Data.Coordinates.Metric[i],
                        volume = data[volumeIndex]?.ToString(),
                        score = data[scoreIndex]?.ToString(),
                        positives = data[positivesIndex]?.ToString(),
                        negatives = data[negativesIndex]?.ToString(),
                        neutrals = data[neutralsIndex]?.ToString(),
                        positives_neutrals = data[positivesNeutralIndex]?.ToString(),
                        negatives_neutrals = data[negativesNeutralIndex]?.ToString()
                    };

                    facts.Add(fact);
                }

                return facts;
            }).SelectMany(x => x);

            writeToFile(JArray.FromObject(flat), entityId, fileDate, fileName);
        }
    }
}
