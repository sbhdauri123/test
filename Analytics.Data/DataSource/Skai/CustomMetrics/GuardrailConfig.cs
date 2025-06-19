using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.DataSource.Skai.CustomMetrics
{
    public class GuardrailConfig
    {
        [JsonProperty("columnRestrictions")]
        public List<ColumnRestrictionSettings> ColumnRestrictions { get; set; }
        [JsonProperty("maxTotalColumns")]
        public int MaxTotalColumns { get; set; }

        /// <summary>
        /// Returns list of custom columns following SKAI's limits to the total fields requested
        /// </summary>
        public List<List<CustomMetricField>> GetCustomColumnsLists(List<CustomMetricField> customFields)
        {
            List<List<CustomMetricField>> finalList = new();

            if (customFields?.Count == 0)
            {
                return finalList;
            }

            //list of column-lists that include custom fields (broken out by their restrictions)
            List<List<CustomMetricField>> customFieldsLists = new();

            //list of max columns to take from each column-list
            List<int> takeFromEachList = new();

            foreach (var columnRestrictionSettings in ColumnRestrictions)
            {
                List<CustomMetricField> restrictedColumns = customFields.Where(x => x.Group == columnRestrictionSettings.ColumnGroup).ToList();
                if (restrictedColumns.Count == 0)
                    continue;
                customFieldsLists.Add(restrictedColumns);
                takeFromEachList.Add(columnRestrictionSettings.MaxColumns);
            }

            //add non-restricted columns last
            List<CustomMetricField> unrestrictedColumns = customFields.Except(customFields.Where(field => ColumnRestrictions.Select(x => x.ColumnGroup).Contains(field.Group))).ToList();
            customFieldsLists.Add(unrestrictedColumns);
            takeFromEachList.Add(MaxTotalColumns);

            //combine the custom fields and any restrictions to generate final output
            while (customFieldsLists.Exists(list => list.Count != 0))
            {
                List<CustomMetricField> combinedList = new();

                int columnsAdded = 0;
                for (int i = 0; i < customFieldsLists.Count && columnsAdded < MaxTotalColumns; i++)
                {
                    if (customFieldsLists[i].Count != 0)
                    {
                        int columnsToTake = Math.Min(takeFromEachList[i], MaxTotalColumns - columnsAdded);
                        combinedList.AddRange(customFieldsLists[i].Take(columnsToTake));
                        customFieldsLists[i] = customFieldsLists[i].Skip(columnsToTake).ToList();
                        columnsAdded += columnsToTake;
                    }
                }

                finalList.Add(combinedList);
            }

            return finalList;
        }
    }

    public class ColumnRestrictionSettings
    {
        [JsonProperty("maxColumns")]
        public int MaxColumns { get; set; }
        [JsonProperty("columnGroup")]
        public string ColumnGroup { get; set; }
    }
}
