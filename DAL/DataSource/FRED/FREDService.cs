using Greenhouse.Data.DataSource.FRED.Series;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.DAL.DataSource.FRED
{
    public static class FREDService
    {
        public static void StageObservations(string entityId, string seriesId, DateTime fileDate, List<ObservationResponse> fullData, string fileName, Action<JArray, string, DateTime, string> writeToFileSignature)
        {
            var flatData = fullData.SelectMany(d => d.Observations, (d, data) => new
            {
                series_id = seriesId,
                realtime_start = data.RealtimeStart,
                realtime_end = data.RealtimeEnd,
                date = data.Date,
                value = data.Value
            });

            writeToFileSignature(JArray.FromObject(flatData), entityId, fileDate, fileName);
        }
    }
}
