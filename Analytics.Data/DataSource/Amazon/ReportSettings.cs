using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Amazon
{
    [Serializable]

    public class ReportSettings
    {
        [JsonProperty("fileNameRegex")]
        public string FileNameRegex { get; set; }

        [JsonProperty("path")]
        public string FilePath { get; set; }

        [JsonProperty("dateFormat")]
        public string DateFormat { get; set; }
    }
}
