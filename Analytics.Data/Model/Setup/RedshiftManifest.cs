using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.Model.Setup
{
    public class RedshiftManifest
    {
        public RedshiftManifest()
        {
            entries = new List<ManifestEntry>();
        }

        public List<ManifestEntry> entries { get; set; }

        public void AddEntry(string s3Path, bool isMandatory = false)
        {
            entries.Add(new ManifestEntry()
            {
                Url = s3Path,
                Mandatory = isMandatory
            });
        }

        public void AddEntryWithMeta(string s3Path, long contentLength, bool isMandatory = false)
        {
            entries.Add(new ManifestEntry()
            {
                Url = s3Path,
                Mandatory = isMandatory,
                Meta = new ManifestMeta { ContentLength = contentLength }
            });
        }

        public string GetManifestBody()
        {
            var manifest = new { entries };
            return JsonConvert.SerializeObject(manifest,
                new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
        }
    }

    public class ManifestEntry
    {
        [JsonProperty("url")]
        public string Url { get; set; }
        [JsonProperty("mandatory")]
        public bool Mandatory { get; set; } = false;
        [JsonProperty("meta")]
        public ManifestMeta Meta { get; set; }
    }
    public class ManifestMeta
    {
        [JsonProperty("content_length")]
        public long ContentLength { get; set; }
    }
}