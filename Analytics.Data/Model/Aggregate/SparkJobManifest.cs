using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.Model.Aggregate
{
    public class SparkJobManifest
    {
        public SparkJobManifest()
        {
            files = new List<SparkFileBatch>();
        }

        public List<SparkFileBatch> files { get; set; }

        public void AddEntry(string s3RawPath, string s3StagePath, List<SparkFileEntry> sparkFileEntries)
        {
            files.Add(new SparkFileBatch()
            {
                s3RawPath = s3RawPath,
                s3StagePath = s3StagePath,
                fileNames = sparkFileEntries
            });
        }

        public string GetManifestBody()
        {
            var manifest = new { files };
            return JsonConvert.SerializeObject(manifest,
                new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
        }
    }

    public class SparkFileBatch
    {
        public string s3RawPath { get; set; }
        public string s3StagePath { get; set; }
        public List<SparkFileEntry> fileNames { get; set; }
    }

    public class SparkFileEntry
    {
        public string fileGUID { get; set; }
        public string sourceFileName { get; set; }
        public string fileName { get; set; }
        public string fileDelimiter { get; set; }
        public string skipRows { get; set; }
        public string hasFileHeaders { get; set; }
    }
}