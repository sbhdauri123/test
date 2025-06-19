using Dapper.Contrib.Extensions;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.Model.Setup;

[Serializable]
public class AggregateProcessingSettings
{
    [JsonProperty("hasStageFiles")]
    public bool HasStageFiles { get; set; }

    [JsonProperty("createManifestFile")]
    public bool CreateManifestFile { get; set; }
    [JsonProperty("useRawFile")]
    public bool UseRawFile { get; set; } = false;
    [JsonProperty("prefixFileGuid")]
    public bool PrefixFileGuid { get; set; }

    [JsonProperty("fileLogCheckType")]
    public string FileLogCheckType { get; set; }

    [JsonProperty("allowEmptyFiles")]
    public bool AllowEmptyFiles { get; set; }

    [JsonProperty("enforceQueueOrder")]
    public bool EnforceQueueOrder { get; set; }

    /// <summary>
    /// will skip file and not process it
    /// </summary>
    [JsonProperty("skipEmptyFiles")]
    public bool SkipEmptyFiles { get; set; }

    [JsonProperty("addFileToStagePath")]
    public bool AddFileToStagePath { get; set; }

    [JsonProperty("addSourceFileNameToFileToName")]
    public bool AddSourceFileNameToFileToName { get; set; }

    [JsonProperty("etlParameterOverrideJson")]
    public IEnumerable<EtlParameterOverride> EtlParameterOverrideSettings { get; set; }

    [JsonProperty("filesToSkipRegex")]
    public string FilesToSkipRegex { get; set; }

    [JsonProperty("continueWithErrors")]
    public bool ContinueWithErrors { get; set; } = true;

    /// <summary>
    /// creates a json file with all stage file names and sizes to be used by ETL script
    /// </summary>
    [JsonProperty("createStageFileList")]
    public bool CreateStageFileList { get; set; } = false;

    /// <summary>
    /// Once a queue is process complete, delete the queue for Filedate >= Queue.Filedate - NbDaysQueueDelete
    /// </summary>
    [JsonProperty("nbDayQueueToDelete")]
    public int? NbDaysQueueToDelete { get; set; }
    [JsonProperty("fileDelimiter")]
    public string FileDelimiter { get; set; }
    [JsonProperty("saveStagedFiles")]
    public bool SaveStagedFiles { get; set; }

    [JsonProperty("skipEntityOnError")]
    public bool SkipEntityOnError { get; set; }
    [JsonProperty("filesToParquetRegex")]
    public string FilesToParquetRegex { get; set; }
    [JsonProperty("hasQueueBundles")]
    public bool HasQueueBundles { get; set; } = false;
    [JsonProperty("skipFileEncoding")]
    public bool SkipFileEncoding { get; set; } = false;
    [JsonProperty("dimensionFilesRegex")]
    public string DimensionFilesRegex { get; set; }
    [JsonProperty("compressedfilesSettings")]
    public List<CompressedFileSetting> CompressedFilesSettings { get; set; }
    [JsonProperty("integrationProcessingRequired")]
    public bool IntegrationProcessingRequired { get; set; } = false;
    [JsonProperty("fileDateFormat")]
    public string FileDateFormat { get; set; }

    [JsonProperty("noOfConcurrentProcesses")]
    public int? NoOfConcurrentProcesses { get; set; }
    [JsonProperty("skipDuplicateQueues")]
    public bool SkipDuplicateQueues { get; set; } = false;
    [JsonProperty("useFileCollectionInEtl")]
    public bool UseFileCollectionInEtl { get; set; } = false;
}

[Serializable]
public class CompressedFileSetting
{
    [JsonProperty("regexMask")]
    private string RegexMax { get; set; }

    private RegexCodec _regexCodec;

    [Computed]
    public RegexCodec FileRegexCodec
    {
        get
        {
            if (_regexCodec == null)
            {
                _regexCodec = new RegexCodec(this.RegexMax);
            }
            return _regexCodec;
        }
    }

    [JsonProperty("linesToSkip")]
    public int LinesToSkip { get; set; }

    [JsonProperty("addRowNumber")]
    public bool AddRowNumber { get; set; }
}
