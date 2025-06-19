using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Internal;

[Export("RedshiftMaintenancePartitionJob", typeof(IDragoJob))]
public class ReportingTablePartitionJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();
    private Action<string> _logInfo;
    private Action<string> _logWarning;
    private Action<string, Exception> _logEx;
    private RemoteAccessClient _remoteAccessClient;
    private ParallelOptions _parallelOptions;
    private int _batchCommandSize;
    private int _maxRetry;
    private readonly Stopwatch _runtime = new();
    private TimeSpan _maxRuntime;
    private int _exceptionCounter;
    private int _warningCounter;
    private const string ETL_SCRIPT_PREFIX = "partitionjob";
    private List<string> _blackList;

    public void PreExecute()
    {
        _logInfo = (msg) => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid(msg)));
        _logWarning = (msg) =>
        {
            _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, PrefixJobGuid(msg)));
            _warningCounter++;
        };
        _logEx = (msg, ex) =>
        {
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid(msg), ex));
            _exceptionCounter++;
        };

        _logInfo($"PREEXECUTE {base.DefaultJobCacheKey}");

        base.Initialize();
        _batchCommandSize = LookupService.GetLookupValueWithDefault(Constants.INTERNAL_AGG_TABLE_PARTITIONING_BATCH_COMMAND_SIZE, 50);
        _maxRetry = LookupService.GetLookupValueWithDefault(Constants.INTERNAL_AGG_TABLE_PARTITIONING_MAX_RETRY, 1);
        int maxParallelAPI = LookupService.GetLookupValueWithDefault(Constants.INTERNAL_AGG_TABLE_PARTITIONING_MAX_CONCURRENT_COMMANDS, 3);
        _parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxParallelAPI };
        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.INTERNAL_AGG_TABLE_PARTITIONING_MAX_RUNTIME, new TimeSpan(0, 3, 0, 0));
        _blackList = LookupService.GetAndDeserializeLookupValueWithDefault(Constants.INTERNAL_AGG_TABLE_PARTITIONING_BLACKLIST, new BlackListingSettings() { BlackList = new List<string>() })?.BlackList;
    }

    public void Execute()
    {
        _logInfo($"EXECUTE START {DefaultJobCacheKey}");

        _runtime.Start();

        try
        {
            CurrentIntegration = Data.Services.SetupService.GetItems<Integration>(new { SourceId = CurrentSource.SourceID }).FirstOrDefault();
            if (CurrentIntegration == null)
            {
                _logWarning("Job cannot run without an active Integration and endpoint set to an s3 location where the partition-files are saved");
                return;
            }

            _remoteAccessClient = GetRemoteAccessClient(CurrentIntegration);
            var rootPartitionDirectory = (S3Directory)_remoteAccessClient.WithDirectory();

            foreach (var sourceDirectory in rootPartitionDirectory.GetDirectories())
            {
                if (IsBlackListed(sourceDirectory.Name))
                {
                    _logInfo($"Skipping blacklisted directory: {sourceDirectory.Name}");
                    continue;
                }

                ProcessSourcePartitions((S3Directory)sourceDirectory);
            }
        }
        catch (Exception exc)
        {
            _logEx($"Global Catch on Execute-> Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
        }

        if (_exceptionCounter > 0)
        {
            throw new ErrorsFoundException($"Total errors regular: {_exceptionCounter}; Please check Splunk for more detail.");
        }
        else if (_warningCounter > 0)
        {
            JobLogger.JobLog.Status = nameof(Constants.JobLogStatus.Warning);
            JobLogger.JobLog.Message = $"Total warnings: {_warningCounter}; For full list search for Warnings in splunk";
        }

        _logInfo($"EXECUTE END {DefaultJobCacheKey}");
    }

    private void ProcessSourcePartitions(S3Directory sourcePartitionDirectory)
    {
        var directoriesWIthPartitions = sourcePartitionDirectory.GetDirectoriesRecursive(true);
        _logInfo($"{CurrentIntegration.IntegrationName} retrieved total directories with partition files:{directoriesWIthPartitions.Count()}");

        foreach (var directory in directoriesWIthPartitions)
        {
            try
            {
                if (IsMaxRuntime())
                {
                    break;
                }

                LoadPartitionsFromFiles(directory);
            }
            catch (HttpClientProviderRequestException exc)
            {
                _logEx($"Error occurred in directory:{directory.FullName} -> Exception details : {exc}", exc);
            }
            catch (Exception exc)
            {
                _logEx($"Error occurred in directory:{directory.FullName}-> Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
            }
        }
    }

    #region Execute helpers
    private void LoadPartitionsFromFiles(S3Directory directory)
    {
        var partitionDataCsvFiles = directory.GetFiles();

        if (!partitionDataCsvFiles.Any())
        {
            _logInfo($"no partition files to load from directory {directory.Name}");
        }

        foreach (var csvFile in partitionDataCsvFiles)
        {
            if (!csvFile.Name.EndsWith(".csv"))
            {
                continue;
            }

            IFile partitionSqlScriptFile = GenerateNewPartitionCommands(csvFile);

            _logInfo($"Loading partitions from file {partitionSqlScriptFile?.FullName}");

            List<(int batchNumber, List<string> batchCommand)> partitionCommandBatches = GeneratePartitionCommandBatches(partitionSqlScriptFile);

            ConcurrentQueue<Exception> exceptions = new();

            Parallel.ForEach(partitionCommandBatches, _parallelOptions, (partitionCommandBatch, state) =>
            {
                try
                {
                    StringBuilder queryText = new();

                    foreach (var partitionCommand in partitionCommandBatch.batchCommand)
                    {
                        queryText.AppendLine(partitionCommand);
                    }

                    _logInfo($"Issuing total commands:{partitionCommandBatch.batchCommand.Count} for batch number: {partitionCommandBatch.batchNumber} out of total batches:{partitionCommandBatches.Count}");

                    PollyAction(() => RedshiftRepository.ExecuteRedshiftCommand(queryText.ToString(), 300), "AddPartitionKeys");
                }
                catch (Exception ex)
                {
                    _logEx($"Error message: {ex.Message} - occurred in file: {partitionSqlScriptFile.FullName}", ex);
                    exceptions.Enqueue(ex);
                    state.Stop();
                }
            });

            if (!exceptions.IsEmpty)
            {
                _logInfo($"Errors occurred in file: {csvFile.FullName} - skipping archive step to try file again next job run.");
                continue;
            }

            ArchiveFile(directory, csvFile);
            ArchiveFile(directory, partitionSqlScriptFile);
        }
    }

    private List<(int batchNumber, List<string> batchCommand)> GeneratePartitionCommandBatches(IFile partitionSqlScriptFile)
    {
        List<(int batchNumber, List<string> batchCommand)> partitionCommandBatches = new();

        if (partitionSqlScriptFile == null)
        {
            return partitionCommandBatches;
        }

        List<string> addOrDropPartitionCommands = new();

        string line;
        using StreamReader reader = new(partitionSqlScriptFile.Get());
        while ((line = reader.ReadLine()) != null)
        {
            addOrDropPartitionCommands.Add(line);
        }

        var batchCommandSubLists = UtilsText.GetSublistFromList(addOrDropPartitionCommands, _batchCommandSize);

        int batchCounter = 0;
        foreach (var batchItem in batchCommandSubLists)
        {
            partitionCommandBatches.Add((batchCounter, batchItem.ToList()));
            batchCounter++;
        }

        return partitionCommandBatches;
    }

    private IFile GenerateNewPartitionCommands(IFile file)
    {
        var redShiftScriptPath = GetRedShiftScriptPath($"{ETL_SCRIPT_PREFIX}{CurrentSource.SourceName.ToLower()}.sql");

        var redshiftProcessSql = ETLProvider.GetRedshiftScripts(RootBucket, redShiftScriptPath);

        var odbcParams = base.GetScriptParameters($"{file.Directory.Uri.ToString().TrimEnd('/')}/{file.Name}", file.Name).ToList();

        odbcParams.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "region", Value = Greenhouse.Configuration.Settings.Current.AWS.Region });
        odbcParams.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "iamrole", Value = Greenhouse.Configuration.Settings.Current.AWS.IamRoleRedshiftCopyS3 });

        _logInfo($"Getting new partitions from file {file.FullName}");

        var queryText = RedshiftRepository.PrepareCommandText(redshiftProcessSql, odbcParams);

        PollyAction(() => RedshiftRepository.ExecuteRedshiftCommand(queryText, 0), "GetNewPartitions");

        var newSqlFileDirectory = new S3Directory(new Uri($"{file.Directory.Uri.ToString().TrimEnd('/')}/{Path.GetFileNameWithoutExtension(file.FullName)}"), GreenhouseS3Creds);

        return newSqlFileDirectory.GetFiles().FirstOrDefault();
    }

    private void ArchiveFile(S3Directory directory, IFile file)
    {
        _logInfo($"Archiving file {file.FullName}");

        string[] paths = new string[]
        {
            "partitionkeys-archive", directory.Uri.AbsolutePath, file.Name
        };

        Uri baseUri = RemoteUri.GetServiceUri(Constants.URI_SCHEME_S3, Configuration.Settings.Current.AWS.Region, RootBucket);

        S3File rawFile = new(RemoteUri.CombineUri(baseUri, paths), GreenhouseS3Creds);
        file.CopyTo(rawFile, true);
        file.Delete();
    }

    public void PollyAction(Action call, string logName)
    {
        var backoff = new ExponentialBackOffStrategy()
        {
            Counter = 0,
            MaxRetry = _maxRetry
        };

        GetPollyPolicy<Exception>("ReportingTablePartitionJob", backoff)
            .Execute((_) => call(),
                new Dictionary<string, object> { { "methodName", logName } });
    }

    private bool IsMaxRuntime()
    {
        bool isMaxRuntime = false;

        if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1)
        {
            _logWarning($"Current runtime:{_runtime.Elapsed} greater than maxRuntime:{_maxRuntime}. Stopping the Job");
            isMaxRuntime = true;
        }

        return isMaxRuntime;
    }

    private string[] GetRedShiftScriptPath(string EtlScriptName)
    {
        return new string[] {
        "scripts", "etl", "redshift"
        , CurrentSource.SourceName.ToLower()
        , EtlScriptName };
    }

    public bool IsBlackListed(string sourceDirectoryName)
    {
        if (_blackList is null || string.IsNullOrEmpty(sourceDirectoryName))
        {
            return false;
        }

        return _blackList.Any(bl => sourceDirectoryName.Contains($"datasource={bl}"));
    }
    #endregion

    public void PostExecute()
    {
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            _remoteAccessClient.Dispose();
        }
    }

    ~ReportingTablePartitionJob()
    {
        Dispose(false);
    }

    public class BlackListingSettings
    {
        [JsonProperty("blackList")]
        public List<string> BlackList { get; set; }
    }
}
