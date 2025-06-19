using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.Databricks;
using Greenhouse.Data.DataSource.DSP;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Aggregate.DSP
{
    [Export("GenericAggregateDSPDataLoad", typeof(IDragoJob))]
    public class DataLoadJob : Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private readonly ETLProvider _ETLProvider = new();
        private Uri _sourceURI;
        private Uri _destURI;
        private string _stageFilePath;
        private string _manifestFilePath;
        private readonly QueueRepository _queueRepository = new();
        private const string ETL_SCRIPT_PREFIX = "redshiftload";
        private const string SPARK_JOB_CLASS_NAME = "GenericAggregateDelivery";
        private const string QUEUE_BUNDLE_MANIFEST = "bundle_manifest";
        private string _filePassword;
        private IEnumerable<APIEntity> _apiEntities;
        private readonly List<long> _deletedQueueIds = new();
        private readonly Dictionary<string, Encoding> _fileEncodingCache = new();
        private readonly List<string> _failedEntities = new();
        private readonly List<int> _failedIntegrations = new();
        private Lookup _sparkJobCacheLookup;
        private SparkJobCache _sparkJobCache;
        private int _exceptionCount;
        private int _warningCount;
        private readonly Stopwatch _runtime = new();
        private TimeSpan _maxRuntime;
        private TimeSpan _dbxWaitTime;
        private int _nbResults;
        private DatabricksJobProvider _databricksJobProvider;
        private List<OrderedQueue> _queueItems;
        private readonly CancellationTokenSource _cts = new();
        private bool _stopJob;
        private DatabricksETLJob _databricksEtlJob;

        private bool IsJobRunningAtSourceLevel => CurrentSource.ETLTypeID == (int)Constants.ETLProviderType.Redshift && !CurrentSource.HasIntegrationJobsChained && !CurrentSource.AggregateProcessingSettings.IntegrationProcessingRequired;
        private bool IsDatabricksJob => CurrentSource.ETLTypeID == (int)Constants.ETLProviderType.Databricks;

        public void PreExecute()
        {
            Initialize();
            _ETLProvider.SetJobLogGUID(JED.JobGUID.ToString());
            var filePasswordLookup = SetupService.GetById<Greenhouse.Data.Model.Setup.Lookup>($"{Constants.FILEPASSWORD}{CurrentSource.SourceName}");
            _filePassword = string.IsNullOrEmpty(filePasswordLookup?.Value) ? "" : Utilities.UtilsText.Decrypt(filePasswordLookup.Value);
            _apiEntities = JobService.GetAllActiveAPIEntities(CurrentSource.SourceID);
            _sparkJobCacheLookup = SetupService.GetById<Greenhouse.Data.Model.Setup.Lookup>($"{SPARK_JOB_CLASS_NAME}_{CurrentSource.SourceID}");
            if (string.IsNullOrEmpty(_sparkJobCacheLookup?.Value))
            {
                _sparkJobCacheLookup = new Lookup { Name = $"{SPARK_JOB_CLASS_NAME}_{CurrentSource.SourceID}", IsEditable = true };
                _sparkJobCache = new SparkJobCache { FailedIdList = new List<string>() };
            }
            else
            {
                _sparkJobCache = ETLProvider.DeserializeType<SparkJobCache>(_sparkJobCacheLookup.Value);
            }

            _maxRuntime = LookupService.GetProcessingMaxRuntime(CurrentSource.SourceID);
            _dbxWaitTime = LookupService.GetLookupValueWithDefault($"{Constants.DATABRICKS_FAILED_RUN_WAIT_TIME}{CurrentSource.SourceID}", new TimeSpan(0, 6, 0, 0));
            _nbResults = LookupService.GetNbResultsForProcessing(CurrentSource.SourceID);
            if (IsDatabricksJob)
            {
                _databricksJobProvider = CreateDatabricksJobProvider();
            }
            _cts.CancelAfter(_maxRuntime);
        }

        public void Execute()
        {
            LogMessage(LogLevel.Info, $"EXECUTE START {base.DefaultJobCacheKey}");

            try
            {
                if (IsJobRunningAtSourceLevel && IsDuplicateSourceJED())
                {
                    return;
                }

                _runtime.Start();

                // matching conditional logic found in Framework.BatchDataLoadJob
                if (IsJobRunningAtSourceLevel)
                {
                    //Redshift Dataload is called for one integration, so we retrieve all the queues at the source level
                    _queueItems = JobService.GetOrderedQueueProcessingBySource(CurrentSource.SourceID, this.JobLogger.JobLog.JobLogID, false, _nbResults).ToList();
                }
                else
                {
                    // Sources that have chained integrations are loaded one integration at a time
                    _queueItems = JobService.GetOrderedQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID).ToList();
                    var previouslyFailedQueues = JobService.GetAllDatabricksFailedJobs(_queueItems).ToDictionary(q => q.QueueID, q => q.StatusUpdatedDate);

                    _queueItems = _queueItems.Where(q => !previouslyFailedQueues.ContainsKey(q.ID) || previouslyFailedQueues[q.ID].Add(_dbxWaitTime) < DateTime.Now)
                                             .GroupBy(q => q.ID)
                                             .Select(g => g.First())
                                             .ToList();
                }

                // filter queue to only active API entities (if any exist for source)
                // NOTE: Non-API sources (eg AMC-Agg) have API entities, so we have to check the table
                var sourceHasEntities = SetupService.GetItems<APIEntity>(new { CurrentSource.SourceID, IsActive = true }).Any();
                if (sourceHasEntities)
                {
                    _queueItems = _queueItems.Where(q => _apiEntities.Any(x => x.APIEntityCode == q.EntityID)).ToList();
                }

                // First try processing queues in groups
                // Stop at first sign of error
                // Any unprocessed queues to be processed normally
                if (CurrentSource.AggregateProcessingSettings.HasQueueBundles)
                {
                    List<(OrderedQueue primaryQueue, List<OrderedQueue> queuesInBundle)> queueBundles = BundleQueues(_queueItems);
                    LogMessage(LogLevel.Info, $"Source has {queueBundles.Count} total queue bundles." +
                        $"Bundled Primary QueueItems: {string.Join(",", queueBundles.Select(x => x.primaryQueue.ID))}; FileGuid: {string.Join(",", queueBundles.Select(x => x.primaryQueue.FileGUID.ToString()))}");

                    //queues are grouped by entity and isbackfill
                    List<OrderedQueue> primaryQueues = queueBundles.Select(q => q.primaryQueue).ToList();

                    ProcessQueues(primaryQueues, (q) => { ProcessQueueBundles(q, queueBundles); });
                }

                ProcessQueues(_queueItems, ProcessStageFiles);
            }
            catch (Exception ex)
            {
                LogExceptionAndIncrement(LogLevel.Error, $"Error caught in Execute. Message:{ex.Message} - STACK:{ex.StackTrace}", ex);
            }

            if (_exceptionCount > 0)
            {
                throw new ErrorsFoundException($"Total errors: {_exceptionCount}; Please check Splunk for more detail.");
            }

            if (_warningCount > 0)
            {
                JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
                JobLogger.JobLog.Message = $"Total warnings: {_warningCount}; For full list search for Warnings in splunk";
            }

            LogMessage(LogLevel.Info, $"EXECUTE END {base.DefaultJobCacheKey}");
        }

        #region execute helpers

        private void ResetRunningQueuesBackToPendingStatus()
        {
            // reset RUNNING queues back to PENDING
            var runningQueues = _queueItems.Where(x => x.Status.Equals(nameof(Constants.JobStatus.Running), StringComparison.OrdinalIgnoreCase));
            if (runningQueues.Any())
            {
                base.UpdateQueueWithDelete(runningQueues, Constants.JobStatus.Pending, false);
            }
        }

        private bool AnyMissingDate(Queue nextToProcess)
        {
            DateTime? maxDateProcessed = JobService.GetMaxFileDateProcessedComplete(nextToProcess.IntegrationID, nextToProcess.EntityID);

            if (!maxDateProcessed.HasValue)
            {
                return false;
            }

            int diff = (nextToProcess.FileDate.Date - maxDateProcessed.Value).Days;

            // if on diff > 1  instead of diff != 1 because:
            // if a queue.Filedate is skipping days compare to the latest processed complete, we don't process that queue
            // if a queue.Filedate is earlier than the latest processed complete we consider it to be a BF scheduled in BigQuery DTS
            //      if not a true BF, the processing will not overwrite newer data with older one
            if (diff > 1)
            {
                LogWarningAndIncrement("Skipping Processing Queue, missing dates for source with enforceQueueOrder=true. " +
                    $"Max Date Processed={maxDateProcessed} - Next Queue to Import={nextToProcess.FileDate} Fileguid={nextToProcess.FileGUID} EntityID={nextToProcess.EntityID}");
                return true;
            }

            return false;
        }

        private List<(OrderedQueue, List<OrderedQueue>)> BundleQueues(List<OrderedQueue> queueItems)
        {
            List<(OrderedQueue primaryQueue, List<OrderedQueue> queuesInBundle)> queueBundles = new List<(OrderedQueue, List<OrderedQueue>)>();

            // order groups by row number, ie in order to process daily groups first
            var queuedGroups = queueItems
                .GroupBy(x => new { entityId = x.EntityID, isBackfill = x.IsBackfill })
                .OrderBy(x => x.Min(y => y.RowNumber))
                .Select(group =>
                {
                    return new
                    {
                        GroupKey = group.Key,
                        OrderedItems = group.OrderBy(item => item.RowNumber)
                    };
                });

            foreach (var entityGroup in queuedGroups)
            {
                LogMessage(LogLevel.Info, $"Start bundling - {CurrentIntegration.IntegrationName}; EntityID: {entityGroup.GroupKey.entityId}; IsBackfil: {entityGroup.GroupKey.isBackfill}");

                List<long> allQueueIds = entityGroup.OrderedItems.Select(x => x.ID).Distinct().ToList();
                List<OrderedQueue> chainedQueues = queueItems.Where(x => allQueueIds.Contains(x.ID)).ToList();

                // get primary queue
                long primaryId = allQueueIds.First();
                OrderedQueue primaryQueue = queueItems.First(q => q.ID == primaryId);

                queueBundles.Add((primaryQueue, chainedQueues));
            }

            return queueBundles;
        }

        private void ProcessQueueBundles(Queue queueItem, List<(OrderedQueue primaryQueue, List<OrderedQueue> queuesInBundle)> queueBundles)
        {
            // get bundle
            var queueBundle = queueBundles.First(q => q.primaryQueue.ID == queueItem.ID);
            LogMessage(LogLevel.Info, $"Start processing queue bundle. Primary QueueItem:QueueID: {queueBundle.primaryQueue.ID}; FileGuid: {queueBundle.primaryQueue.FileGUID}| " +
                $"Bundled QueueItems - total: {queueBundle.queuesInBundle.Count}: {string.Join(",", queueBundle.queuesInBundle.Select(x => x.ID))}; FileGuid: {string.Join(",", queueBundle.queuesInBundle.Select(x => x.FileGUID.ToString()))}");

            // mark other queues in bundle to status RUNNING
            // NOTE: the primary Queue in bundle was already updated to RUNNING in method "ProcessQueues" calling this action
            var bundledQueues = queueBundle.queuesInBundle.Where(x => x.ID != queueBundle.primaryQueue.ID);
            LogMessage(LogLevel.Info, $"Update status to 'Running' for bundled QueueItems: {string.Join(",", bundledQueues.Select(x => x.ID))}; FileGuid: {string.Join(",", bundledQueues.Select(x => x.FileGUID.ToString()))}");
            UpdateQueueWithDelete(bundledQueues, Constants.JobStatus.Running);

            // create new manifest files for each file type in file collection in all bundled queues
            // destURI is set in method "GetStagedFiles" so this file-collection re-assignment needs to happen before we set the "manifest-bundle" destURI
            queueBundle.queuesInBundle.ForEach(queue => queue.FileCollectionJSON = JsonConvert.SerializeObject(GetStagedFiles(queue)));

            string[] manifestPath = new string[] { queueItem.EntityID.ToLower(), QUEUE_BUNDLE_MANIFEST, queueItem.FileGUID.ToString().ToLower() };
            _destURI = GetUri(manifestPath, Constants.ProcessingStage.STAGE, false);

            //set stageFilePath for use when creating manifest file and executing redshift etl script
            _stageFilePath = System.Net.WebUtility.UrlDecode($"{_destURI.ToString().Trim('/')}");

            //clean up the stage folder before manifest file creation
            DeleteStageFiles(manifestPath, queueItem.FileGUID);

            var allManifestFiles = GetManifestFiles(queueBundle, _destURI);

            CreateStageFileList(queueItem, allManifestFiles, null);

            ProcessFile(queueItem, queueItem.FileCollection.ToList());

            // mark other queues in bundle to status COMPLETE
            // NOTE: the primary Queue in bundle was already updated to COMPLETE in method "ProcessFile"
            LogMessage(LogLevel.Info, $"Start Update status to 'Complete'. Deleting bundled QueueItems: {string.Join(",", bundledQueues.Select(x => x.ID))}; FileGuid: {string.Join(",", bundledQueues.Select(x => x.FileGUID.ToString()))}");

            UpdateQueueWithDelete(bundledQueues, Constants.JobStatus.Complete, true);
            _deletedQueueIds.AddRange(bundledQueues.Select(q => q.ID));
            LogMessage(LogLevel.Info, $"End Update status to 'Complete'. Deleting bundled QueueItems for Primary QueueItem:QueueID: {queueBundle.primaryQueue.ID}; FileGuid: {queueBundle.primaryQueue.FileGUID}");
        }

        /// <summary>
        /// Retrieve unique file items across queues in a bundle that will be used in a manifest file
        /// </summary>
        /// <param name="queueBundle"></param>
        /// <param name="destURI"></param>
        /// <returns></returns>
        private List<FileCollectionItem> GetManifestFiles((OrderedQueue primaryQueue, List<OrderedQueue> queuesInBundle) queueBundle, Uri destURI)
        {
            List<FileCollectionItem> allManifestFiles = new List<FileCollectionItem>();

            // group queue file collections by sourcefile name
            // so there will be a manifest file for each report type
            var filesByReportType = queueBundle.queuesInBundle
                .SelectMany(queue =>
                queue.FileCollection
                .Select(f => new
                {
                    filePath = f.FilePath,
                    fileSize = f.FileSize,
                    sourceFileName = f.SourceFileName,
                    queueId = queue.ID,
                    rowNumber = queue.RowNumber,
                    isBackfillFlag = queue.IsBackfill,
                    fileDate = queue.FileDate.Date,
                    createdDate = queue.CreatedDate,
                    stageUri = GetUri(new string[] { queue.EntityID.ToLower(), GetDatedPartition(queue.FileDate), f.FilePath }, Constants.ProcessingStage.STAGE, false),
                    isDimensionFile = IsDimensionFile(f.FilePath)
                }))
                .GroupBy(f => f.sourceFileName);

            // list of manifest file entries grouped by file format (ie JSON, CSV)
            // this is to prevent Redshift COPY command errors by isolating files
            // NOTE: this is needed for Facebook processing b/c the backlog queues have stage files in JSON
            // and new queues will have stage files in CSV to leverage Redshift parallel processing
            // TODO: need to remove this file format req if not needed for other sources and Facebook queues no longer contain JSON
            List<ManifestFileEntryGroups> manifestFileEntryGroups = new List<ManifestFileEntryGroups>();

            foreach (var files in filesByReportType)
            {
                // Dimension reports can repeat across queues, so we need to de-duplicate the reports here
                // and take the first dimension report
                // TODO: need to refactor this region (or remove entirely) when we de-couple dimension report processing from current Aggregate Import Jobs
                #region de-duplicate dimension report types and add to manifest group list
                if (files.Any(x => x.isDimensionFile))
                {
                    var latestDimensionFile = files.OrderByDescending(x => x.createdDate).OrderBy(x => x.rowNumber).First();

                    var filePathExtension = Path.GetExtension(latestDimensionFile.filePath.ToLower());

                    // this is a group of one item because we don't want to process duplicate dimension data
                    var dimensionFile = new ManifestFileEntryGroups
                    {
                        FileFormat = filePathExtension,
                        FileCollectionItems = new List<FileCollectionItem> { new FileCollectionItem { FilePath = System.Net.WebUtility.UrlDecode($"{latestDimensionFile.stageUri.ToString().Trim('/')}"), FileSize = latestDimensionFile.fileSize, SourceFileName = $"{latestDimensionFile.sourceFileName}{filePathExtension.Replace('.', '_')}" } }
                    };

                    manifestFileEntryGroups.Add(dimensionFile);
                    continue;
                }
                #endregion

                // group files by file format (ie JSON, CSV)
                // then de-duplicate files by using file-date (note: this is needed to handle backfill entries)
                // TODO: may need to remove this if determined there is no need for other sources
                var filesByFormat = files.GroupBy(f => Path.GetExtension(f.filePath.ToLower())).Select(group =>
                {
                    return new ManifestFileEntryGroups
                    {
                        FileFormat = group.Key,
                        FileCollectionItems = group.GroupBy(x => x.fileDate)
                        .Select(g => g.OrderByDescending(x => x.createdDate).OrderBy(x => x.rowNumber)
                        .First())
                        .Select(x => new FileCollectionItem { FilePath = System.Net.WebUtility.UrlDecode($"{x.stageUri.ToString().Trim('/')}"), FileSize = x.fileSize, SourceFileName = $"{x.sourceFileName}{group.Key.Replace('.', '_')}" })
                    };
                });

                manifestFileEntryGroups.AddRange(filesByFormat);
            }

            // The following group-by-fileFormat is again to handle Facebook case of processing both JSON and CSV files
            // (NOTE: we did not want to re-import the backlog of facebook queues)
            // TODO: may need to remove this if determined there is no need for other sources
            var fileGroups = manifestFileEntryGroups.GroupBy(x => x.FileFormat);

            foreach (var group in fileGroups)
            {
                List<FileCollectionItem> fileCollections = group.ToList()
                    .SelectMany(g => g.FileCollectionItems.Select(x => new FileCollectionItem { FilePath = x.FilePath, FileSize = x.FileSize, SourceFileName = x.SourceFileName })).ToList();

                var manifestFiles = ETLProvider.CreateManifestFiles(queueBundle.primaryQueue, fileCollections, destURI, GetDatedPartition);

                allManifestFiles.AddRange(manifestFiles);
            }
            LogMessage(LogLevel.Info, $"Manifest files created. Creating stage filelist of manifest files for queue bundle. Primary QueueItem:QueueID: {queueBundle.primaryQueue.ID}; FileGuid: {queueBundle.primaryQueue.FileGUID}");

            return allManifestFiles;
        }

        /// <summary>
        /// Determines if file is a dimension report by checking the Regex set in CurrentSource.AggregateProcessingSettings.DimensionFilesRegex
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        private bool IsDimensionFile(string filePath)
        {
            bool isDimensionFile = false;

            if (!string.IsNullOrEmpty(CurrentSource.AggregateProcessingSettings.DimensionFilesRegex))
            {
                var dimensionRegexCodec = new RegexCodec(CurrentSource.AggregateProcessingSettings.DimensionFilesRegex);

                isDimensionFile = dimensionRegexCodec.FileNameRegex.IsMatch(filePath);
            }

            return isDimensionFile;
        }

        private void ProcessStageFiles(Queue queueItem)
        {
            var stageFiles = GetStagedFiles(queueItem);

            if (CurrentSource.AggregateProcessingSettings.CreateStageFileList)
            {
                var columnHeaderDict = GetColumnHeaders(stageFiles, _destURI, queueItem);
                CreateStageFileList(queueItem, stageFiles, columnHeaderDict);
            }
            //set stageFilePath for use when creating manifest file and executing redshift etl script
            _stageFilePath = System.Net.WebUtility.UrlDecode($"{_destURI.ToString().Trim('/')}");

            GenerateManifestFile(queueItem, stageFiles);

            ProcessFile(queueItem, stageFiles);
        }

        private void ProcessQueues(List<OrderedQueue> orderedQueues, Action<Queue> queueAction)
        {
            List<OrderedQueue> queues = orderedQueues.Where(q => !_deletedQueueIds.Contains(q.ID)).ToList();

            if (queues.Count == 0)
            {
                LogMessage(LogLevel.Debug, $"There are no queues to process|action reference:{queueAction.Method.Name}");
                return;
            }

            InitializeRunningJobsInDatabricks(queues);

            foreach (Queue queueItem in queues)
            {
                if (_stopJob)
                {
                    LogMessage(LogLevel.Info, $"Stopping the Processing Job. Error has occurred and sourceID {CurrentSource.SourceID} setting AggregateProcessingSettings.ContinueWithErrors is set to false.");
                    break;
                }

                if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1)
                {
                    LogMaxRunTimeWarning();
                    break;
                }

                try
                {
                    if (_deletedQueueIds.Contains(queueItem.ID) || _failedIntegrations.Contains(queueItem.IntegrationID) || _failedEntities.Contains(queueItem.EntityID))
                    {
                        continue;
                    }

                    if (CurrentSource.AggregateProcessingSettings.EnforceQueueOrder &&
                        AnyMissingDate(queueItem))
                    {
                        // if source at the source level: multiple integrations for an integration. If the order of an entity is incorrect we can move to another entity
                        // for FileDrop, we can stop the processing as if the next queue's order is incorrect, the following ones won't be correct either
                        if (IsJobRunningAtSourceLevel)
                        {
                            if (!string.IsNullOrEmpty(queueItem.EntityID))
                            {
                                _failedEntities.Add(queueItem.EntityID);

                            }
                            else
                            {
                                _failedIntegrations.Add(queueItem.IntegrationID);
                            }

                            continue;
                        }
                        else
                        {
                            break;
                        }
                    }
                    if (queueItem.Status == Constants.JobStatus.Error.ToString() && queueItem.LastUpdated + _dbxWaitTime > DateTime.Now)
                    {
                        LogMessage(LogLevel.Info, $"Skipping processing of queue {queueItem.ID} - Databricks job was recently submitted");
                        return;
                    }
                    queueItem.Status = nameof(Constants.JobStatus.Running);
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

                    queueAction(queueItem);
                }
                catch (Exception exc)
                {
                    if (IsMaxRunTimeReachedAndTaskCancelled(exc))
                    {
                        LogMaxRunTimeWarning();
                    }
                    else
                    {
                        LogProcessQueuesException(queueItem, exc);
                        HandleQueueErrorAndContinueOrSkipEntities(queueItem.ID);
                    }
                }
            }

            CheckRunningJobsInDatabricks();
            ResetRunningQueuesBackToPendingStatus();
        }

        private void InitializeRunningJobsInDatabricks(List<OrderedQueue> queues)
        {
            if (!IsDatabricksJob)
            {
                return;
            }


            List<long> queueIds = queues.Select(x => x.ID).ToList();
            _databricksJobProvider.InitializeRunningJobs(queueIds);
            LogMessage(LogLevel.Info, $"Retrieved total running jobs in Databricks: {_databricksJobProvider.RunningJobs.Count}");

        }

        private void CheckRunningJobsInDatabricks()
        {
            if (!IsDatabricksJob)
            {
                return;
            }

            if (_databricksJobProvider.RunningJobs.Count == 0 || _cts.IsCancellationRequested)
            {
                return;
            }

            try
            {
                LogMessage(LogLevel.Info, $"No more Databricks jobs to submit. Checking status of total running jobs: {_databricksJobProvider.RunningJobs.Count}..");
                _databricksJobProvider.WaitForMaxRunJobsToCompleteAsync(OnDatabricksJobCompletion, OnDatabricksJobException, _cts.Token, true).GetAwaiter().GetResult();
            }
            catch (OperationCanceledException)
            {
                LogException(LogLevel.Error, $"Checking status of running jobs in Databricks was canceled. Total running jobs remaining: {_databricksJobProvider.RunningJobs.Count}.");
            }
        }

        private void LogProcessQueuesException(Queue queueItem, Exception exc)
        {
            LogExceptionAndIncrement(LogLevel.Error, $"Processing error->failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  ->" +
                $" Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
        }

        private void LogMaxRunTimeWarning()
        {
            LogWarningAndIncrement($"Current runtime:{_runtime.Elapsed} greater than maxRuntime:{_maxRuntime}. Stopping the Job");
            _stopJob = true;
        }

        private bool IsMaxRunTimeReachedAndTaskCancelled(Exception ex)
        {
            return ex is OperationCanceledException && TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1;
        }

        /// <summary>
        /// Convert text files into Parquet when Redshift cannot load text file directly
        /// Example: Redshift cannot escape leading apostrophe
        /// </summary>
        /// <param name="queueItem"></param>
        private List<FileCollectionItem> StageParquetFiles(Queue queueItem)
        {
            var convertedParquetFiles = new List<FileCollectionItem>();

            var files = new List<FileCollectionItem>();

            if (string.IsNullOrEmpty(queueItem.FileCollectionJSON))
            {
                files.Add(new FileCollectionItem() { FilePath = queueItem.FileName, FileSize = queueItem.FileSize, SourceFileName = queueItem.SourceFileName });
            }
            else
            {
                files.AddRange(queueItem.FileCollection);
            }

            // based on regex in source setting, get only the files that need to be converted to parquet
            // if any, then create manifest file
            var sparkFileEntries = new List<SparkFileEntry>();

            foreach (var file in files)
            {
                var regexCodec = new RegexCodec(CurrentSource.AggregateProcessingSettings.FilesToParquetRegex);
                if (regexCodec.FileNameRegex.IsMatch(file.FilePath))
                {
                    var currentSourceFile = SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(file.FilePath));

                    var sparkFileEntry = new SparkFileEntry()
                    {
                        sourceFileName = currentSourceFile.SourceFileName,
                        fileGUID = queueItem.FileGUID.ToString(),
                        fileName = file.FilePath,
                        fileDelimiter = currentSourceFile.FileDelimiter,
                        skipRows = currentSourceFile.RowsToSkip.ToString(),
                        hasFileHeaders = currentSourceFile.HasHeader.ToString()
                    };

                    sparkFileEntries.Add(sparkFileEntry);

                    // add to list this method returns
                    // we will need to skip these files in downstream transformations
                    var parquetFileCollectionItem = new FileCollectionItem
                    {
                        SourceFileName = file.SourceFileName,
                        FileSize = file.FileSize,
                        FilePath = $"{file.SourceFileName}/gold/part"
                    };
                    convertedParquetFiles.Add(parquetFileCollectionItem);
                }
            }

            // submit databricks job
            var sparkJobManifest = new SparkJobManifest();
            var dirPath = new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate) };

            // delete stage files if any from previous run
            DeleteStageFiles(dirPath, queueItem.FileGUID);

            var rawUriPath = GetUri(dirPath, Constants.ProcessingStage.RAW);
            var rawS3Path = rawUriPath.ToString();

            var stageUriPath = GetUri(dirPath, Constants.ProcessingStage.STAGE);
            var stageS3Path = stageUriPath.ToString();

            sparkJobManifest.AddEntry(rawS3Path, stageS3Path, sparkFileEntries);

            var filesJSON = sparkJobManifest.GetManifestBody();
            var manifestFileURI = StageSparkJobManifest(filesJSON);

            // EX: ["s3","test-datalake-databricks","stage/<sourceName>/GenericAggregateDelivery/manifest.json","<jobGuid>"]
            var jobParams = new string[]
            {
                    S3Protocol
                    , this.RootBucket
                    , manifestFileURI
                    , JED.JobGUID.ToString()
            };

            LogMessage(LogLevel.Info, $"{queueItem.FileGUID} - Submitting spark job for integration: {CurrentIntegration.IntegrationID};" +
                $" source: {queueItem.SourceFileName}; with parameters {JsonConvert.SerializeObject(jobParams)}");

            var etlJobRepo = new DatabricksETLJobRepository();
            var etlJob = etlJobRepo.GetEtlJobByDataSourceID(CurrentSource.DataSourceID);

            if (etlJob == null)
            {
                throw new DatabricksETLJobNotFoundException("No DatabricksETLJob found for DataSourceID=" + CurrentSource.DataSourceID);
            }

            var job = Task.Run(async () => await base.SubmitSparkJobDatabricks(etlJob.DatabricksJobID, queueItem, false, false, true, jobParams));
            job.Wait();

            var jsonResult = JsonConvert.SerializeObject(job.Result);
            //If job failed, then throw exception               
            if (job.Result != ResultState.SUCCESS)
            {
                if (string.IsNullOrEmpty(queueItem.EntityID))
                {
                    _sparkJobCache.IsIntegrationId = true;
                    _sparkJobCache.FailedIdList.Add(queueItem.IntegrationID.ToString());
                }
                else
                {
                    _sparkJobCache.IsIntegrationId = false;
                    _sparkJobCache.FailedIdList.Add(queueItem.EntityID);
                }
                _sparkJobCache.FailedIdList = _sparkJobCache.FailedIdList.Distinct().ToList();
                _sparkJobCacheLookup.Value = JsonConvert.SerializeObject(_sparkJobCache);
                _sparkJobCacheLookup.LastUpdated = DateTime.Now;
                _sparkJobCacheLookup.IsEditable = true;
                var repo = new LookupRepository();
                LookupRepository.AddOrUpdateLookup(_sparkJobCacheLookup);
                string errMessage = PrefixJobGuid($"ERROR->Spark job for queue id: {queueItem.ID} returned job status: {job.Result}");
                throw new DatabricksResultNotSuccessfulException(errMessage);
            }
            else
            {
                LogMessage(LogLevel.Info, $"SUCCESS->Spark job for integration: {CurrentIntegration.IntegrationID};queue id: {queueItem.ID}; source: {queueItem.SourceFileName}; Summary: {job.Result}");
            }

            return convertedParquetFiles;
        }

        /// <summary>
        /// Create manifest file for Spark Job processing to create parquet files
        /// </summary>
        /// <param name="filesJSON"></param>
        /// <returns></returns>
        private string StageSparkJobManifest(string filesJSON)
        {
            var RAC = GetS3RemoteAccessClient();

            Stage = Constants.ProcessingStage.STAGE;
            var paths = new string[] { SPARK_JOB_CLASS_NAME, "manifest.json" };
            var fileName = Utilities.RemoteUri.CombineUri(GetDestinationFolder(), paths);
            var rawFile = RAC.WithFile(fileName);

            if (rawFile.Exists)
                rawFile.Delete();

            var byteData = System.Text.Encoding.UTF8.GetBytes(filesJSON);
            using (MemoryStream stream = new MemoryStream(byteData))
            {
                rawFile.Put(stream);
            }

            Stage = Constants.ProcessingStage.RAW;
            return
                $"{Constants.ProcessingStage.STAGE.ToString().ToLower()}/{CurrentSource.SourceName.ToLower()}/{SPARK_JOB_CLASS_NAME}/manifest.json";
        }

        private List<FileCollectionItem> GetStagedFiles(Queue queueItem)
        {
            //Pass only the path of the files since all files within that path will be processed by the etl script
            var dirPath = new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate) };

            //The setting "HasStageFiles" indicates that there are files in s3 "stage" folder that were placed there by the import job
            //If this setting is TRUE, then we get the files from the "stage" folder in s3
            //If FALSE, then we get the files from the "raw" folder in s3
            var stageFiles = CurrentSource.AggregateProcessingSettings.HasStageFiles ? GetS3StageFiles(queueItem, dirPath) : StageFiles(queueItem, dirPath);

            if (stageFiles.Count == 0)
            {
                throw new NullOrEmptyFileCollectionException(
                    PrefixJobGuid(
                        $"Integration: {CurrentIntegration.IntegrationName}; FileGuid: {queueItem.FileGUID}; No stage files Found."));
            }

            return stageFiles;
        }

        private List<FileCollectionItem> StageFiles(Queue queueItem, string[] dirPath)
        {
            var fileCollectionItems = new List<FileCollectionItem>();

            //standardize the collection of files by creating a list of one or many based on queue record
            if (string.IsNullOrEmpty(queueItem.FileCollectionJSON))
            {
                fileCollectionItems.Add(new FileCollectionItem() { FilePath = queueItem.FileName, FileSize = queueItem.FileSize, SourceFileName = queueItem.SourceFileName });
            }
            else
            {
                fileCollectionItems.AddRange(queueItem.FileCollection);
            }

            if (CurrentSource.AggregateProcessingSettings.UseRawFile)
            {
                _destURI = GetUri(dirPath, Constants.ProcessingStage.RAW, false);
                return fileCollectionItems;
            }
            //before we stage files, we check if any of the raw files are empty
            var emptyFiles = fileCollectionItems.Where(f => f.FileSize < 1).Select(x => x.FilePath).ToList();

            if (emptyFiles.Count != 0 && !CurrentSource.AggregateProcessingSettings.AllowEmptyFiles)
            {
                var emptyFileNames = emptyFiles.Aggregate((current, next) => current + "|" + next);

                throw new NullOrEmptyFileCollectionException(
                    PrefixJobGuid(
                        $"Integration: {CurrentIntegration.IntegrationName}; FileGuid: {queueItem.FileGUID}; Skipping empty file(s): [{emptyFileNames}]"));
            }

            // convert files to Parquet
            // necessary when Redshift cannot directly copy file due to limitation
            var stagedParquetFiles = new List<FileCollectionItem>();

            if (!string.IsNullOrEmpty(CurrentSource.AggregateProcessingSettings.FilesToParquetRegex))
            {
                var currentId = _sparkJobCache.IsIntegrationId ? queueItem.IntegrationID.ToString() : queueItem.EntityID;

                if (_sparkJobCache.FailedIdList.Contains(currentId))
                {
                    LogWarningAndIncrement($"{queueItem.FileGUID}-{SPARK_JOB_CLASS_NAME}-skipping job submission for previously " +
                            $"failed ID {currentId}-Investigate error in Databricks and resubmit spark job by removing ID from lookup {_sparkJobCacheLookup.Name}");
                    return new List<FileCollectionItem>();
                }

                stagedParquetFiles = StageParquetFiles(queueItem);
            }

            // files converted to parquet are staged in s3 already
            // keep only the files that were not converted to be staged here
            var stageParquetNames = stagedParquetFiles.Select(y => y.SourceFileName);
            var files = fileCollectionItems.Where(x => !stageParquetNames.Contains(x.SourceFileName)).ToList();

            //get file extensions from lookup that we need to unzip/uncompress
            var extensionsToDecompress = ETLProvider.GetApprovedCompressionTypes();

            _destURI = GetUri(dirPath, Constants.ProcessingStage.STAGE, false);

            //keep record of file(s) to gz
            var filesToCompress = new List<FileCollectionItem>();

            //archive files in a format not supported by redshift. will be decompress and then compress to gzip
            var filesToDecompress = new List<FileCollectionItem>();

            //Redshift expects UTF-8 files, so we keep track of files that are non-UTF8 to convert them
            var filesNonUTF8 = new List<FileCollectionItem>();

            //files that dont need any extraction, compression or re-encoding 
            var rawFiles = new List<FileCollectionItem>();

            foreach (var f in files)
            {
                //Set files path
                var filePath = new string[]
                {
                queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate),
                f.FilePath
                };

                _sourceURI = GetUri(filePath, Constants.ProcessingStage.RAW, false);

                if (extensionsToDecompress.Any(c => f.FilePath.EndsWith(c)))
                {
                    filesToDecompress.Add(f);
                    LogMessage(LogLevel.Info, $"File {f.FilePath} added to filesToDecompress");
                    continue;
                }

                //check encoding of raw file except for manifest files that import job has created
                var fileExtension = Path.GetExtension(f.FilePath);
                if (fileExtension == ".manifest")
                {
                    rawFiles.Add(f);
                    LogMessage(LogLevel.Debug, $"File {f.FilePath} added to rawFiles");

                    continue;
                }

                var rac = new RemoteAccessClient(_sourceURI, GreenhouseS3Creds);
                var rawFile = rac.WithFile(_sourceURI);
                var compressionType = UtilsIO.GetCompressionType(fileExtension);
                //return the encoding or the file or the file in a archive
                var currentEncoding = GetEncoding(compressionType, rawFile);

                bool hasUtf8Bom = false;
                if (currentEncoding.BodyName == Encoding.UTF8.BodyName)
                {
                    hasUtf8Bom = HasUtf8Bom(compressionType, rawFile);
                }

                if (currentEncoding.BodyName != Encoding.UTF8.BodyName ||
                    (currentEncoding.BodyName == Encoding.UTF8.BodyName && hasUtf8Bom))
                {
                    // files are either not utf8 or they are utf8 but with a BOM header and in both cases
                    // need to be converted

                    if (f.FilePath.EndsWith(".gz"))
                    {
                        // file in the gz is non utf8 (or is utf8 but with BOM): unzip the files, encode them and gzipped
                        LogMessage(LogLevel.Debug, $"File {f.FilePath} added to filesToDecompress");
                        filesToDecompress.Add(f);
                    }
                    else
                    {
                        // file is not utf8 (or is UTF8 but with BOM), it will be converted and gzipped
                        filesNonUTF8.Add(f);
                        LogMessage(LogLevel.Debug, $"File {f.FilePath} added to filesNonUT8");
                    }

                    continue;
                }
                //file is utf8 with no bom
                else if (f.FilePath.EndsWith(".gz"))
                {
                    //this file is gz and its content is utf8 with no bom, no action to take, it can stay in the raw folder
                    rawFiles.Add(f);
                    LogMessage(LogLevel.Debug, $"File {f.FilePath} added to rawFiles");

                    continue;
                }

                if (ShouldBeCompressed(f.SourceFileName))
                {
                    filesToCompress.Add(f);
                    LogMessage(LogLevel.Debug, $"File {f.FilePath} added to filesToCompress");
                }
                else
                {
                    rawFiles.Add(f);
                    LogMessage(LogLevel.Debug, $"File {f.FilePath} added to rawFiles");
                }
            }

            var stagedFiles = new List<FileCollectionItem>();

            if (stagedParquetFiles.Count != 0)
                stagedFiles.AddRange(stagedParquetFiles);

            if (filesToCompress.Count == 0 && filesToDecompress.Count == 0 && filesNonUTF8.Count == 0)
            {
                LogMessage(LogLevel.Debug, "All files are in S3 raw folder - using raw for processing");

                // if all files are UTF8, no file needed to be gzipped and no file has been decompressed, we can use Raw as destURI
                _destURI = GetUri(dirPath, Constants.ProcessingStage.RAW, false);
                stagedFiles.AddRange(rawFiles);
            }
            else
            {
                LogMessage(LogLevel.Debug, "some files need to be transformed - using stage for processing");

                // note that we give a parameters all the lists of files that require an action (decompress, compress or encode)
                // those transformed files will be stored in S3 stage folder
                // we also pass the rawFiles as they need to be moved to S3 stage folder (all files to be processed need to be in a same s3 folder)
                var transformed = TransformFiles(queueItem, dirPath, filesToCompress, filesToDecompress, filesNonUTF8, rawFiles);
                stagedFiles.AddRange(transformed);
            }

            return stagedFiles;
        }

        private static bool ShouldBeCompressed(string fileName)
        {
            string extension = Path.GetExtension(fileName);
            return UtilsIO.GetCompressionType(extension) != Constants.CompressionType.GZIP;
        }

        private List<FileCollectionItem> TransformFiles(Queue queueItem, string[] dirPath, List<FileCollectionItem> filesToCompress, List<FileCollectionItem> filesToExtract, List<FileCollectionItem> filesNonUTF8, List<FileCollectionItem> rawFiles)
        {
            var outputFiles = new List<FileCollectionItem>();
            var localDestUri = GetUri(dirPath, Constants.ProcessingStage.RAW, true);

            var localDirectory = new FileSystemDirectory(localDestUri);
            if (localDirectory.Exists)
            {
                //clean up any files here and after conversion
                localDirectory.Delete(true);
            }

            localDirectory.Create();

            try
            {
                foreach (var file in filesToExtract)
                {
                    LogMessage(LogLevel.Debug, $"File {file.FilePath} to be decompressed and compressed to gzip; FileGuid: {queueItem.FileGUID}");

                    var filePath = new string[]
                    {
                    queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), file.FilePath
                    };

                    _sourceURI = GetUri(filePath, Constants.ProcessingStage.RAW, false);

                    //Unzip and Stage
                    var extractedFiles = _ETLProvider.UnzipAndStageFiles(_sourceURI, _destURI.AbsoluteUri,
                        queueItem.FileGUID.ToString(), localDestUri, CurrentSource.AggregateProcessingSettings, file.SourceFileName, _filePassword);
                    outputFiles.AddRange(extractedFiles);
                }

                foreach (var file in filesToCompress)
                {
                    // file is already UTF8 so we just stage the file in s3
                    // it will be streamed from raw to stage
                    // add fileguid if not already present to raw filename so it is clear which stage files to remove
                    // file should be gzip compressed
                    GetRawFileAndEncoding(queueItem, file, out var rawFile, out var currentEncoding);

                    LogMessage(LogLevel.Debug, $"File {file.FilePath} to be compressed; FileGuid: {queueItem.FileGUID}");

                    var stageFileCollectionItem = new FileCollectionItem() { FilePath = rawFile.Name, FileSize = rawFile.Length, SourceFileName = file.SourceFileName };

                    var localFileUri = RemoteUri.CombineUri(localDestUri, file.FilePath);

                    var compressionType = UtilsIO.GetCompressionType(Path.GetExtension(rawFile.Name));
                    var gzipFilePath = UtilsIO.ChangeEncodingAndCompress(localFileUri.AbsolutePath, currentEncoding, new UTF8Encoding(false), rawFile.Get(), compressionType, Constants.CompressionType.GZIP);

                    var rac = new RemoteAccessClient(_destURI, GreenhouseS3Creds);
                    var gzipFile = rac.WithFile(new Uri($"{gzipFilePath}"));
                    var fileName = $"{queueItem.FileGUID.ToString().ToLower()}_{rawFile.Name}.gz";

                    var destRemoteFile = rac.WithFile(new Uri($"{_destURI.AbsoluteUri}/{fileName}"));
                    gzipFile.CopyTo(destRemoteFile, true);
                    stageFileCollectionItem.FilePath = destRemoteFile.Name;

                    outputFiles.Add(stageFileCollectionItem);
                }

                foreach (var file in filesNonUTF8)
                {
                    GetRawFileAndEncoding(queueItem, file, out var rawFile, out var currentEncoding);

                    var stageFileCollectionItem = new FileCollectionItem() { FilePath = rawFile.Name, FileSize = rawFile.Length, SourceFileName = file.SourceFileName };

                    LogMessage(LogLevel.Debug, $"file {file.FilePath} encoding is {currentEncoding} and will be converted to {System.Text.Encoding.UTF8.EncodingName}; FileGuid: {queueItem.FileGUID}");
                    //check if file needs compression
                    var compressFile = false;
                    var compressionType = UtilsIO.GetCompressionType(Path.GetExtension(rawFile.Name));
                    if (compressionType != Constants.CompressionType.GZIP)
                    {
                        compressFile = true;
                    }

                    //ConvertToUTF8 method will download raw file to dataload server, convert the local file to UTF8 and put new converted file in s3 stage folder
                    var utf8File = _ETLProvider.ConvertToUTF8(rawFile, localDestUri, _destURI, currentEncoding, queueItem.FileGUID.ToString(), compressFile);
                    stageFileCollectionItem.FilePath = utf8File.Name;
                    stageFileCollectionItem.FileSize = utf8File.Length;

                    outputFiles.Add(stageFileCollectionItem);
                }

                foreach (var file in rawFiles)
                {
                    //file stays in s3 raw
                    LogMessage(LogLevel.Debug, $"Raw file {file.FilePath} to be copied from raw to stage; FileGuid: {queueItem.FileGUID}");

                    GetRawFileAndEncoding(queueItem, file, out var rawFile, out var currentEncoding);

                    var stageFileCollectionItem = new FileCollectionItem() { FilePath = rawFile.Name, FileSize = rawFile.Length, SourceFileName = file.SourceFileName };
                    var rac = new RemoteAccessClient(_destURI, GreenhouseS3Creds);

                    string fileName = $"{queueItem.FileGUID.ToString().ToLower()}_{rawFile.Name}";

                    var fileExtension = Path.GetExtension(file.FilePath);
                    if (fileExtension == ".manifest")
                    {
                        fileName = rawFile.Name;
                    }

                    var destRemoteFile = rac.WithFile(new Uri($"{_destURI.AbsoluteUri}/{fileName}"));

                    rawFile.CopyTo(destRemoteFile, true);
                    stageFileCollectionItem.FilePath = destRemoteFile.Name;

                    outputFiles.Add(stageFileCollectionItem);
                }
            }
            finally
            {
                localDirectory.Delete(true);
            }

            LogMessage(LogLevel.Info, $"Deleting files from local import directory; fileGUID: {queueItem.FileGUID}. Path {localDirectory.Uri.AbsolutePath}.");

            return outputFiles;
        }

        private void GetRawFileAndEncoding(Queue queueItem, FileCollectionItem file, out IFile rawFile,
            out Encoding currentEncoding)
        {
            var filePath = new string[]
            {
            queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate),
            file.FilePath
            };
            var fileExtension = Path.GetExtension(file.FilePath);
            _sourceURI = GetUri(filePath, Constants.ProcessingStage.RAW, false);
            var sourceRac = new RemoteAccessClient(_sourceURI, GreenhouseS3Creds);
            rawFile = sourceRac.WithFile(_sourceURI);
            var sourceCompressionType = UtilsIO.GetCompressionType(fileExtension);
            currentEncoding = GetEncoding(sourceCompressionType, rawFile);
        }

        private List<FileCollectionItem> GetS3StageFiles(Queue queueItem, string[] dirPath)
        {
            var s3StagedFiles = new List<FileCollectionItem>();

            var rac = GetS3RemoteAccessClient();
            _sourceURI = GetUri(dirPath, Constants.ProcessingStage.RAW, true);
            _destURI = GetUri(dirPath, Constants.ProcessingStage.STAGE);
            var dir = rac.WithDirectory(_destURI);

            if (!dir.Exists) return s3StagedFiles;

            LogMessage(LogLevel.Debug, $"Staged files are available at: {_destURI}");

            var s3Files = dir.GetFiles().Where(f => f.Name.Contains(queueItem.FileGUID.ToString()));

            //check encoding and convert non UTF8 files
            foreach (var s3File in s3Files)
            {
                var currentSourceFile = SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(s3File.Name));
                var sourceFileName = currentSourceFile != null ? currentSourceFile.SourceFileName : queueItem.SourceFileName;

                var stageFile = new FileCollectionItem() { FilePath = s3File.Name, SourceFileName = sourceFileName, FileSize = s3File.Length };

                // sources that transform and stage data may not need to check the encoding for their stage files
                // we allow for them to be skipped in order to save processing time
                if (CurrentSource.AggregateProcessingSettings.SkipFileEncoding)
                {
                    s3StagedFiles.Add(stageFile);
                    continue;
                }

                //check encoding of stage file
                var compressionType = UtilsIO.GetCompressionType(Path.GetExtension(s3File.Name));
                var currentEncoding = GetEncoding(compressionType, s3File);

                if (currentEncoding.BodyName != System.Text.Encoding.UTF8.BodyName)
                {
                    LogMessage(LogLevel.Debug, $"Current file encoding is {currentEncoding.EncodingName} and will be converted to {System.Text.Encoding.UTF8.EncodingName}; FileGuid: {queueItem.FileGUID}");
                    //download stage file, convert to UTF8 and put back into s3 stage folder

                    var utf8File = _ETLProvider.ConvertToUTF8(s3File, _sourceURI, _destURI, currentEncoding, queueItem.FileGUID.ToString());
                    stageFile.FilePath = utf8File.Name;
                    stageFile.FileSize = utf8File.Length;
                }

                s3StagedFiles.Add(stageFile);
            }

            return s3StagedFiles;
        }

        private Uri GetUri(string[] dirPath, Constants.ProcessingStage stage, bool isTransformDir)
        {
            Stage = stage;
            return isTransformDir ?
                RemoteUri.CombineUri(GetLocalTransformDestinationFolder(), dirPath) :
                RemoteUri.CombineUri(GetDestinationFolder(), dirPath);
        }

        private void ProcessFile(Queue queueItem, List<FileCollectionItem> stageFiles)
        {
            LogMessage(LogLevel.Info, $"Start processing - {CurrentIntegration.IntegrationName}; FileGuid: {queueItem.FileGUID}");

            //IIF all file(s) are empty, then skip processing. Checking source to see if it's allowing empty file here is overkill, but doesn't hurt to check once more. 
            if (stageFiles.All(x => x.FileSize < 1) && CurrentSource.AggregateProcessingSettings.SkipEmptyFiles)
            {
                LogMessage(LogLevel.Info, $"Skipping processing of empty file(s). QueueItem:QueueID: {queueItem.ID}; FileGuid: {queueItem.FileGUID}");
            }
            else if (IsDatabricksJob)
            {
                RunDatabricksWorkflow(queueItem, stageFiles);
                return;
            }
            else
            {
                LogMessage(LogLevel.Info, $"Calling GetAndProcessEtlScript - QueueItem:QueueID: {queueItem.ID}; FileGuid: {queueItem.FileGUID}");
                GetAndProcessEtlScript(queueItem, stageFiles);
            }

            // DIAT-11577 updating the queue before deleting the files
            // if the job is interrupted after deleting the files and before the queue is deleted
            // reprocessing the queue will throw a "no files in stage" exception
            MarkQueueAsComplete(queueItem);

            //if NbDaysQueueDelete has value, a number of queues can be deleted
            if (CurrentSource.AggregateProcessingSettings.NbDaysQueueToDelete.HasValue)
            {
                var startDate = queueItem.FileDate.AddDays(-CurrentSource.AggregateProcessingSettings.NbDaysQueueToDelete.Value);
                var endDate = queueItem.FileDate.AddDays(-1);
                LogMessage(LogLevel.Info, $"Source.AggregateProcessingSettings.NbDaysQueueToDelete = {CurrentSource.AggregateProcessingSettings.NbDaysQueueToDelete.Value} - Deleting queues with same SourceID EntityID with Filedate between {startDate} and {endDate}");

                var deletedIds = DeleteQueuesByFileDate(startDate, endDate, queueItem.SourceID, queueItem.JobLogID, queueItem.EntityID);
                _deletedQueueIds.AddRange(deletedIds);
                LogMessage(LogLevel.Info, "Queue IDs deleted=" + string.Join(",", _deletedQueueIds));
            }

            if (!CurrentSource.AggregateProcessingSettings.SaveStagedFiles)
            {
                //Delete stage files by fileguid
                var dirPath = new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate) };
                DeleteStageFiles(dirPath, queueItem.FileGUID, queueItem.FileGUID.ToString());
                //Delete any manifest files created
                if (!string.IsNullOrEmpty(_manifestFilePath))
                {
                    var manifestDirPath = new string[]
                        {"manifest", queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate)};
                    DeleteStageFiles(manifestDirPath, queueItem.FileGUID, queueItem.FileGUID.ToString());
                }
            }

            LogMessage(LogLevel.Info, $"Completed processing - {CurrentIntegration.IntegrationName}; FileGuid: {queueItem.FileGUID}");
        }

        private void RunDatabricksWorkflow(Queue queueItem, List<FileCollectionItem> stageFiles)
        {
            if (HandleDuplicateQueuesAndDetermineSkip(queueItem))
            {
                return;
            }

            var apiEntityName = _apiEntities.FirstOrDefault(a => a.APIEntityCode == queueItem.EntityID)?.APIEntityName;
            var stageFilesJson = CurrentSource.AggregateProcessingSettings.UseFileCollectionInEtl ? JsonConvert.SerializeObject(stageFiles) : string.Empty;

            JobRunRequest jobRequest = new()
            {
                JobID = Convert.ToInt64(_databricksEtlJob.DatabricksJobID),
                JobParameters = _databricksJobProvider.CreateStandardizedJobParameters(
                    new DatabricksJobParameterOptions()
                    {
                        StageFilePath = _stageFilePath,
                        FileGuid = queueItem.FileGUID.ToString(),
                        FileDate = queueItem.FileDate.ToString(CurrentSource.AggregateProcessingSettings.FileDateFormat ?? "MM-dd-yyyy"),
                        EntityID = queueItem.EntityID,
                        Profileid = queueItem.EntityID,
                        IsDimOnly = queueItem.IsDimOnly,
                        ManifestFilePath = _manifestFilePath,
                        EntityName = apiEntityName,
                        NoOfConcurrentProcesses = CurrentSource.AggregateProcessingSettings.NoOfConcurrentProcesses.GetValueOrDefault(1),
                        FileCollectionJson = stageFilesJson,
                        SourceId = queueItem.SourceID
                    })
            };

            _databricksJobProvider.QueueJobAsync(queueItem.ID, jobRequest, OnDatabricksJobException, _cts.Token).GetAwaiter().GetResult();
            _databricksJobProvider.WaitForMaxRunJobsToCompleteAsync(OnDatabricksJobCompletion, OnDatabricksJobException, _cts.Token, CurrentSource.AggregateProcessingSettings.EnforceQueueOrder).GetAwaiter().GetResult();
        }

        private bool HandleDuplicateQueuesAndDetermineSkip(Queue queueItem)
        {
            if (!CurrentSource.AggregateProcessingSettings.SkipDuplicateQueues)
            {
                return false;
            }

            var queuesWithDuplicateKeys = queueItem.IsDimOnly
                ? _queueItems.Where(x => x.IsDimOnly && x.ID != queueItem.ID)
                : _queueItems.Where(x => !x.IsDimOnly && x.FileDate == queueItem.FileDate && x.ID != queueItem.ID);

            if (!queuesWithDuplicateKeys.Any())
            {
                return false;
            }

            if (ShouldSkipQueueForRecentData(queueItem, queuesWithDuplicateKeys))
            {
                WaitForDatabricksJobsToComplete(new List<long>() { queueItem.ID });
                MarkQueueAsComplete(queueItem);
                return true;
            }

            WaitForDatabricksJobsToComplete(queuesWithDuplicateKeys.Select(q => q.ID));

            return false;
        }

        private void WaitForDatabricksJobsToComplete(IEnumerable<long> queueIdList)
        {
            var runningJobs = _databricksJobProvider.RunningJobs.Where(x => queueIdList.Contains(x.QueueID)).ToList();

            if (runningJobs.Count == 0)
            {
                return;
            }

            LogMessage(LogLevel.Info, $"Wait for following queues to complete before submitting new job request: {string.Join(',', runningJobs.Select(q => q.QueueID))}");

            foreach (var job in runningJobs)
            {
                _databricksJobProvider.WaitForJobToCompleteAsync(job.QueueID, OnDatabricksJobCompletion, (queueID) => OnDatabricksJobException(queueID, job.JobRunID), _cts.Token).GetAwaiter().GetResult();
            }
        }

        private bool ShouldSkipQueueForRecentData(Queue queueItem, IEnumerable<OrderedQueue> queuesWithSameFileDate)
        {
            bool skipQueue = false;
            if (!queuesWithSameFileDate.Any())
            {
                return false;
            }

            var latestDuplicateDeliveryDate = queuesWithSameFileDate.Max(x => x.DeliveryFileDate);
            if (latestDuplicateDeliveryDate.HasValue)
            {
                if (queueItem?.DeliveryFileDate?.CompareWithoutMilliseconds(latestDuplicateDeliveryDate.Value) < 0)
                {
                    LogMessage(LogLevel.Info, $"Skipping processing of current queue ({queueItem.FileGUID}) - there is another queue with more up-to-date files - queue delivery file date: {queueItem.DeliveryFileDate} - latest duplicate deliveryFileDate:{latestDuplicateDeliveryDate.Value}");
                    skipQueue = true;
                }
                else if (queueItem?.DeliveryFileDate?.CompareWithoutMilliseconds(latestDuplicateDeliveryDate.Value) == 0
                    && queuesWithSameFileDate.Any(x => x.Status == nameof(Constants.JobStatus.Complete)))
                {
                    LogMessage(LogLevel.Info, $"Skipping processing of current queue ({queueItem.FileGUID}) - there is another queue marked COMPLETE with same file date: {queueItem.FileDate} and delivery file date: {queueItem.DeliveryFileDate} - latest duplicate deliveryFileDate:{latestDuplicateDeliveryDate.Value}");
                    skipQueue = true;
                }
            }

            return skipQueue;
        }

        private void MarkQueueAsComplete(Queue queueItem)
        {
            JobService.RemoveDatabricksFailedJobQueue(queueItem.ID);
            LogMessage(LogLevel.Info, $"Start Update status to 'Complete'. Deleting QueueItem:QueueID: {queueItem.ID}; FileGuid: {queueItem.FileGUID}");
            UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Complete, true);
            queueItem.Status = nameof(Constants.JobStatus.Complete);
            _deletedQueueIds.Add(queueItem.ID);
            LogMessage(LogLevel.Info, $"End Update status to 'Complete'. Deleting QueueItem:QueueID: {queueItem.ID}; FileGuid: {queueItem.FileGUID}");
        }

        private IEnumerable<long> DeleteQueuesByFileDate(DateTime startDate, DateTime endDate, int sourceID, long jobLogId, string APIEntityCode)
        {
            var queues = _queueRepository.GetQueueItemsByFileDate(startDate, endDate, sourceID, APIEntityCode);
            var ids = queues.Select(q => q.ID);

            // TODO: create SP that updates queue properties and delete them in 1 call
            foreach (var queue in queues)
            {
                queue.JobLogID = jobLogId;
                queue.Step = Constants.ExecutionType.Processing.ToString();
                JobService.Update(queue);
            }

            UpdateQueueWithDelete(queues, Constants.JobStatus.Complete, true);

            return ids;
        }

        private void GetAndProcessEtlScript(Queue queueItem, List<FileCollectionItem> stageFiles)
        {
            //default is to use the source name in the etl script name
            var loadEtlScriptName = $"{ETL_SCRIPT_PREFIX}{CurrentSource.SourceName.ToLower()}.sql";

            //if override enabled, then use the source file name in the queue record instead of the source name
            if (CurrentSource.OverrideEtlScriptName)
            {
                loadEtlScriptName = $"{ETL_SCRIPT_PREFIX}{queueItem.SourceFileName.ToLower()}.sql";
            }

            //if we have values in the lookup for ETL Script Version, then it will take precedent
            var etlScriptVersionCollection = GetEtlScriptVersion(CurrentSource.SourceName);

            //Only occurs when multiple etl scripts are setup in [lookup]
            if (etlScriptVersionCollection.Any())
            {
                if (!string.IsNullOrEmpty(queueItem.FileCollectionJSON))
                {
                    //End processing
                    throw new LookupException(
                        PrefixJobGuid(
                            $"Integration: {CurrentIntegration.IntegrationName}; FileGuid: {queueItem.FileGUID}; " +
                            "Requirement: Configure only one etl script in the [lookup] table when processing multiple per [Queue] record."));
                }

                //Get [load etl script] version name based on header count
                var headerColCount = _ETLProvider.GetHeaderColCount(_sourceURI);
                loadEtlScriptName = _ETLProvider.GetEtlScriptName(CurrentSource.SourceName, headerColCount, etlScriptVersionCollection);
            }

            ProcessEtlScript(loadEtlScriptName, queueItem, stageFiles);
        }

        public static IEnumerable<Lookup> GetEtlScriptVersion(string sourceName)
        {
            var key = $"{Constants.ETL_SCRIPT_VERSION}_{sourceName.ToUpper()}";

            var value = JobService.GetById<Greenhouse.Data.Model.Setup.Lookup>(key);

            if (value == null) return new List<Lookup>();

            var etlScriptNames = JsonConvert.DeserializeObject<List<Lookup>>(value.Value);

            return etlScriptNames;
        }

        private void ProcessEtlScript(string loadEtlScriptName, Queue queueItem, List<FileCollectionItem> stageFiles)
        {
            //Get [load etl script] path
            var redShiftScriptPath = GetRedShiftScriptPath(loadEtlScriptName);

            //Get [load etl script]
            var redshiftProcessSql = ETLProvider.GetRedshiftScripts(RootBucket, redShiftScriptPath);

            //get a file from the stageFiles collection to check header
            var stagedFileName = stageFiles.First().FilePath;

            if (CurrentSource.AggregateProcessingSettings.AddFileToStagePath)
            {
                _stageFilePath = $"{_stageFilePath}/{stagedFileName}";
            }

            var stageFilesJson = JsonConvert.SerializeObject(stageFiles);

            var compressionOption = ETLProvider.GetCompressionOption(queueItem, stageFilesJson);

            var headerLine = compressionOption == Constants.CompressionType.GZIP.ToString() ? ETLProvider.GetHeaderLineFromGzip(_destURI, stagedFileName, CurrentSource.AggregateProcessingSettings.FileDelimiter) : ETLProvider.GetHeaderLineFromFile(_destURI, stagedFileName, CurrentSource.AggregateProcessingSettings.FileDelimiter);

            var parameterOverrideDictionary = GetParameterOverrideDictionary(queueItem);

            var apiEntityName = _apiEntities.FirstOrDefault(a => a.APIEntityCode == queueItem.EntityID)?.APIEntityName;

            var odbcParams = base.GetScriptParameters(_stageFilePath, queueItem.FileGUID.ToString(), queueItem.FileDate.ToString("yyyy-MM-dd"), _manifestFilePath, queueItem.EntityID, stageFilesJson, compressionOption, null, parameterOverrideDictionary, apiEntityName: apiEntityName).ToList();

            LogMessage(LogLevel.Info, $"Start executing redshift load - script:{loadEtlScriptName}; stagefilepath:{_stageFilePath}; manifestFilePath:{_manifestFilePath}");

            if (!string.IsNullOrEmpty(headerLine))
            {
                odbcParams.Add(new System.Data.Odbc.OdbcParameter("columnlist", headerLine));
            }

            odbcParams.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "region", Value = Greenhouse.Configuration.Settings.Current.AWS.Region });
            odbcParams.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "iamrole", Value = Greenhouse.Configuration.Settings.Current.AWS.IamRoleRedshiftCopyS3 });

            //PROCESS load
            string sql = string.Empty;

            try
            {
                sql = RedshiftRepository.PrepareCommandText(redshiftProcessSql, odbcParams);
                var result = RedshiftRepository.ExecuteRedshiftCommand(sql);
            }
            catch
            {
                LogMessage(LogLevel.Error, $"Redshift Error - SQL={SanitizeAWSCredentials(sql, 500)}");
                throw;
            }

            LogMessage(LogLevel.Info, $"Completed executing redshift load - script:{loadEtlScriptName}; stagefilepath:{_stageFilePath}; manifestFilePath:{_manifestFilePath}");
        }

        private Dictionary<string, string> GetParameterOverrideDictionary(Queue queueItem)
        {
            var parameterOverrideDictionary = new Dictionary<string, string>();

            var overrideParameters = CurrentSource.AggregateProcessingSettings.EtlParameterOverrideSettings;

            if (overrideParameters == null) return parameterOverrideDictionary;

            //add override parameters by Integration
            var parameterOverrideDictionaryByIntegration = GetOverrideParametersByType(overrideParameters, Constants.EtlParameterOverrideType.integration.ToString(), queueItem.IntegrationID.ToString());

            //add override parameters by Entity
            var parameterOverrideDictionaryByEntity = GetOverrideParametersByType(overrideParameters, Constants.EtlParameterOverrideType.entity.ToString(), queueItem.EntityID);

            var combinedOverrideDictionary = parameterOverrideDictionaryByIntegration
                .Union(parameterOverrideDictionaryByEntity).ToDictionary(d => d.Key, d => d.Value);

            return combinedOverrideDictionary;
        }

        private Dictionary<string, string> GetOverrideParametersByType(IEnumerable<EtlParameterOverride> overrideParameters,
            string parameterOverrideType, string parameterOverrideTypeIdString)
        {
            var parameterOverrideDictionary = new Dictionary<string, string>();

            var overrideDictionary =
                GetEtlParameterOverrideDictionary(parameterOverrideType, parameterOverrideTypeIdString);

            if (overrideDictionary.Count != 0)
            {
                foreach (var keyValuePair in overrideDictionary)
                {
                    if (parameterOverrideDictionary.Any(x => x.Key.Contains(keyValuePair.Key))) continue;
                    parameterOverrideDictionary.Add(keyValuePair.Key, keyValuePair.Value);
                }
            }
            else
            {
                var defaultParameters = overrideParameters.Where(x =>
                    x.OverrideType == parameterOverrideType);
                if (!defaultParameters.Any()) return parameterOverrideDictionary;

                foreach (var parameter in defaultParameters)
                {
                    if (parameterOverrideDictionary.Any(x => x.Key.Contains(parameter.ParameterName))) continue;
                    parameterOverrideDictionary.Add(parameter.ParameterName, parameter.ReplacementValue);
                }
            }

            return parameterOverrideDictionary;
        }

        /// <summary>
        /// Get etl redshift script path from s3
        /// </summary>
        /// <param name="scriptType"></param>
        /// <returns></returns>
        private string[] GetRedShiftScriptPath(string EtlScriptName)
        {
            return new string[] {
            "scripts", "etl", "redshift"
            , CurrentSource.SourceName.ToLower()
            , EtlScriptName };
        }

        private string[] GetManifestFilePath(Queue queueItem)
        {
            string[] manifestPath = new string[]
            {
            "manifest", queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate)
            };

            var manifestUri = GetUri(manifestPath, Constants.ProcessingStage.STAGE, false);
            return new string[]
            {
            manifestUri.AbsolutePath, $"{queueItem.FileGUID}.manifest"
            };
        }

        private void GenerateManifestFile(Queue queueItem, List<FileCollectionItem> extractedFiles)
        {
            if (!CurrentSource.AggregateProcessingSettings.CreateManifestFile) return;
            LogMessage(LogLevel.Info, "Start creating manifest file");
            var manifest = new RedshiftManifest();

            foreach (var file in extractedFiles)
            {
                var s3File = $"{_stageFilePath}/{file.FilePath}";
                manifest.AddEntry(s3File, true);
            }

            var manifestPath = GetManifestFilePath(queueItem);
            _manifestFilePath = ETLProvider.GenerateManifestFile(manifest, this.RootBucket, manifestPath);

            LogMessage(LogLevel.Info, $"Successfully created manifest file at: {_manifestFilePath}");
        }

        public Dictionary<string, string> GetEtlParameterOverrideDictionary(string parameterOverrideType,
            string parameterOverrideTypeIdString)
        {
            var parameterOverrideDictionary = new Dictionary<string, string>();

            var lookupName = $"{Constants.ETL_PARAMETER_OVERRIDE_KEY_PREFIX}{CurrentSource.SourceID}";
            var etlParametersJson = SetupService.GetById<Greenhouse.Data.Model.Setup.Lookup>(lookupName);

            if (etlParametersJson == null)
            {
                return new Dictionary<string, string>();
            }

            var etlOverrideParameters = JsonConvert.DeserializeObject<IEnumerable<EtlParameterOverride>>(etlParametersJson.Value);

            var etlParameters = etlOverrideParameters.Where(x =>
                x.OverrideType == parameterOverrideType && x.OverrideTypeId == parameterOverrideTypeIdString);
            if (!etlParameters.Any()) return parameterOverrideDictionary;

            foreach (var parameter in etlParameters)
            {
                if (parameterOverrideDictionary.Any(x => x.Key.Contains(parameter.ParameterName))) continue;
                parameterOverrideDictionary.Add(parameter.ParameterName, parameter.ReplacementValue);
            }

            return parameterOverrideDictionary;
        }

        /// <summary>
        /// Creates a json file in s3 (stage) that has a list of all stage files and their sizes to be used by ETL script.
        /// </summary>
        /// <param name="queueItem"></param>
        /// <param name="fileList"></param>
        /// <param name="columnHeaderDict"></param>
        private void CreateStageFileList(Queue queueItem, List<FileCollectionItem> fileList, Dictionary<string, string> columnHeaderDict = null)
        {
            //make fields lower case to match the destination table schema in Redshift
            var stageFiles = fileList.Select(x => new { filepath = x.FilePath, filesize = x.FileSize, sourcefilename = x.SourceFileName, header = columnHeaderDict != null ? columnHeaderDict[x.FilePath] : string.Empty });

            var rac = GetS3RemoteAccessClient();
            IFile transformedFile = rac.WithFile(Utilities.RemoteUri.CombineUri(_destURI, $"{CurrentSource.SourceName.ToLower()}_{queueItem.FileGUID}_{queueItem.FileDate:yyyy-MM-dd}.json"));

            ETLProvider.SerializeRedshiftJson(JArray.FromObject(stageFiles), transformedFile);

            LogMessage(LogLevel.Debug, $"Stage file list has been created and is available at: {transformedFile.FullName}. ETL script will use this file to check file size.");
        }

        private Encoding GetEncoding(Constants.CompressionType format, IFile file)
        {
            if (_fileEncodingCache.TryGetValue(file.FullName, out Encoding value))
            {
                return value;
            }

            Encoding result = Encoding.Default;

            using (var stream = file.Get())
            {
                result = UtilsIO.GetEncoding(format, stream);
            }

            _fileEncodingCache.Add(file.FullName, result);
            return result;
        }

        private static bool HasUtf8Bom(Constants.CompressionType format, IFile file)
        {
            var result = false;

            using (var stream = file.Get())
            {
                result = UtilsIO.HasUtf8Bom(format, stream);
            }

            return result;
        }

        /// <summary>
        /// Get the column headers for each staged file
        /// </summary>
        /// <param name="fileList"></param>
        /// <param name="destURI"></param>
        /// <param name="queueItem"></param>
        /// <returns>
        /// Dictionary with file name as key and column-headers as value
        /// </returns>
        private Dictionary<string, string> GetColumnHeaders(List<FileCollectionItem> fileList, Uri destURI, Queue queueItem)
        {
            // key = fileName; value = column-headers
            var columnHeaderDict = new Dictionary<string, string>();

            foreach (var file in fileList)
            {
                var stageFileJson = JsonConvert.SerializeObject(new List<FileCollectionItem>() { file });
                var compressionOption = ETLProvider.GetCompressionOption(queueItem, stageFileJson);

                var headerLine = compressionOption == Constants.CompressionType.GZIP.ToString()
                    ? ETLProvider.GetHeaderLineFromGzip(destURI, file.FilePath, CurrentSource.AggregateProcessingSettings.FileDelimiter)
                    : ETLProvider.GetHeaderLineFromFile(destURI, file.FilePath, CurrentSource.AggregateProcessingSettings.FileDelimiter);

                columnHeaderDict.Add(file.FilePath, headerLine);
            }

            return columnHeaderDict;
        }

        /// <summary>
        /// Used in conjunction with Queue Bundling and manifest file creation
        /// TODO: may need to remove this if determined there is no need for other sources to process multiple file formats (eg JSON, CSV) for a single report type
        /// </summary>
        public class ManifestFileEntryGroups
        {
            public string FileFormat { get; set; }
            public IEnumerable<FileCollectionItem> FileCollectionItems { get; set; }
        }

        private void HandleQueueErrorAndContinueOrSkipEntities(long queueID)
        {
            var queueItem = _queueItems.Find(q => q.ID == queueID);
            queueItem.Status = nameof(Constants.JobStatus.Error);
            JobService.UpdateQueueStatus(queueID, Constants.JobStatus.Error, false);
            LogExceptionAndIncrement(LogLevel.Error, $"{queueItem.FileGUID}-Marking queue as Error");

            if (!CurrentSource.AggregateProcessingSettings.ContinueWithErrors)
            {
                _stopJob = true;
                return;
            }

            if (CurrentSource.AggregateProcessingSettings.SkipEntityOnError
                && !string.IsNullOrEmpty(queueItem.EntityID))
            {
                _failedEntities.Add(queueItem.EntityID);
                LogWarningAndIncrement($"Queues with EntityID='{queueItem.EntityID}' will be skipped");
            }
        }

        private void OnDatabricksJobException(long queueID, long jobRunId)
        {
            var queueItem = _queueItems.Find(q => q.ID == queueID);
            LogMessage(LogLevel.Error, $"Databricks job failed. FileGUID={queueItem.FileGUID};");
            HandleQueueErrorAndContinueOrSkipEntities(queueID);
            JobService.InsertDatabricksFailedJobQueue(queueID, jobRunId, ResultState.FAILED.ToString(), Convert.ToInt64(jobRunId));
        }

        private void OnDatabricksJobCompletion(DatabricksJobResult jobResult)
        {
            var queueItem = _queueItems.Find(q => q.ID == jobResult.QueueID);
            if (jobResult.JobStatus == ResultState.SUCCESS)
            {
                JobService.RemoveDatabricksFailedJobQueue(jobResult.QueueID);
                MarkQueueAsComplete(queueItem);
                LogMessage(LogLevel.Info, $"Databricks job completed successfully. FileGUID={queueItem.FileGUID}; QueueID={jobResult.QueueID}; JobRunID={jobResult.JobRunID}; JobStatus={jobResult.JobStatus}");
            }
            else if (jobResult.JobStatus == ResultState.WAITING || jobResult.JobStatus == ResultState.QUEUED)
            {
                queueItem.Status = nameof(Constants.JobStatus.Pending);
                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Pending);
                LogMessage(LogLevel.Info, $"Update queue status back to 'Pending' as job is not yet completed. FileGUID={queueItem.FileGUID}; JobRunID={jobResult.JobRunID}; JobStatus={jobResult.JobStatus}");
            }
            else
            {
                HandleQueueErrorAndContinueOrSkipEntities(jobResult.QueueID);
                LogMessage(LogLevel.Error, $"Databricks job failed. FileGUID={queueItem.FileGUID}; QueueID={jobResult.QueueID}; JobRunID={jobResult.JobRunID}; JobStatus={jobResult.JobStatus}");
            }
        }

        #endregion

        #region pre execute helpers

        private DatabricksJobProvider CreateDatabricksJobProvider()
        {
            DatabricksETLJobRepository etlJobRepo = new();
            _databricksEtlJob = etlJobRepo.GetEtlJobBySourceID(CurrentSource.SourceID) ?? throw new DatabricksETLJobNotFoundException("No DatabricksETLJob found for SourceID=" + CurrentSource.SourceID);
            string encryptedConnectionString = LookupService.GetLookupValueWithDefault(Constants.DATABRICKS_API_CREDS) ?? throw new LookupException($"Lookup value for {Greenhouse.Common.Constants.DATABRICKS_API_CREDS} is not defined");
            Credential databricksCredential = new(encryptedConnectionString);
            int pageSize = LookupService.GetLookupValueWithDefault(Constants.DATABRICKS_API_PAGESIZE, 25);

            return new DatabricksJobProvider(
                new DatabricksJobProviderOptions
                {
                    IntegrationID = CurrentIntegration.IntegrationID,
                    JobLogID = this.JobLogger.JobLog.JobLogID,
                    MaxConcurrentJobs = LookupService.GetGlobalLookupValueWithDefault(Constants.DSP_DATALOAD_MAX_CONCURRENT_JOBS, CurrentIntegration.IntegrationID, 5),
                    RetryDelayInSeconds = LookupService.GetLookupValueWithDefault(Constants.DSP_DATALOAD_STATUS_CHECK_DELAY_SECONDS, 30),
                    DatabricksJobID = _databricksEtlJob.DatabricksJobID,
                    Logger = LogMessage,
                    ExceptionLogger = LogException,
                    JobRequestRetryMaxAttempts = LookupService.GetGlobalLookupValueWithDefault(Constants.DATABRICKS_API_JOB_REQUESTS_BACKOFF_MAX_RETRY, CurrentSource.SourceID, 3),
                    JobRequestRetryDelayInSeconds = LookupService.GetGlobalLookupValueWithDefault(Constants.DATABRICKS_API_JOB_REQUESTS_BACKOFF_DELAY_SECONDS, CurrentSource.SourceID, 1),
                    JobRequestRetryUseJitter = LookupService.GetGlobalLookupValueWithDefault(Constants.DATABRICKS_API_JOB_REQUESTS_BACKOFF_USE_JITTER, CurrentSource.SourceID, true),
                    JobStatusCheckRetryMaxAttempts = LookupService.GetGlobalLookupValueWithDefault(Constants.DATABRICKS_API_JOB_STATUS_BACKOFF_MAX_RETRY, CurrentSource.SourceID, 3),
                    JobStatusCheckRetryDelayInSeconds = LookupService.GetGlobalLookupValueWithDefault(Constants.DATABRICKS_API_JOB_STATUS_BACKOFF_DELAY_SECONDS, CurrentSource.SourceID, 1),
                    JobStatusCheckRetryUseJitter = LookupService.GetGlobalLookupValueWithDefault(Constants.DATABRICKS_API_JOB_STATUS_BACKOFF_USE_JITTER, CurrentSource.SourceID, true)
                },
                new DatabricksCalls(databricksCredential, pageSize, HttpClientProvider),
                new DatabricksJobLogRepository()
                );
        }

        #endregion

        #region Logs

        private void LogMessage(LogLevel logLevel, string message)
        {
            _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(message)));
        }

        private void LogWarningAndIncrement(string message)
        {
            LogMessage(LogLevel.Warn, message);
            _warningCount++;
        }

        private void LogExceptionAndIncrement(LogLevel logLevel, string message, Exception exc = null)
        {
            LogException(logLevel, message, exc);
            _exceptionCount++;
        }

        private void LogException(LogLevel logLevel, string message, Exception exc = null)
        {
            _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(message), exc));
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
                _cts.Dispose();
            }
        }

        ~DataLoadJob()
        {
            Dispose(false);
        }
    }
}
