using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;

namespace Greenhouse.Jobs.Aggregate.DBM;

[Export("DV360-AggregateDataLoad", typeof(IDragoJob))]
public partial class DataLoadJob : Framework.BaseFrameworkJob, IDragoJob
{
    private static Logger _logger { get; set; } = LogManager.GetCurrentClassLogger();
    private Uri _destUri { get; set; }
    private ETLProvider _ETLProvider { get; set; }
    private IBackOffStrategy _backoff;
    private readonly Stopwatch _runtime = new Stopwatch();
    private TimeSpan _maxRuntime;

    public void PreExecute()
    {
        _ETLProvider = new ETLProvider();
        _ETLProvider.SetJobLogGUID(JED.JobGUID.ToString());
        _maxRuntime = LookupService.GetProcessingMaxRuntime(CurrentSource.SourceID);
    }

    public void Execute()
    {
        if (IsDuplicateSourceJED())
            return;

        if (!int.TryParse(SetupService.GetById<Data.Model.Setup.Lookup>(Constants.DV360_DATALOAD_POLLY_MAX_RETRY)?.Value, out int maxRetry))
            maxRetry = 6;

        _backoff = new ExponentialBackOffStrategy()
        {
            Counter = 0,
            MaxRetry = maxRetry
        };

        List<Exception> exceptions = new List<Exception>();
        //script path
        var redshiftProcessSQL = ETLProvider.GetRedshiftScripts(RootBucket, GetRedShiftScriptPath("load"));
        //for aggregate the processing is at the source level
        var queueItems = JobService.GetOrderedQueueProcessingBySource(CurrentSource.SourceID, JobLogger.JobLog.JobLogID);

        var queuedGroups = queueItems.GroupBy(x => x.EntityID).OrderBy(x => x.Min(y => y.RowNumber));
        string localDir = $"{Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseTransformPath}\\{CurrentIntegration.IntegrationID}";

        _runtime.Start();

        foreach (var entityGroup in queuedGroups)
        {
            if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1)
            {
                _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name,
                    PrefixJobGuid($"Current runtime:{_runtime.Elapsed} greater than maxRuntime:{_maxRuntime}. Stopping the Job")));
                break;
            }

            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Start processing - {CurrentIntegration.IntegrationName}; EntityID: {entityGroup.Key}")));
            foreach (Queue queueItem in entityGroup.OrderBy(e => e.RowNumber))
            {
                var dirPath = new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate) };
                try
                {
                    //clean up the stage folder at the onset of the job since new stage files are created at run time. 
                    //This would eliminate Redshift copy command errors caused by unwanted stage files that may have been remnants of previous failed processing
                    PollyAction(() =>
                    {
                        DeleteStageFiles(dirPath, queueItem.FileGUID);
                    }, "DeleteStageFilesOnset");

                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Start processing - {CurrentIntegration.IntegrationName}; FileGuid: {queueItem.FileGUID}")));

                    if (queueItem.FileCollection == null || !queueItem.FileCollection.Any())
                    {
                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{CurrentIntegration.IntegrationName}; FileGuid: {queueItem.FileGUID} - Skipping: No FileCollection Found. Resetting to Import Pending to retry downloading files")));
                        queueItem.Step = Constants.JobStep.Import.ToString();
                        queueItem.Status = Constants.JobStatus.Pending.ToString();
                        queueItem.StatusId = (int)Constants.JobStatus.Pending;
                        JobService.Update((Queue)queueItem);

                        continue;
                    }
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

                    //cleaning up local folder here and after processing data
                    if (System.IO.Directory.Exists(localDir))
                    {
                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Start Deleting local files in : {localDir}")));
                        System.IO.Directory.Delete(localDir, true);
                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"End Deleting local files in : {localDir}")));
                    }

                    var columnLists = new Dictionary<string, string>();

                    foreach (FileCollectionItem file in queueItem.FileCollection)
                    {
                        var fileFolderPath = new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate) };

                        var filePath = new List<string>(fileFolderPath);
                        filePath.Add(file.FilePath);

                        //Set source for raw files
                        var sourceUri = GetUri(filePath.ToArray(), Constants.ProcessingStage.RAW);

                        //Set destination for stage directory
                        _destUri = GetUri(filePath.ToArray(), Constants.ProcessingStage.STAGE);
                        var destFolderUri = GetUri(fileFolderPath.ToArray(), Constants.ProcessingStage.STAGE);

                        string headers = _ETLProvider.CopyRawToStageCsvGzip(sourceUri, _destUri, true, null, Constants.REPORT_TIME, localDir, queueItem.FileGUID, base.GetMultipartTransferUtility);

                        var columnListKey = file.FilePath.Split('_')[0].ToLower();

                        // if header is empty (empty file) then we skip adding it to the column-list-dictionary
                        // which will result in the parameter not being added 
                        // and ultimately being replaced with a null value in the etl script
                        if (!columnLists.ContainsKey(columnListKey) && !string.IsNullOrEmpty(headers))
                        {
                            columnLists.Add(columnListKey, headers);
                        }
                    }

                    //Pass only the dirPath since all files within that path will be processed by the etl script
                    var stageFilePath = System.Net.WebUtility.UrlDecode($"{GetUri(dirPath, Constants.ProcessingStage.STAGE).OriginalString.Trim('/')}");

                    //since we're splitting and compressing files, we're going to use wildcard for filename.
                    //and group by source-file-name to process set of reports (multiple Google reports for same report type)
                    var modifiedJSONFileCollection = Newtonsoft.Json.JsonConvert.SerializeObject(queueItem.FileCollection
                        .GroupBy(x => x.SourceFileName)
                        .Select(x => new FileCollectionItem { SourceFileName = x.Key, FilePath = x.Key, FileSize = x.Sum(s => s.FileSize) }));

                    //Add parameters key/value pair
                    var odbcParams = base.GetScriptParameters(stageFilePath, queueItem.FileGUID.ToString(), queueItem.FileDate.ToString("MM-dd-yyyy"), manifestFilePath: null, entityId: queueItem.EntityID, fileCollection: modifiedJSONFileCollection, compressionOption: Common.Constants.RedshiftCompressionType.GZIP.ToString()).ToList();

                    foreach (var keyValuePair in columnLists)
                    {
                        odbcParams.Add(new System.Data.Odbc.OdbcParameter($"columnlist-{keyValuePair.Key}", keyValuePair.Value));
                    }

                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Start executing redshift load - {string.Join("/", GetRedShiftScriptPath("load"))}")));

                    //PROCESS                
                    try
                    {
                        string sql = RedshiftRepository.PrepareCommandText(redshiftProcessSQL, odbcParams);
                        string cleanedSql = RemovePlaceHolders(sql);

                        var result = RedshiftRepository.ExecuteRedshiftCommand(cleanedSql);

                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Completed executing redshift load - {string.Join("/", GetRedShiftScriptPath("load"))}")));

                        //Update and Delete Queue
                        //Using Polly to handle SQL timeout errors
                        PollyAction(() =>
                        {
                            UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Complete, true);
                        }, "UpdateQueueWithDelete");

                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Update status to 'complete'. Deleting QueueItem:QueueID: {queueItem.ID}; FileGuid: {queueItem.FileGUID}")));
                    }
                    catch (Exception jobException)
                    {
                        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid($"Error: in redshift section- {jobException.Message}.  Skipping all other queue item(s) for this Entity: {queueItem.EntityID}")));
                        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                        exceptions.Add(jobException);
                        break;
                    }
                    finally
                    {
                        if (System.IO.Directory.Exists(localDir))
                        {
                            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Start Deleting local files in : {localDir}")));
                            System.IO.Directory.Delete(localDir, true);
                            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"End Deleting local files in : {localDir}")));
                        }
                    }
                }
                catch (HttpClientProviderRequestException exc)
                {
                    HandleQueueItemException(exceptions,
                        exc,
                        queueItem,
                        $"Error: outside of redshift section. Exception details: {exc}. Skipping all other queue item(s) for this Entity: {queueItem.EntityID}");

                    break;
                }
                catch (Exception exc)
                {
                    HandleQueueItemException(exceptions,
                        exc,
                        queueItem,
                        $"Error: outside of redshift section {exc.Message}. Skipping all other queue item(s) for this Entity: {queueItem.EntityID}");
                    break;
                }

                try
                {
                    //leave stage file deletion at tail end of job (ie after record is marked completed)
                    PollyAction(() =>
                    {
                        DeleteStageFiles(dirPath, queueItem.FileGUID);
                    }, "DeleteStageFiles");
                }
                catch (Exception exc)
                {
                    _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid($"Error: Deleting stage files {exc.Message} for Entity: {queueItem.EntityID}; Date: {queueItem.FileDate}; FileGuid: {queueItem.FileGUID}")));
                    exceptions.Add(exc);
                }
            }//Completed foreach grouping
        }//Completed foreach entity

        if (exceptions.Count > 0)
        {
            var msg = new System.Text.StringBuilder();
            msg.AppendFormat(PrefixJobGuid($"{exceptions.Count} failures in processing"));

            foreach (Exception exc in exceptions)
            {
                msg.AppendFormat(PrefixJobGuid($"{exc.Message};{exc.StackTrace}"));
                if (exc.InnerException != null)
                {
                    msg.AppendFormat(PrefixJobGuid($"{exc.InnerException.Message}, {exc.InnerException.StackTrace}"));
                }
            }

            throw new ErrorsFoundException(msg.ToString(), exceptions.First());
        }
    }

    private void HandleQueueItemException(List<Exception> exceptions, Exception exc, Queue queueItem, string message)
    {
        // Log the error with relevant details
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid(message)));

        // Update queue status to "Error"
        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);

        // Add the exception to the list
        exceptions.Add(exc);
    }
    private static string RemovePlaceHolders(string sql)
    {
        string output = RemovePlaceHoldersRegex().Replace(sql, "null");
        return output;
    }

    /// <summary>
    /// Get etl redshift script path from s3
    /// </summary>
    /// <param name="scriptType"></param>
    /// <returns></returns>
    private string[] GetRedShiftScriptPath(string scriptType)
    {
        return new string[] {
            "scripts"
            , "etl"
            , "redshift"
            , CurrentSource.SourceName.ToLower()
            , "redshift" + scriptType + CurrentSource.SourceName.ToLower() + ".sql" };
    }

    public void PollyAction(Action call, string logName)
    {
        GetPollyPolicy<Exception>("DV360-AggregateDataLoad", _backoff)
            .Execute((_) => { call(); },
                new Dictionary<string, object> { { "methodName", logName } });
    }

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

        }
    }

    ~DataLoadJob()
    {
        Dispose(false);
    }

    [GeneratedRegex(@"'\@(sourcefile|columnlist)-.*?'", RegexOptions.IgnoreCase | RegexOptions.Multiline, "en-US")]
    private static partial Regex RemovePlaceHoldersRegex();
}
