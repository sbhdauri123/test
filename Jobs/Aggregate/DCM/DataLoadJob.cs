using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json.Linq;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Data.Odbc;
using System.Diagnostics;
using System.Linq;

namespace Greenhouse.Jobs.Aggregate.DCM;

[Export("DCM-AggregateDataLoad", typeof(IDragoJob))]
public class DataLoadJob : Framework.BaseFrameworkJob, IDragoJob
{
    private static Logger _logger { get; set; } = LogManager.GetCurrentClassLogger();
    private Uri _destUri { get; set; }
    private ETLProvider _ETLProvider { get; set; }
    private IEnumerable<APIEntity> _APIEntities;
    private readonly Stopwatch _runtime = new Stopwatch();
    private TimeSpan _maxRuntime;
    private int _nbResults;
    private readonly List<string> _failedEntities = new();
    private bool _stopJob;

    public void PreExecute()
    {
        _ETLProvider = new ETLProvider();
        _ETLProvider.SetJobLogGUID(JED.JobGUID.ToString());

        var repo = new BaseRepository<APIEntity>();
        _APIEntities = repo.GetItems(new { SourceID = CurrentSource.SourceID });
        _maxRuntime = LookupService.GetProcessingMaxRuntime(CurrentSource.SourceID);
        _nbResults = LookupService.GetNbResultsForProcessing(CurrentSource.SourceID);
    }

    public void Execute()
    {
        List<Exception> exceptions = new List<Exception>();

        if (IsDuplicateSourceJED())
            return;

        //for aggregate the processing is at the source level
        var queueItems = JobService.GetOrderedQueueProcessingBySource(SourceId, JobLogger.JobLog.JobLogID, false, _nbResults);

        string localDir = $"{Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseTransformPath}\\{CurrentIntegration.IntegrationID}";

        _runtime.Start();

        foreach (Queue queueItem in queueItems.OrderBy(q => q.RowNumber))
        {
            if (_stopJob)
            {
                LogMessage(LogLevel.Info, $"Stopping the Processing Job. Error has occurred and sourceID {CurrentSource.SourceID} setting AggregateProcessingSettings.ContinueWithErrors is set to false.");
                break;
            }

            if (_failedEntities.Contains(queueItem.EntityID))
            {
                continue;
            }

            if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1)
            {
                _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name,
                    PrefixJobGuid($"Current runtime:{_runtime.Elapsed} greater than maxRuntime:{_maxRuntime}. Stopping the Job")));
                break;
            }

            try
            {
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

                //Pass only the dirPath since all files within that path will be processed by the etl script
                var dirPath = new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate) };
                var stageFilePath = System.Net.WebUtility.UrlDecode($"{GetUri(dirPath, Constants.ProcessingStage.STAGE).OriginalString.Trim('/')}");

                //cleaning s3 folder from previous processing for this queue
                DeleteStageFiles(dirPath, queueItem.FileGUID, queueItem.FileGUID.ToString());

                //script path           
                var redshiftProcessSQL = queueItem.IsDimOnly ? ETLProvider.GetRedshiftScripts(RootBucket, GetRedshiftDimensionScriptPath()) : ETLProvider.GetRedshiftScripts(RootBucket, GetRedShiftScriptPath("load"));
                List<OdbcParameter> odbcParams = new List<OdbcParameter>();

                if (queueItem.IsDimOnly)
                {
                    //Get list of manifests from fileCollectionJSON and upload them to a file in S3
                    var manifestFiles = Newtonsoft.Json.JsonConvert.DeserializeObject<IEnumerable<FileCollectionItem>>(queueItem.FileCollectionJSON);

                    var manifestPath = new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate) };
                    _destUri = GetUri(manifestPath, Constants.ProcessingStage.RAW);

                    CreateManifestFileList(queueItem, manifestFiles, manifestPath);

                    //retrieve APIEntity Name containing the parentID to pass it to the ETL script
                    string profileName = _APIEntities.First(a => a.APIEntityCode == queueItem.EntityID).APIEntityName;

                    //Add parameters key/value pair
                    odbcParams = base.GetScriptParameters(
                        stagefilepath: _destUri.ToString(),
                        fileGuid: queueItem.FileGUID.ToString(),
                        profileid: queueItem.EntityID,
                        profileName: profileName).ToList();

                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Start executing redshift load - {string.Join("/", GetRedshiftDimensionScriptPath())}")));
                }
                else
                {
                    var columnLists = new Dictionary<string, string>();

                    foreach (FileCollectionItem file in queueItem.FileCollection)
                    {
                        var filePath = new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), file.FilePath };

                        //Set source for raw files
                        var sourceUri = GetUri(filePath, Constants.ProcessingStage.RAW);

                        //Set destination for stage directory
                        _destUri = GetUri(filePath, Constants.ProcessingStage.STAGE);

                        string headers = _ETLProvider.CopyRawToStageCsvGzip(sourceUri, _destUri, true, Constants.REPORT_FIELDS_TEXT, Constants.REPORT_GRAND_TOTAL_TEXT, localDir, queueItem.FileGUID, base.GetMultipartTransferUtility);

                        var fileName = file.FilePath.Split('_')[1].ToLower();

                        if (!columnLists.ContainsKey(fileName))
                        {
                            columnLists.Add(fileName, string.IsNullOrEmpty(headers) ? null : headers);
                        }
                    }

                    //since we're splitting and compressing files, we're going to use wildcard for filename.
                    var modifiedJSONFileCollection = Newtonsoft.Json.JsonConvert.SerializeObject(queueItem.FileCollection
                        .Select(x => new FileCollectionItem
                        {
                            SourceFileName = x.SourceFileName,
                            FilePath = x.SourceFileName,
                            FileSize = x.FileSize
                        }));

                    //retrieve APIEntity Name containing the parentID to pass it to the ETL script
                    string profileName = _APIEntities.First(a => a.APIEntityCode == queueItem.EntityID).APIEntityName;

                    //Add parameters key/value pair
                    odbcParams = base.GetScriptParameters(stageFilePath, queueItem.FileGUID.ToString(), queueItem.FileDate.ToString("MM-dd-yyyy"), manifestFilePath: null, entityId: queueItem.EntityID, fileCollection: modifiedJSONFileCollection, compressionOption: Common.Constants.RedshiftCompressionType.GZIP.ToString(), profileid: queueItem.EntityID, profileName: profileName).ToList();

                    foreach (var keyValuePair in columnLists)
                    {
                        odbcParams.Add(new System.Data.Odbc.OdbcParameter($"columnlist-{keyValuePair.Key}", keyValuePair.Value));
                    }

                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Start executing redshift load - {string.Join("/", GetRedShiftScriptPath("load"))}")));
                }

                //PROCESS
                string sql = string.Empty;

                try
                {
                    sql = RedshiftRepository.PrepareCommandText(redshiftProcessSQL, odbcParams);
                    var result = RedshiftRepository.ExecuteRedshiftCommand(sql);

                    var message = queueItem.IsDimOnly ? $"Completed executing redshift load - {string.Join("/", GetRedshiftDimensionScriptPath())}" : $"Completed executing redshift load - {string.Join("/", GetRedShiftScriptPath("load"))}";
                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid(message)));

                    //Update and Delete Queue
                    UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Complete, true);

                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Updated status to 'complete'. Deleting QueueItem:QueueID: {queueItem.ID}; FileGuid: {queueItem.FileGUID}")));

                    DeleteStageFiles(dirPath, queueItem.FileGUID);

                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Completed processing - {CurrentIntegration.IntegrationName}; FileGuid: {queueItem.FileGUID}")));
                }
                catch (Exception jobException)
                {
                    _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid($"Error: in redshift section- {jobException.Message}.  Skipping all other queue item(s) for this Entity: {queueItem.EntityID} - SQL={SanitizeAWSCredentials(sql, 500)}")));
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                    exceptions.Add(jobException);
                    ContinueOrSkipEntities(queueItem);
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
            catch (HttpClientProviderRequestException exception)
            {
                HandleException(exceptions, queueItem, exception);
            }
            catch (Exception exc)
            {
                HandleException(exceptions, queueItem, exc);
            }
        }//Completed foreach grouping   

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

    private void ContinueOrSkipEntities(Queue queueItem)
    {
        if (!CurrentSource.AggregateProcessingSettings.ContinueWithErrors)
        {
            _stopJob = true;
            return;
        }

        if (CurrentSource.AggregateProcessingSettings.SkipEntityOnError
            && !string.IsNullOrEmpty(queueItem.EntityID))
        {
            _failedEntities.Add(queueItem.EntityID);
            LogMessage(LogLevel.Warn, $"Queues with EntityID='{queueItem.EntityID}' will be skipped");
        }
    }

    private void HandleException<TException>(List<Exception> exceptions, Queue queueItem, TException exception) where TException : Exception
    {
        var logMsg = BuildLogMessage(queueItem, exception);
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid(logMsg)));
        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
        exceptions.Add(exception);
        ContinueOrSkipEntities(queueItem);
    }

    private static string BuildLogMessage<TException>(Queue queueItem, TException exception) where TException : Exception
    {
        return exception switch
        {
            HttpClientProviderRequestException httpEx =>
                $"Exception details : {httpEx}.  Skipping all other queue item(s) for this Entity: {queueItem.EntityID}",
            _ =>
                $"Error: outside of redshift section {exception.Message}.  Skipping all other queue item(s) for this Entity: {queueItem.EntityID}"
        };
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
            , "redshift" + scriptType + "dcmfact.sql" };
    }

    private string[] GetRedshiftDimensionScriptPath()
    {
        return new string[] {
            "scripts"
            , "etl"
            , "redshift"
            , CurrentSource.SourceName.ToLower()
            , "redshiftloaddcmdimension.sql" };
    }

    private void CreateManifestFileList(Queue queueItem, IEnumerable<FileCollectionItem> fileList, string[] manifestPath)
    {
        //make fields lower case to match the destination table schema in Redshift
        var manifestFiles = fileList.Select(x => new { filepath = x.FilePath, filesize = x.FileSize, sourcefilename = x.SourceFileName });

        var rac = GetS3RemoteAccessClient();
        IFile transformedFile = rac.WithFile(Utilities.RemoteUri.CombineUri(_destUri, $"{CurrentSource.SourceName.ToLower()}_{queueItem.FileGUID}_{queueItem.FileDate:yyyy-MM-dd}.json"));

        DeleteRawFiles(manifestPath, $"{CurrentSource.SourceName.ToLower()}_{queueItem.FileGUID}_{queueItem.FileDate:yyyy-MM-dd}.json");

        ETLProvider.SerializeRedshiftJson(JArray.FromObject(manifestFiles), transformedFile);

        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Manifest file list has been created and is available at: {transformedFile.FullName}. ETL script will use this file to check file size.")));
    }

    private void LogMessage(LogLevel logLevel, string message)
    {
        _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(message)));
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
}
