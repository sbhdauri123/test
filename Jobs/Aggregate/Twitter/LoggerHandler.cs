using Greenhouse.Data.DataSource.Twitter;
using Greenhouse.Data.Model.Core;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;

namespace Greenhouse.Jobs.Aggregate.Twitter;

public class LoggerHandler : ILoggerHandler
{
    private readonly Func<string, string> _prefixJobGuid;
    private readonly string _defaultJobCacheKey;
    private readonly string _currentSourceSourceName;
    private readonly Logger _logger;

    public LoggerHandler(Logger logger, Func<string, string> prefixJobGuid, string defaultJobCacheKey,
        string currentSourceSourceName)
    {
        _logger = logger;
        _prefixJobGuid = prefixJobGuid;
        _defaultJobCacheKey = defaultJobCacheKey;
        _currentSourceSourceName = currentSourceSourceName;
    }

    public void LogPreExecuteInfo()
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid($"{_currentSourceSourceName} - IMPORT-PREEXECUTE {_defaultJobCacheKey}")));
    }

    public void LogStartJobExecution()
    {
        _logger.Log(
            Msg.Create(LogLevel.Info, _logger.Name, _prefixJobGuid($"EXECUTE START {_defaultJobCacheKey}")));
    }

    public void LogJobComplete()
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, _prefixJobGuid("Import job complete")));
    }

    public void LogExecutionComplete()
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, _prefixJobGuid($"EXECUTE END {_defaultJobCacheKey}")));
    }

    public void LogNoReportsInQueue()
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, _prefixJobGuid("There are no reports in the Queue")));
    }

    public void LogRuntimeExceeded(long elapsedMilliseconds)
    {
        _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name,
            _prefixJobGuid(
                $"Runtime exceeded time allotted - {elapsedMilliseconds}ms")));
    }

    public void LogOptInReports(IEnumerable<string> optInReports)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid($"Opt-in reports: {string.Join(",", optInReports)}")));
    }

    public void LogGlobalException(Exception exception)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            _prefixJobGuid(
                $"Global Catch - if reached the SaveUnfinishedReport method might not have been executed -> Exception: {exception.GetType().FullName} - Message: {exception.Message} - STACK {exception.StackTrace}")
            , exception));
    }

    public void LogFailedReportGenerateReport(Exception exception, IFileItem queueItem)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            _prefixJobGuid(
                $"Error queueing daily report -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  -> Exception: {exception.Message} - STACK {exception.StackTrace}")
            , exception));
    }
    public void LogFailedReportGenerateReport(HttpClientProviderRequestException exception, IFileItem queueItem)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            _prefixJobGuid(
                $"Error queueing daily report -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} -> |Exception details : {exception}")
            , exception));
    }

    public void LogReportsNotReadyForQueue(long queueId)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            _prefixJobGuid(
                $"Reports not ready for queue ID={queueId}. Reports saved for next run. Queue reset to pending ")));
    }

    public void LogReportsNotReadyToDownload(int reportsNotReadyCount, int totalReportsCount)
    {
        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
            _prefixJobGuid(
                $"Total reports not ready: {reportsNotReadyCount}; total reports: {totalReportsCount}")));
    }

    public void LogSkippingReportStatusCheck(Guid reportItemFileGuid, string reportItemReportName,
        string jobId = null)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid(
                $"Skipping Check Report Status: {(jobId is not null ? $"JobId:{jobId}" : string.Empty)}   FileGUID: {reportItemFileGuid}->Report Name: {reportItemReportName} because associated report failed")));
    }

    public void LogMissingReports(List<string> missingReportIdList)
    {
        _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name,
            _prefixJobGuid(
                $"Missing report IDs from report-status-check response:{string.Join(",", missingReportIdList)}")));
    }

    public void LogFailedReportStatusCheck(Guid reportItemFileGuid, string reportItemReportName, string reportId)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid(
                $"Failed Check Report Status: FileGUID: {reportItemFileGuid}->Report Name: {reportItemReportName} ReportID: {reportId}")));
    }

    public void FailedReportStatusCheckException(Exception exception, string entityId)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            _prefixJobGuid(
                $"Error checking report status- failed " +
                $"for EntityID: {entityId} " +
                $"- Exception: {exception.Message} - STACK {exception.StackTrace}")
            , exception));
    }

    public void LogSkippingReportStatusCheckAllReportsFailed()
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid(
                $"Skipping Check Report Status because all associated reports in batch failed")));
    }

    public void LogNoReportsToRun()
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, _prefixJobGuid("There are no reports to run")));
    }

    public void LogCheckStatusAndDownloadReportException(Exception exception)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            _prefixJobGuid(
                $"Exception: {exception.GetType().FullName} - Message: {exception.Message} - STACK {exception.StackTrace}")
            , exception));
    }

    public void LogAccountIsNonUS(string queueItemEntityId, string reportApiReportName)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid(
                $"This account '{queueItemEntityId}' is non US - not making a call for apireport '{reportApiReportName}'")));
    }

    public void LogEntityDrivenReportsComplete(IFileItem queueItem)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid(
                $"All entity driven reports complete for Entity:{queueItem.EntityID}; FileDate:{queueItem.FileDate}; fileguid:{queueItem.FileGUID}")));
    }

    public void LogMarkAsCompleteRecordsWithNoData()
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid($"Checking for queue records with no data and marking them as complete")));
    }

    public void LogNoActiveEntitiesForDimensionReport(IFileItem queueItem, string reportName)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid(
                $"No Active Entities for Dimension Report Entity:{queueItem.EntityID}; FileDate:{queueItem.FileDate}; Report:{reportName}; fileguid:{queueItem.FileGUID}")));
    }

    public void LogNoActiveEntitiesForEntity(IFileItem queueItem, string reportName)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid(
                $"No Active Entities for Entity:{queueItem.EntityID}; FileDate:{queueItem.FileDate}; Report:{reportName}; fileguid:{queueItem.FileGUID}")));
    }

    public void LogErrorDownloadingDimensionReport(Exception exception, IFileItem queueItem)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            _prefixJobGuid(
                $"Error downloading report - failed on queueID: {queueItem.ID} for EntityID: {queueItem.EntityID} " +
                $"  - Exception: {exception.Message} - STACK {exception.StackTrace}")
            , exception));
    }
    public void LogErrorDownloadingDimensionReport(HttpClientProviderRequestException exception, IFileItem queueItem)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            _prefixJobGuid(
                $"Error downloading report - failed on queueID: {queueItem.ID} for EntityID: {queueItem.EntityID} " +
                $" |Exception details : {exception}")
            , exception));
    }
    public void LogSyncDownloadReportEnd(IFileItem queueItem, string cursor, string fileName,
        string currentSourceSourceName)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid(
                $"{currentSourceSourceName} end DownloadReport: Queue ID:{queueItem.ID} FileGUID: {queueItem.FileGUID} Cursor: {cursor} Saving to S3 as {fileName}")));
    }

    public void LogUnableToStageData(IFileItem queueItem)
    {
        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
            _prefixJobGuid(
                $"File Collection is empty; unable to stage data for FileGUID: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} ")));
    }

    public void LogChangingQueueStatusToComplete(IFileItem queueItem, int counter)
    {
        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
            _prefixJobGuid(
                $"Changing queue status to complete. Job complete for account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID} counter: {counter}")));
    }

    public void LogErrorStagingDataInS3(Exception exception, IFileItem queueItem)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            _prefixJobGuid(
                $"Error staging data in S3 -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  -> Exception: {exception.GetType().FullName} - Message: {exception.Message} - STACK {exception.StackTrace}")
            , exception));
    }

    public void LogErrorStagingDataInS3(HttpClientProviderRequestException exception, IFileItem queueItem)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            _prefixJobGuid(
                $"Error staging data in S3 -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} -> Exception details : {exception}")
            , exception));
    }

    public void LogStagingMetricsReport(IFileItem queueItem, FileCollectionItem file, int counter,
        string reportSettingsEntity)
    {
        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
            _prefixJobGuid(
                $"Staging Metrics Report for raw file: {file.FilePath}; report type {file.SourceFileName}; report entity: {reportSettingsEntity}; account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; fileGUID: {queueItem.FileGUID} counter: {counter}")));
    }

    public void LogStartDownloadReport(IFileItem queueItem, ApiReportItem reportItem, string fileName,
        string sourceName)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid(
                $"{sourceName} start DownloadReport: queueID: {queueItem.ID}->{reportItem.ReportID}->{reportItem.ReportName}->{reportItem.ReportURL}. Saving to S3 as {fileName}")));
    }

    public void LogReportNoFound(string readyReportId)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid(
                $"Report ID: {readyReportId} could not be found in the list of reports that were originally queued up")));
    }

    public void LogErrorDownloadingReport(Exception exception, ApiReportItem reportItem)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            _prefixJobGuid(
                $"Error downloading report - failed on queueID: {reportItem.QueueID} for EntityID: {reportItem.AccountID} " +
                $" ReportID: {reportItem.ReportID} FileID: {reportItem.ReportID} Report Name: {reportItem.ReportName}" +
                $"  - Exception: {exception.Message} - STACK {exception.StackTrace}")
            , exception));
    }
    public void LogErrorDownloadingReport(HttpClientProviderRequestException exception, ApiReportItem reportItem)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            _prefixJobGuid(
                $"Error downloading report - failed on queueID: {reportItem.QueueID} for EntityID: {reportItem.AccountID} " +
                $" ReportID: {reportItem.ReportID} FileID: {reportItem.ReportID} Report Name: {reportItem.ReportName}" +
                $" - Exception details : {exception}")
            , exception));
    }
    public void LogStagingDimensionReport(FileCollectionItem report, IFileItem queueItem, int counter,
        string reportSettingsEntity)
    {
        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
            _prefixJobGuid(
                $"Staging Dimension Report for raw file: {report.FilePath}; report type {report.SourceFileName}; report entity: {reportSettingsEntity}; account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; fileGUID: {queueItem.FileGUID} counter: {counter}")));
    }

    public void LogDownloadReportEnd(IFileItem queueItem, ApiReportItem reportItem, string fileName,
        string currentSourceSourceName)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            _prefixJobGuid(
                $"{currentSourceSourceName} end DownloadReport: FileGUID: {queueItem.FileGUID}->{reportItem.ReportID}->{reportItem.ReportName}->{reportItem.ReportURL}. Saving to S3 as {fileName}")));
    }
}
