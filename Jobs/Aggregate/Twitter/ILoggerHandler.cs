using Greenhouse.Data.DataSource.Twitter;
using Greenhouse.Data.Model.Core;
using Greenhouse.Utilities;
using System;
using System.Collections.Generic;

namespace Greenhouse.Jobs.Aggregate.Twitter;

public interface ILoggerHandler
{
    void LogPreExecuteInfo();
    void LogStartJobExecution();
    void LogJobComplete();
    void LogExecutionComplete();
    void LogNoReportsInQueue();
    void LogRuntimeExceeded(long elapsedMilliseconds);
    void LogOptInReports(IEnumerable<string> optInReports);
    void LogGlobalException(Exception exception);
    void LogFailedReportGenerateReport(Exception exception, IFileItem queueItem);
    void LogFailedReportGenerateReport(HttpClientProviderRequestException exception, IFileItem queueItem);
    void LogReportsNotReadyForQueue(long queueId);
    void LogReportsNotReadyToDownload(int reportsNotReadyCount, int totalReportsCount);
    void LogSkippingReportStatusCheck(Guid reportItemFileGuid, string reportItemReportName, string jobId = null);
    void LogMissingReports(List<string> missingReportIdList);
    void LogFailedReportStatusCheck(Guid reportItemFileGuid, string reportItemReportName, string reportId);
    void FailedReportStatusCheckException(Exception exception, string entityId);
    void LogSkippingReportStatusCheckAllReportsFailed();
    void LogNoReportsToRun();
    void LogCheckStatusAndDownloadReportException(Exception exception);
    void LogAccountIsNonUS(string queueItemEntityId, string reportApiReportName);
    void LogEntityDrivenReportsComplete(IFileItem queueItem);
    void LogMarkAsCompleteRecordsWithNoData();
    void LogNoActiveEntitiesForDimensionReport(IFileItem queueItem, string reportName);
    void LogNoActiveEntitiesForEntity(IFileItem queueItem, string reportName);
    void LogErrorDownloadingDimensionReport(Exception exception, IFileItem queueItem);
    void LogErrorDownloadingDimensionReport(HttpClientProviderRequestException exception, IFileItem queueItem);
    void LogSyncDownloadReportEnd(IFileItem queueItem, string cursor, string fileName,
        string currentSourceSourceName);

    void LogUnableToStageData(IFileItem queueItem);
    void LogChangingQueueStatusToComplete(IFileItem queueItem, int counter);
    void LogErrorStagingDataInS3(Exception exception, IFileItem queueItem);

    void LogErrorStagingDataInS3(HttpClientProviderRequestException exception, IFileItem queueItem);

    void LogStagingMetricsReport(IFileItem queueItem, FileCollectionItem file, int counter,
        string reportSettingsEntity);

    void LogStartDownloadReport(IFileItem queueItem, ApiReportItem reportItem, string fileName, string sourceName);
    void LogReportNoFound(string readyReportId);
    void LogErrorDownloadingReport(Exception exception, ApiReportItem reportItem);
    void LogErrorDownloadingReport(HttpClientProviderRequestException exception, ApiReportItem reportItem);


    void LogStagingDimensionReport(FileCollectionItem report, IFileItem queueItem, int counter,
        string reportSettingsEntity);

    void LogDownloadReportEnd(IFileItem queueItem, ApiReportItem reportItem, string fileName,
        string currentSourceSourceName);
}
