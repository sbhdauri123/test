# Twitter Import Job

## Overview

This job handles the import of Twitter reports, processing them and storing them in S3.

## Supported Report Types

* AccountDimsReport
* CampaignDimsReport
* CampaignDMAStatsReport
* CampaignSegmentedStatsReport
* CampaignStatsReport
* LineItemDimsReport
* LineItemDMAStatsReport
* LineItemSegmentedStatsReport
* LineItemStatsReport
* MediaCreativeDimsReport
* MediaCreativeStatsReport
* PromotedTweetDimsReport
* PromotedTweetDMAStatsReport
* PromotedTweetSegmentedStatsReport
* PromotedTweetStatsReport

## Process Flow

::: mermaid
graph TB;
    PreExecute -->|Initialization| Execute -->|Queue Processing| GetActiveEntities
    --> ProcessReportByType{ProcessReportByType}
        ProcessReportByType -->|Dims| DownloadDimensionReports --> SaveReportToS3Raw[(Save Report in S3 'RAW' Folder)]
        ProcessReportByType -->|Stats| RequestReports --> CheckStatusAndDownloadReport
        --> ReportStatus{ReportStatus}
            ReportStatus --> |Status != PROCESSING | CancelReport
            ReportStatus --> |SUCCESS| DownloadReport --> SaveReportToS3Raw[(Save Report in S3 'RAW' Folder)]
            --> AreAllReportsReadyToStage{AreAllReportsReadyToStage}
            AreAllReportsReadyToStage -->|Yes| StageReport --> aveReportToS3Stage[(Save Report in S3 'STAGE' Folder)]
            AreAllReportsReadyToStage -->|No| UpdateQueueStatus --> |PENDING| SaveUnfinishedReports
:::

1. **Initialization**: The job initializes necessary components and retrieves configuration settings.
2. **Queue Processing**: Processes queue items for report generation.
3. **Get Active Entities**: Gets active entities for each report.
4. **Process Report By Type**: Orchestrates the report flow depending on report type, "dims" or "stats".
5. **Download Dimension Reports**: Synchronously downloads dimension reports into "raw" folder in S3.
6. **Requests Report Generation**: Submits report requests to the Twitter API for each report async generation.
7. **Gets Report Status**: Gets the report generation current status (SUCCESS, PROCESSING, CANCELLED).
8. **Report Download**: Downloads successful reports as .gz to the "raw" folder in S3.
9. **Stage report**: Once a report is downloaded it is ready to stage. Downloads reports as .json to the "stage" folder
   in S3.
10. **Error Handling**: Manages exceptions and retries failed operations.
11. **Cleanup**: Saves information about unfinished reports for future processing.

## API Rate Limits

[Synchronous vs. Asynchronous](https://developer.x.com/en/docs/x-ads-api/analytics/overview#:~:text=Synchronous%20vs.%20Asynchronous)

## Related Documentation

* https://developer.x.com/en/docs/x-ads-api/analytics/overview
* https://developer.x.com/en/docs/x-ads-api/analytics/guides/active-entities
* https://developer.x.com/en/docs/x-ads-api/analytics/guides/asynchronous-analytics