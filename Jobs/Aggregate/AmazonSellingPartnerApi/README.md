# AmazonSellingPartnerApi Import Job

## Overview
This job handles the import of AmazonSellingPartnerApi reports, processing them and storing them in S3.

## Prerequisites

- OAuth credentials
  - Client Id
  - Client Secret
  - Refresh Token
  - Endpoint Uri
- Valid access token
- AWS S3 bucket for storing reports

## Supported Report Types
The job supports various report types, including:

- BrandAnalyticsMarketBasketReport
- BrandAnalyticsSearchTermsReport
- BrandAnalyticsRepeatPurchaseReport
- VendorRealTimeInventoryReport
- VendorRealTimeSalesReport
- VendorRealTimeTrafficReport
- VendorSalesReport
- VendorNetPureProductMarginReport
- VendorTrafficReport
- VendorForecastingReport
- VendorInventoryReport
- CouponPerformanceReport
- PromotionPerformanceReport

## Process Flow

::: mermaid
graph TB;
    A[PreExecute] -->|Initialization| B[Execute]
    B -->|Queue Processing| C[RequestGenerateReport]
    C -->|ReportId Creation| D[GetReportStatusAndDocumentId]
    D -->|Check Processing Status| E{ProcessingStatus}
    E -->|DONE| F{Has DocumentId?}
    F -->|Yes| G[DownloadReport]
    G --> H[(Save Report in S3)]
    F -->|No| I[SaveUnfinishedReports]
    E -->|FATAL| J[HandleFailedReport]
    E -->|IN_PROGRESS| K[Save as Unfinished Report]
    E -->|IN_QUEUE| K[Save as Unfinished Report]
    K --> L[Wait for Next Queue Check]
    L --> D[GetReportStatusAndDocumentId]
    H --> I[SaveUnfinishedReports]
    J --> I[SaveUnfinishedReports]
:::

1. **Initialization**: The job initializes necessary components and retrieves configuration settings.
2. **Queue Processing**: Processes queue items for report generation.
3. **Report Generation**: Submits report requests to the Amazon SeelingPartner Api to create the reportIds.
4. **Status Checking**: Polls the API to check if reports are ready for download.
5. **Processing Status**: Evaluates the processing status of the report.
   5.1 **DONE**: Checks if the document ID is available    .
          Yes: If the document ID exists, the report is downloaded and saved in S3.
          No: If the document ID does not exist, the report is saved as unfinished for future processing.
   5.2 **FATAL**: Handles errors that prevent successful report generation  .
   5.3 **IN_PROGRESS**: The report is still being processed, so it is marked as unfinished.
   5.4 **IN_QUEUE**: The report is still in the queue, so it is saved as unfinished and waits for the next queue check.
6. **Wait for Next Queue Check**: The system waits before checking the queue again for the report’s
7. **Error Handling**: Manages exceptions and retries failed operations.
8. **Cleanup**: Saves information about unfinished reports for future processing.

## Key Components

- `ImportJob`: Main class orchestrating the import process.
- `Reports`: Handles interaction with the Amazon SellingPartner API.
- `RemoteAccessClient`: Manages S3 operations.
- `MultiplicativeBackOffStrategy`: Implements retry logic for API calls.

## Error Handling and Logging

The job implements comprehensive error handling and logging:

* Retries failed API calls up to a configurable number of attempts.
* Logs detailed information about each step of the process.
* Tracks unfinished reports for processing in subsequent runs.

## Performance Considerations

* Implements a maximum runtime to prevent excessively long job execution.
* Uses parallel processing for handling multiple report types.
* Implements caching mechanisms for API tokens.


## Add New Report Type

To add support for additional report types:

* Update the ReportTypes enum.
* Implement the corresponding case in the RequestGenerateReport method.
* Add any necessary data processing logic.

## Troubleshooting

Common issues and their solutions:

* **API Authentication Failures**: Ensure OAuth credentials are correct and not expired. 
Reach out [Maxwell Adamsky](mailto:maxwell.adamsky@publicismedia.com) for requesting a new refresh token.
* **Report Download Timeouts**: Check the MaxRuntime setting and consider increasing it.
* **Missing Data**: Verify that the account has data for the specified date range and report types.

## API Rate Limits

Be aware of the rate limits for the Amazon SeelingPartner API. 
The job is designed to respect these limits through its retry mechanism.
For more information see: 
[Request and method call limits](https://developer-docs.amazon.com/sp-api/docs/strategies-to-optimize-rate-limits-for-your-application-workloads)

## Related Documentation

- [Bing Ads Reporting API Reference](https://developer-docs.amazon.com/sp-api/docs/report-type-values-analytics#market-basket-analysis-report)
- [Application Support | API Details](https://developer.amazonservices.com/support)