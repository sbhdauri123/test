# Innovid Import Job

## Overview
This job handles the import of Innovid reports, processing them and storing them in S3.

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

- Delivery
- Viewability
- VersionDelivery

## Innovid API Used Endpoints

BaseUrl - https://papi.innovid.com

- /v3/reports - POST
- /v3/reports/{reportId}/status - GET
- /v3/advertisers - GET

## Process Flow

::: mermaid
graph TB;
    A[PreExecute] -->|Initialization| B[Execute] -->|Queue Processing| C[RequestGenerateReport]
    -->|Status Checking|D[GetReportStatus] --> E{ReportRequestStatusType}
    E -->|Success| F[DownloadReport] --> G[(Save Report in S3)]
    E -->|Error| H[HandleFailedReport]
    G --> I[SaveUnfinishedReports]
    H --> I[SaveUnfinishedReports]
:::

1. **Initialization**: The job initializes necessary components and retrieves configuration settings, including loading the unfinished "bundles" (reports)
2. **Queue Processing**: Processes queue items for report generation.
3. **Report Generation**: Submits report requests to the Innovid API for each report type.
4. **Status Checking**: Polls the API to check if reports are ready for download.
5. **Download and Storage**: Downloads ready reports and stores them in S3.
6. **Error Handling**: Manages exceptions and retries failed operations.
7. **Cleanup**: Saves information about unfinished reports for future processing.

## Key Components

- `ImportJob`: Main class orchestrating the import process.
- `Reports`: Handles interaction with the Innovid API.
- `RemoteAccessClient`: Manages S3 operations.
- `ExponentialBackOffStrategy`: Implements retry logic for API calls.

## Error Handling and Logging

The job implements comprehensive error handling and logging:

* Retries failed API calls up to a configurable number of attempts.
* Logs detailed information about each step of the process.
* Tracks unfinished reports for processing in subsequent runs.

## Performance Considerations

* Implements a maximum runtime to prevent excessively long job execution.


## Add New Report Type



## Troubleshooting

Common issues and their solutions:

* **API Authentication Failures**: Ensure OAuth credentials are correct and not expired. 
Reach out [Maxwell Adamsky](mailto:maxwell.adamsky@publicismedia.com) for requesting a new refresh token.
* **Report Download Timeouts**: Check the MaxRuntime setting and consider increasing it.
* **Missing Data**: Verify that the account has data for the specified date range and report types.

## API Rate Limits

Be aware of the rate limits for the Innovid API. 
The job is designed to respect these limits through its retry mechanism.
For more information see: 
[Request and method call limits](https://learn.microsoft.com/en-us/advertising/shopping-content/request-method-limits)

## Related Documentation

- [Innovid API Documentation](https://help.innovid.com/hc/en-us/articles/115005281145-Innovid-s-Data-API)
- [Application Support | API Details](https://techconfluence.publicis.com/pages/viewpage.action?spaceKey=PT&title=Application+Support+%7C+API+Details)