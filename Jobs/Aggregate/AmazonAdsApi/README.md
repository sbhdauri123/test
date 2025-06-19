# Amazon Ads API Import Job

## Overview
This job handles the import of Amazon Ads API reports, processing them and storing them in S3.

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

- SponsoredProductsCampaignByCampaignReport
- SponsoredProductsCampaignByAdGroupReport
- SponsoredProductsCampaignByCampaignPlacementReport
- SponsoredBrandsCampaignByCampaignReport
- SponsoredDisplayCampaignByCampaignReport
- SponsoredDisplayCampaignByMatchedTargetReport
- SponsoredTelevisionCampaignByCampaignReport
- SponsoredTelevisionCampaignByAdGroupReport
- SponsoredBrandsAdGroupyByAdGroupReport
- SponsoredDisplayAdGroupByAdGroupReport
- SponsoredDisplayAdGroupByMatchedTargetReport
- SponsoredBrandsPlacementByCampaignPlacementReport
- SponsoredProductsTargetingByTargetingReport
- SponsoredBrandsTargetingByTargetingReport
- SponsoredDisplayTargetingByTargetingReport
- SponsoredDisplayTargetingByMatchedTargetReport
- SponsoredTelevisionTargetingByCampaignReport
- SponsoredTelevisionTargetingByAdGroupReport
- SponsoredTelevisionTargetingByTargetingReport
- SponsoredProductsSearchtermBySearchTermReport
- SponsoredBrandsSearchtermBySearchTermReport
- SponsoredProductsAdvertisedByAdvertiserReport
- SponsoredDisplayAdvertisedByAdvertiserReport
- SponsoredBrandsAdByAdsReport
- SponsoredProductsPurchasedByAsinReport
- SponsoredBrandsPurchasedByPurchasedAsinReport
- SponsoredDisplayPurchasedByAsinReport
- SponsoredProductsGrossandInvalidByCampaignReport
- SponsoredBrandsGrossandInvalidByCampaignReport
- SponsoredDisplayGrossandInvalidByCampaignReport
- AmazonDSPTechByOrderReport
- AmazonDSPTechByLineItemReport
- AmazonDSPTechByOperatingSystemReport
- AmazonDSPTechByBrowserTypeReport
- AmazonDSPTechByBrowserVersionReport
- AmazonDSPTechByDeviceTypeReport
- AmazonDSPTechByEnvironmentTypeReport
- AmazonDSPProductByOrderReport
- AmazonDSPProductByLineItemReport
- AmazonDSPInventoryBySupplySourceReport
- AmazonDSPInventoryBySiteReport
- AmazonDSPInventoryByCampaignReport
- AmazonDSPInventoryByCreativeReport
- AmazonDSPInventoryByAdReport
- AmazonDSPInventoryByPlacementSizeReport
- AmazonDSPInventoryByPlacementReport
- AmazonDSPInventoryByDealReport
- AmazonDSPGeoByOrderReport
- AmazonDSPGeoByLineItemReport
- AmazonDSPGeoByCountryReport
- AmazonDSPGeoByRegionReport
- AmazonDSPGeoByCityReport
- AmazonDSPGeoByPostalCodeReport
- AmazonDSPGeoByDmaReport
- AmazonDSPAudienceByOrderReport
- AmazonDSPAudienceByLineItemReport
- AmazonDSPCampaignByCampaignReport
- AmazonDSPCampaignByAdReport
- AmazonDSPCampaignByCreativeReport

## Process Flow

::: mermaid
graph TB;
    A[PreExecute] -->|Initialization| B[Execute]
    B -->|ProcessQueueItems| C{Check Account Type}
    C -->|Agency| D[GetAdvertiserIds] --> E[ReportGeneration for AmazonDSP]
    C -->|Vendor or Seller| F[ReportGeneration for Non-AmazonDSP]
    E --> G[SaveUnfinishedReports]
    F --> G
    B -->|CheckReportStatusAndDownload| H[Check Report Status]
    H --> I{ReportRequestStatusType}
    I -->|Success| J[DownloadReport] --> K[(Save Report in S3)]
    I -->|Error| L[HandleFailedReport]
    K --> G
    L --> G
:::

1. **Initialization**: The job initializes necessary components and retrieves configuration settings.
2. **Queue Processing**: Processes queue items to determine which reports need to be generated.
3. **Account Type Check**: Evaluates the account type to decide the report generation path:
    - **Agency Accounts**: Retrieves advertiser IDs and requests AmazonDSP reports.
    - **Vendor or Seller Accounts**: Directly requests Non-AmazonDSP reports.
4. **Report Generation**: Submits report requests to the Amazon Ads API for each report type.
    - **Save Unfinished Reports**: Both report generation paths save unfinished reports for future processing.
5. **Check Report Status and Download**: Independently, the job checks the status of submitted report requests:
    - If the report status is Success, it downloads the report and saves it to S3 storage.
    - If the report status is Error, it handles the failure appropriately.
6. **Download and Storage**: Downloads ready reports and stores them in S3.
7. **Error Handling**: Manages exceptions and retries failed operations.
8. **Cleanup**: Saves information about unfinished reports for future processing.

## Key Components

- `ImportJob`: Main class orchestrating the import process.
- `AmazonAdsApiService`: Handles interaction with the Amazon Ads API.
- `RemoteAccessClient`: Manages S3 operations.
- `ExponentialBackOffStrategy`: Implements retry logic for API calls.
- `AmazonAdsApiOAuth`: Handles OAuth with the Amazon Ads API.

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

* Insert the new report in APIREPORT table in Sql DB with proper SourceID, CredentialId and ReportSettingsJSON.

## Troubleshooting

Common issues and their solutions:

* **API Authentication Failures**: Ensure OAuth credentials are correct and not expired. 
Reach out [Maxwell Adamsky](mailto:maxwell.adamsky@publicismedia.com) for requesting a new refresh token.
* **Report Download Timeouts**: Check the MaxRuntime setting and consider increasing it.
* **Missing Data**: Verify that the account has data for the specified date range and report types.

## API Rate Limits

Be aware of the rate limits for the Amazon Ads API. 
The job is designed to respect these limits through its retry mechanism.
For more information see: 
[Request and method call limits](https://webservices.amazon.com/paapi5/documentation/troubleshooting/api-rates.html)
(https://advertising.amazon.com/API/docs/en-us/reference/concepts/rate-limiting)
## Related Documentation

- [Amazon Ads Reporting API Reference](https://advertising.amazon.com/API/docs/en-us/guides/reporting/v3/report-types/campaign)
- [Application Support | API Details](https://developer.amazonservices.com/support)