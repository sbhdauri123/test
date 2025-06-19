# Walmart Onsite Import Job

## Introduction
This job imports Walmart Onsite reports.

## API Documentation
https://developer.walmart.com/doc/us/us-wmc-display/us-wmc-display-snapshot-reports/

## API Overview
Walmart refers to reports as Snapshots. There are two types of snapshots: Report and Entity. Report Snapshots contain metric data while Entity Snapshots contain dimension data.

This API uses asynchronous report generation. There are three API endpoints that must be called in order to download a report:<br />
1. Generate a Snapshot
2. Check the status of a Snapshot. If ready to download, the API will provide a link which needs to be used in the download step
3. Download Report

Reports are returned from the API as a GZIP file. When decompressed, a single file will be available. Report Snapshots will be a single CSV file while Entity Snapshots will be a single JSON file. 

#### Authorization
To ensure that API calls are valid, Walmart requires us to send an RSA encrypted string as part of the HTTP call's header. This logic is encapsulated in WalmartOnsiteService's GetAuthSignature() method. 

#### Api Rate Limit
1,000 API calls per hour. The documentation states that the rate limit is enforced per minute, but the Walmart team informed us that it is enforced per hour.

#### Misc Information
If a Snapshot is not available for a requested date, the API will not tell us that until the Download Report step. When this happens, we get a 200 Status from the API with text that states "ERROR No report data available for this \<ENTITY_TYPE\>".

## Code Overview
The logic to make API calls is encapsulated in the WalmartOnsiteService class. The Import Job is responsible for making the appropriate calls using WalmartOnsiteService, saving unfinished reports(Snapshots), saving the reports to S3, and logging any errors.

