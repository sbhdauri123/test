# LinkedIn Import Job

## Overview
This job handles the import of Linkedin reports, processing them and storing them in S3.

## Supported Report Types
* AdAccounts
* AdCampaignGroups
* AdCampaigns
* Creatives
* AdsReport

## Process Flow

::: mermaid
graph TB;
    PreExecute -->|Initialization| Execute -->|Queue Processing| DownloadFactReports
    --> HasDimensionDataBeenDownloadedToday{HasDimensionDataBeenDownloadedToday}
        HasDimensionDataBeenDownloadedToday -->|YES| CreateManifestFiles
        HasDimensionDataBeenDownloadedToday -->|NO| DownloadDimensionReports --> SaveReportToS3Raw[(Save Report in S3 'RAW' Folder)] 
        --> CreateManifestFiles
        --> SkippedDownloadedDimensions{Skipped Downloading Dimensions?}
        SkippedDownloadedDimensions --> | YES | PreviousDownloadedDimensions[Get previous downloaded dimensions from today] --> UpdateFileCollectionJSON
        SkippedDownloadedDimensions --> | NO | CurrentDownloadedDimensions[Get current downloaded dimensions] --> UpdateFileCollectionJSON
        --> SaveLookups[(Save Dimensions Lookups to ConfigurationDB)] 
:::

1. **PreExecute**: The job initializes necessary components and retrieves configuration settings.
2. **Execute**: Processes queue items for report download.
3. **Download Reports**: Downloads the reports and save them in S3.
4. **Skip Dimension Report Download**: If dimensions have been downloaded today, skip the download for the current queue.
5. **Create Manifest Files**: Creates the manifest files for all downloaded reports, fact and dimension.
6. **Update FileCollectionJSON**: Include in the FileCollectionJSON the FilePath, FileSize and SourceFileName.
   * If dimensions download was skipped, then get the dimensions manifest filepath from lookup and update FileCollectionJSON.
   * Otherwise, add the current dimension manifest files to FileCollectionJSON.
7. Update dimensions lookup in Configuration database.
