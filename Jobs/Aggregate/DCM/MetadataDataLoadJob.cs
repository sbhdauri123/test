using Greenhouse.Common;
using Greenhouse.DAL;
using Greenhouse.Data.DataSource.DCM;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Repositories;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Data.Odbc;
using System.IO;

namespace Greenhouse.Jobs.Aggregate.DCM;

[Export("DCM-AggregateMetadataDataLoad", typeof(IDragoJob))]
public class MetadataDataLoadJob : Framework.BaseFrameworkJob, IDragoJob
{
    private static Logger logger { get; set; } = LogManager.GetCurrentClassLogger();
    private Uri baseUri { get; set; }
    private Uri rawFolderUri { get; set; }
    private Uri stageFolderUri { get; set; }
    private Services.RemoteAccess.RemoteAccessClient RAC { get; set; }
    private string fileGuid { get; set; }

    public void PreExecute()
    {
        RAC = GetS3RemoteAccessClient();
    }

    public void Execute()
    {
        IEnumerable<IFileItem> queueItems = Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID);

        //script path                  
        string redshiftProcessSQL = ETLProvider.GetRedshiftScripts(base.RootBucket, GetRedShiftScriptPath("load"));

        foreach (Queue queueItem in queueItems)
        {
            fileGuid = queueItem.FileGUID.ToString();
            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                PrefixJobGuid($"Start processing - {CurrentIntegration.IntegrationName}; FileGuid: {fileGuid}")));

            //Transform and stage files
            StageDcmMetadataApiFiles(queueItem);

            //Add parameters key/value pair
            List<OdbcParameter> odbcParams = new List<OdbcParameter>
            {
                 new OdbcParameter("stagefilepath", $"{stageFolderUri.OriginalString.Trim('/')}")
                ,new OdbcParameter("accesskey", GreenhouseS3Creds.CredentialSet.AccessKey)
                ,new OdbcParameter("secretkey", GreenhouseS3Creds.CredentialSet.SecretKey)
                ,new OdbcParameter("fileguid", fileGuid)
                ,new OdbcParameter("integrationId", CurrentIntegration.IntegrationID)
            };

            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                PrefixJobGuid($"Start executing redshift load - {GetRedShiftScriptPath("load")}")));

            //PROCESS
            string sql = RedshiftRepository.PrepareCommandText(redshiftProcessSQL, odbcParams);
            int result = RedshiftRepository.ExecuteRedshiftCommand(sql);

            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                PrefixJobGuid($"Completed executing redshift load - {GetRedShiftScriptPath("load")}")));

            //Update and Delete Queue
            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                PrefixJobGuid($"Start update status to 'complete'. Deleting QueueItem:QueueID: {queueItem.ID}; FileGuid: {fileGuid}")));

            base.UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Complete, true);

            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                PrefixJobGuid($"Start update status to 'complete'. Deleting QueueItem:QueueID: {queueItem.ID}; FileGuid: {fileGuid}")));

            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                PrefixJobGuid($"Completed processing - {CurrentIntegration.IntegrationName}; FileGuid: {fileGuid}")));
        }//Completed foreach
    }

    private void StageDcmMetadataApiFiles(Queue queueItem)
    {
        try
        {
            Data.Services.JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);
            IEnumerable<FileCollectionItem> reports = queueItem.FileCollection;
            Action<JArray, string, DateTime, string> writeToFileSignature = ((a, b, c, d) => WriteObjectToFile(a, b, c, d));

            foreach (FileCollectionItem report in reports)
            {
                string[] filePath = new string[] { "metadata", queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), report.FilePath };
                //Set uri source for raw files
                rawFolderUri = GetUri(filePath, Constants.ProcessingStage.RAW);

                //Set uri destination for stage directory
                stageFolderUri = GetUri(null, Constants.ProcessingStage.STAGE);

                //Get file
                IFile rawFile = RAC.WithFile(rawFolderUri);
                string rawText;
                using (StreamReader sr = new StreamReader(rawFile.Get()))
                {
                    rawText = sr.ReadToEnd();
                }

                string JsonFileName = report.FilePath.ToLower();
                logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                    PrefixJobGuid($"staging data for report: {JsonFileName} for FileGUID: {fileGuid}")));
                switch (report.FilePath)
                {

                    case "Ads.json":
                        AdDimension AdDimension = JsonConvert.DeserializeObject<AdDimension>(rawText);
                        DcmService.LoadDcmAdDimension(AdDimension.AdCollection, queueItem.EntityID, queueItem.FileDate, JsonFileName, writeToFileSignature);
                        break;

                    case "Advertisers.json":
                        AdvertiserDimension AdvertiserDimension = JsonConvert.DeserializeObject<AdvertiserDimension>(rawText);
                        DcmService.LoadDcmAdvertiserDimension(AdvertiserDimension.AdvertiserCollection, queueItem.EntityID, queueItem.FileDate, JsonFileName, writeToFileSignature);
                        break;

                    case "Campaigns.json":
                        CampaignDimension CampaignDimension = JsonConvert.DeserializeObject<CampaignDimension>(rawText);
                        DcmService.LoadDcmCampaignDimension(CampaignDimension.CampaignCollection, queueItem.EntityID, queueItem.FileDate, JsonFileName, writeToFileSignature);
                        break;

                    case "Creatives.json":
                        CreativeDimension CreativeDimension = JsonConvert.DeserializeObject<CreativeDimension>(rawText);
                        DcmService.LoadDcmCreativeDimension(CreativeDimension.CreativeCollection, queueItem.EntityID, queueItem.FileDate, JsonFileName, writeToFileSignature);
                        break;

                    case "PlacementGroups.json":
                        PlacementGroupDimension PlacementGroupDimension = JsonConvert.DeserializeObject<PlacementGroupDimension>(rawText);
                        DcmService.LoadDcmPlacementGroupDimension(PlacementGroupDimension.PlacementGroupCollection, queueItem.EntityID, queueItem.FileDate, JsonFileName, writeToFileSignature);
                        break;

                    case "Placements.json":
                        PlacementDimension PlacementDimension = JsonConvert.DeserializeObject<PlacementDimension>(rawText);
                        DcmService.LoadDcmPlacementDimension(PlacementDimension.PlacementCollection, queueItem.EntityID, queueItem.FileDate, JsonFileName, writeToFileSignature);
                        break;

                    default:
                        throw new NotSupportedException($"The DCM report {JsonFileName} is not supported and has no matching POCO");
                }
            }
        }
        catch (HttpClientProviderRequestException exc)
        {
            LogErrorAndUpdateQueueStatus(queueItem, exc);
            throw;
        }
        catch (Exception exc)
        {
            LogErrorAndUpdateQueueStatus(queueItem, exc);
            throw;
        }
    }

    private void LogErrorAndUpdateQueueStatus<TException>(Queue fileItem, TException exc) where TException : Exception
    {
        logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuid(BuildLogMessage(exc)), exc));
        Data.Services.JobService.UpdateQueueStatus(fileItem.ID, Constants.JobStatus.Error);
    }
    private static string BuildLogMessage<TException>(
        TException exc) where TException : Exception
    {
        return exc switch
        {
            HttpClientProviderRequestException httpEx => $"Error staging data in S3 | Exception details : {httpEx}",
            _ => $"Error staging data in S3 - Exception: {exc.GetType().FullName} - STACK {exc.StackTrace}"
        };
    }
    new private Uri GetUri(string[] paths, Constants.ProcessingStage stage)
    {
        this.Stage = stage;
        baseUri = base.GetDestinationFolder();
        if (paths?.Length == 0) { return baseUri; }
        return RemoteUri.CombineUri(this.baseUri, paths);
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
            , "redshift" + scriptType + "dcmdim.sql" };
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

    ~MetadataDataLoadJob()
    {
        Dispose(false);
    }

    private void WriteObjectToFile(JArray entity, string entityID, DateTime fileDate, string filename)
    {
        string[] paths = new string[]
        {
            entityID.ToLower(), GetDatedPartition(fileDate), string.Format(filename, fileDate.ToString("yyyy-MM-dd"))
        };

        IFile transformedFile = RAC.WithFile(RemoteUri.CombineUri(stageFolderUri, paths));
        ETLProvider.SerializeRedshiftJson(entity, transformedFile);
    }
}
