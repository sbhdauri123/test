using Amazon.S3.Transfer;
using Greenhouse.Common;
using Greenhouse.DAL;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using ICSharpCode.SharpZipLib.Zip;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;

namespace Greenhouse.Jobs.MediaOcean
{
    [Export("MediaOcean-DigitalDeliveryDataLoadJob", typeof(IDragoJob))]
    [Export("MediaOcean-NetDeliveryDataLoadJob", typeof(IDragoJob))]
    [Export("MediaOcean-PrintDeliveryDataLoadJob", typeof(IDragoJob))]
    [Export("MediaOcean-SpotDeliveryDataLoadJob", typeof(IDragoJob))]
    public class DeliveryDataLoadJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private const string REDSHIFT_LOAD_SCRIPT_NAME = "redshiftloadmo.sql";

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private RemoteAccessClient RAC;
        private Uri _baseDestUri;

        private Uri _scriptPath;
        private string _fileGuid;
        private IFileItem _queueItem;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            RAC = GetS3RemoteAccessClient();
            _baseDestUri = GetDestinationFolder();
            CurrentIntegration = Data.Services.SetupService.GetById<Integration>(base.IntegrationId);
        }

        public void Execute()
        {
            string failurePoint = string.Empty;

            try
            {
                RAC = GetS3RemoteAccessClient();
                IEnumerable<IFileItem> queueItems = Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID).OrderBy(o => o.FileDate);
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{0} items retreived from queue for Integration: {1} - {2}", queueItems.Count(), CurrentIntegration.IntegrationID, CurrentIntegration.IntegrationName)));

                foreach (IFileItem queueItem in queueItems)
                {
                    _queueItem = queueItem;
                    _fileGuid = _queueItem.FileGUID.ToString();
                    string[] paths = new string[] { _queueItem.EntityID.ToLower(), GetDatedPartition(_queueItem.FileDate) };
                    Uri sourceBucket = RemoteUri.CombineUri(this._baseDestUri, paths);
                    Uri destBucket = new Uri(sourceBucket.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));
                    IFile sourceZip = RAC.WithFile(RemoteUri.CombineUri(sourceBucket, _queueItem.FileName));
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("Ready to extract file: {0} ({1} bytes) to {2}", sourceZip.FullName, (sourceZip != null ? sourceZip.Length : -1), destBucket.ToString())));

                    //should probably be fixed in the proc but this is what they want done GH-853
                    //_queueItem.Status = Constants.JobStatus.Running.ToString();
                    //_queueItem.Step = Constants.JobStep.Extract.ToString();
                    //JobServ.Update<Queue>((Queue)_queueItem);

                    Data.Services.JobService.UpdateQueueStatus(_queueItem.ID, Constants.JobStatus.Running);
                    List<FileCollectionItem> extractedFiles = new List<FileCollectionItem>();

                    Uri tempDestUri = RemoteUri.CombineUri(new Uri(Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseImportPath), paths);
                    tempDestUri = RemoteUri.CombineUri(tempDestUri, sourceZip.Name);
                    FileSystemFile tempDestFile = new FileSystemFile(tempDestUri);

                    if (!tempDestFile.Directory.Exists)
                    {
                        tempDestFile.Directory.Create();
                    }

                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("Copying file to file system first: {0}", tempDestUri)));
                    sourceZip.CopyTo(tempDestFile, true);
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, "local copy complete, ready for extract"));

                    using (ZipInputStream inStream = new ZipInputStream(tempDestFile.Get()))
                    {
                        failurePoint = "extract";
                        ZipEntry zipEntry = inStream.GetNextEntry();

                        while (zipEntry != null)
                        {
                            Uri zipEntryUri = RemoteUri.CombineUri(new Uri(Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseTransformPath), paths);
                            zipEntryUri = RemoteUri.CombineUri(zipEntryUri, zipEntry.Name);
                            FileSystemFile zipEntryFile = new FileSystemFile(zipEntryUri);

                            if (!zipEntryFile.Directory.Exists)
                            {
                                zipEntryFile.Directory.Create();
                            }

                            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("Writing ZipEntry: {0}", zipEntryFile)));

                            using (Stream outStream = zipEntryFile.Create())
                            {
                                byte[] buffer = new byte[4096];
                                int read;
                                while ((read = inStream.Read(buffer, 0, buffer.Length)) > 0)
                                {
                                    outStream.Write(buffer, 0, read);
                                }
                            }

                            Uri destUri = RemoteUri.CombineUri(destBucket, zipEntry.Name);
                            IFile destFile = RAC.WithFile(destUri);

                            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("ZipEntry: {0} complete, pushing to S3: {1}", zipEntryFile, destUri)));

                            Amazon.S3.Util.AmazonS3Uri s3Uri = new Amazon.S3.Util.AmazonS3Uri(destUri);
                            TransferUtility tu = GetMultipartTransferUtility(Configuration.Settings.Current.AWS.Region);
                            tu.UploadAsync(zipEntryFile.FullName, s3Uri.Bucket, s3Uri.Key).GetAwaiter().GetResult();
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("TransferUtility S3 URI {0} upload complete, deleting local file: {1}", destUri, zipEntryFile)));
                            zipEntryFile.Delete();

                            zipEntry = inStream.GetNextEntry();
                        }//while (zipEntry != null) {
                    }
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("Archive {0} successfully processed, deleting local file: {1}", sourceZip, tempDestFile)));

                    //all files extracted, time to ETL em
                    failurePoint = "dataload";

                    Uri baseUri = RemoteUri.GetServiceUri(Constants.URI_SCHEME_S3, Greenhouse.Configuration.Settings.Current.AWS.Region, this.RootBucket);

                    DataLoadFile(destBucket, baseUri, _queueItem.FileGUID, _queueItem.EntityID);
                    if (!CurrentSource.AggregateProcessingSettings.SaveStagedFiles)
                    {
                        //Delete stage files by entityid
                        var dirPath = new string[] { _queueItem.EntityID.ToLower(), GetDatedPartition(_queueItem.FileDate) };
                        DeleteStageFiles(dirPath, _queueItem.FileGUID, _queueItem.EntityID);
                    }
                    //all done, delete from local file system and the Queue
                    tempDestFile.Delete();
                    Data.Services.JobService.Delete((Queue)_queueItem);
                }//foreach (IFileItem fi in queueItems) {
            }
            catch (Exception exc)
            {
                logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("Failed during: {0} for FileGUID: {1}", failurePoint, _fileGuid), exc));
                //make sure we log the failure to the transfer logs
                //GH-853
                Data.Services.JobService.UpdateQueueStatus(_queueItem.ID, Constants.JobStatus.Error);
                throw;
            }
        }

        private void DataLoadFile(Uri destBucket, Uri baseUri, Guid fileGUID, string entityId)
        {
            //prepare bucket path to work with Redshift
            string replace = string.Format("{0}/", Amazon.RegionEndpoint.GetBySystemName(Greenhouse.Configuration.Settings.Current.AWS.Region).GetEndpointForService("s3").Hostname);
            string s3StageFilePath = destBucket.ToString().Replace(replace, string.Empty);
            //script path
            string[] paths = new string[] { "scripts", "etl", "redshift", CurrentSource.SourceName.ToLower(), REDSHIFT_LOAD_SCRIPT_NAME };

            _scriptPath = RemoteUri.CombineUri(baseUri, paths);
            IFile scriptFile = RAC.WithFile(_scriptPath);

            string script = ETLProvider.GetScript(scriptFile);
            string cmdText = RedshiftRepository.PrepareCommandText(script, GetScriptParameters(fileGUID.ToString(), s3StageFilePath, entityId));
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("Script: {0} prepared and ready to execute for FileGUID: {1} and stagefilepath: {2}", scriptFile, fileGUID, s3StageFilePath)));
            int retVal = RedshiftRepository.ExecuteRedshiftCommand(cmdText);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("Script: {0} executed, result: {1}", scriptFile, retVal)));
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
                RAC?.Dispose();
            }
        }

        ~DeliveryDataLoadJob()
        {
            Dispose(false);
        }

        private static List<System.Data.Odbc.OdbcParameter> GetScriptParameters(string fileguid, string stagefilepath, string entityId)
        {
            List<System.Data.Odbc.OdbcParameter> parameters = new List<System.Data.Odbc.OdbcParameter>();
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "stagefilepath", Value = stagefilepath });
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "accesskey", Value = Greenhouse.Configuration.Settings.Current.AWS.Credentials.GetCredentials().AccessKey });
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "secretkey", Value = Greenhouse.Configuration.Settings.Current.AWS.Credentials.GetCredentials().SecretKey });
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "fileguid", Value = fileguid });
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "entityid", Value = entityId });
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "region", Value = Greenhouse.Configuration.Settings.Current.AWS.Region });
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "iamrole", Value = Greenhouse.Configuration.Settings.Current.AWS.IamRoleRedshiftCopyS3 });
            return parameters;
        }
    }
}