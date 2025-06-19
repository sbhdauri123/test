using Amazon.S3.Transfer;
using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.Databricks;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Jobs.Framework;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Tar;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.SizmekMDX
{
    [Export("SizmekMDX-DeliveryDataLoad", typeof(IDragoJob))]
    public class DataLoadJob : BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private RemoteAccessClient RAC;
        private Uri _baseDestUri;
        private Uri _baseLocalImportUri;
        private Uri _baseLocalTransformUri;
        private string _fileGuid;
        private IFileItem _queueItem;
        private IEnumerable<FileFormat> _allFileFormats;

        private string JobGUID
        {
            get { return this.JED.JobGUID.ToString(); }
        }

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            RAC = GetS3RemoteAccessClient();
            _baseDestUri = GetDestinationFolder();
            _baseLocalImportUri = GetLocalImportDestinationFolder();
            _baseLocalTransformUri = GetLocalTransformDestinationFolder();
            CurrentIntegration = Data.Services.SetupService.GetById<Integration>(GetUserSelection(Constants.US_INTEGRATION_ID));
            _allFileFormats = Data.Services.SetupService.GetAll<FileFormat>();
        }

        public void Execute()
        {
            string failurePoint = string.Empty;

            try
            {
                failurePoint = "cleanup";
                var ghostBatch = 18;
                var integrationIdPartition = $"integrationid={IntegrationId.ToString()}";
                Uri stageIntegrationRootBucket = new Uri(this._baseDestUri.ToString()
                    .Replace(Constants.ProcessingStage.RAW.ToString().ToLower(),
                        Constants.ProcessingStage.STAGE.ToString().ToLower()));

                var stageS3IntegrationRoot =
                    RAC.WithDirectory(RemoteUri.CombineUri(stageIntegrationRootBucket, integrationIdPartition));
                if (stageS3IntegrationRoot.Exists)
                {
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                        string.Format("{0} - Deleting s3 stage directory at onset. Path {1}.",
                            JobGUID, stageS3IntegrationRoot.Uri.AbsolutePath)));
                    stageS3IntegrationRoot.Delete(true);
                }

                Uri tempLocalImportUri = RemoteUri.CombineUri(_baseLocalImportUri, integrationIdPartition);
                FileSystemDirectory localImportDirectory = new FileSystemDirectory(tempLocalImportUri);
                if (localImportDirectory.Exists)
                {
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                        string.Format("{0} - Deleting files from local import directory at onset. Path {1}.",
                            JobGUID, localImportDirectory.Uri.AbsolutePath)));
                    localImportDirectory.Delete(true);
                }

                Uri tempLocalTransformUri = RemoteUri.CombineUri(_baseLocalTransformUri, integrationIdPartition);
                FileSystemDirectory localTransformDirectory =
                    new FileSystemDirectory(tempLocalTransformUri);
                if (localTransformDirectory.Exists)
                {
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                        string.Format(
                            "{0} - Deleting files from local transform directory at onset. Path {1}.",
                            JobGUID, localTransformDirectory.Uri.AbsolutePath)));
                    localTransformDirectory.Delete(true);
                }

                failurePoint = "pre-extract";
                var queueItems = Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID)
                    .OrderBy(x => x.ID)
                    .ToList();

                var etlJobRepo = new DatabricksETLJobRepository();
                var etlJob = etlJobRepo.GetEtlJobByDataSourceID(CurrentSource.DataSourceID);

                if (etlJob == null)
                {
                    throw new DatabricksETLJobNotFoundException("No DatabricksETLJob found for DataSourceID=" + CurrentSource.DataSourceID);
                }

                foreach (var queueItem in queueItems)
                {
                    _queueItem = queueItem;
                    _fileGuid = _queueItem.FileGUID.ToString();
                    var fileDatePartition = GetDatedPartition(_queueItem.FileDate);
                    var fileDateHour = (int)_queueItem.FileDateHour;
                    var fileDateHourPartition = GetHourPartition(fileDateHour);
                    var fileTypePartition = $"filetype={_queueItem.SourceFileName}";
                    string[] sourcePaths = new string[] { fileDatePartition };
                    string[] destPaths = new string[]
                    {
                        integrationIdPartition, fileDatePartition,
                        fileDateHourPartition, fileTypePartition
                    };
                    Uri sourceBucket = RemoteUri.CombineUri(this._baseDestUri, sourcePaths);
                    Uri destBucket = new Uri(this._baseDestUri.ToString()
                        .Replace(Constants.ProcessingStage.RAW.ToString().ToLower(),
                            Constants.ProcessingStage.STAGE.ToString().ToLower()));
                    var tarExtractFileExtension = "";

                    if (_queueItem.FileDateHour == ghostBatch)
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                            string.Format(
                                "{0} - Archive for {1} contains empty files. Processing skipped for {2}, deleting record from the queue.",
                                JobGUID, _fileGuid, fileDateHourPartition)));
                        Data.Services.JobService.Delete((Queue)_queueItem);
                        continue;
                    }

                    foreach (var file in _queueItem.FileCollection)
                    {
                        IFile sourceTar = RAC.WithFile(RemoteUri.CombineUri(sourceBucket, file.FilePath));
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                            string.Format("{0} - Ready to extract file: {1} ({2} bytes) to {3}", JobGUID,
                                sourceTar.FullName, (sourceTar != null ? sourceTar.Length : -1),
                                destBucket.ToString())));
                        Uri tempDestUri = RemoteUri.CombineUri(_baseLocalImportUri, destPaths);
                        tempDestUri = RemoteUri.CombineUri(tempDestUri, sourceTar.Name);
                        FileSystemFile tempDestFile = new FileSystemFile(tempDestUri);

                        if (!tempDestFile.Directory.Exists)
                        {
                            tempDestFile.Directory.Create();
                        }

                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                            string.Format("{0} - Copying file to file system first: {1}", JobGUID, tempDestUri)));
                        sourceTar.CopyTo(tempDestFile, true);
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                            string.Format("{0} - local copy complete, ready for extract", JobGUID)));

                        using (GZipInputStream inStream = new GZipInputStream(tempDestFile.Get()))
                        {
                            failurePoint = "extract";

                            TarInputStream tarInStream = new TarInputStream(inStream, Encoding.UTF8);
                            TarEntry tarEntry;

                            while ((tarEntry = tarInStream.GetNextEntry()) != null)
                            {
                                if (tarEntry.IsDirectory)
                                    continue;
                                Uri tarEntryUri = RemoteUri.CombineUri(_baseLocalTransformUri, destPaths);
                                tarEntryUri = RemoteUri.CombineUri(tarEntryUri, tarEntry.Name);
                                FileSystemFile tarEntryFile = new FileSystemFile(tarEntryUri);

                                if (!tarEntryFile.Directory.Exists)
                                {
                                    tarEntryFile.Directory.Create();
                                }

                                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                                    string.Format("{0} - Writing TarEntry: {1}", JobGUID, tarEntryFile)));

                                using (Stream outStream = tarEntryFile.Create())
                                {
                                    byte[] buffer = new byte[4096];
                                    int read;
                                    while ((read = tarInStream.Read(buffer, 0, buffer.Length)) > 0)
                                    {
                                        outStream.Write(buffer, 0, read);
                                    }
                                }

                                tarExtractFileExtension = tarEntryFile.Extension;
                                Uri destUri = RemoteUri.CombineUri(destBucket, destPaths);
                                destUri = RemoteUri.CombineUri(destUri, tarEntry.Name);

                                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                                    string.Format("{0} - TarEntry: {1} complete, pushing to S3: {2}", JobGUID,
                                        tarEntryFile,
                                        destUri)));

                                Amazon.S3.Util.AmazonS3Uri s3Uri = new Amazon.S3.Util.AmazonS3Uri(destUri);
                                TransferUtility tu = GetMultipartTransferUtility(Configuration.Settings.Current.AWS.Region);
                                tu.UploadAsync(tarEntryFile.FullName, s3Uri.Bucket, s3Uri.Key).GetAwaiter().GetResult();
                                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                    string.Format(
                                        "{0} - TransferUtility S3 URI {1} upload complete, deleting local file: {2}",
                                        JobGUID, destUri,
                                        tarEntryFile)));
                                tarEntryFile.Delete();
                            }

                            tarInStream.Close();
                        }
                    }

                    failurePoint = "dataload";

                    var dataSource = Data.Services.JobService.GetById<DataSource>(CurrentSource.DataSourceID);
                    var sourceType = base.SourceTypes.Find(x => x.SourceTypeID == dataSource.SourceTypeID);
                    bool isFirstQueueItem = true;
                    Country country = Data.Services.JobService.GetById<Country>(CurrentIntegration.CountryID);
                    var tmpSrc = new SourceFile();
                    var srcFile = base.SourceFiles.FirstOrDefault(x =>
                        x.SourceFileName.Equals(queueItem.SourceFileName, StringComparison.InvariantCultureIgnoreCase));
                    Uri stageUri = RemoteUri.CombineUri(destBucket, destPaths);
                    var srcFileUri = string.Format("s3a://{0}/*{1}", stageUri.AbsolutePath.TrimStart('/'),
                        tarExtractFileExtension);
                    var tableName = string.Format(etlJob.DatabricksTableName, queueItem.SourceFileName);
                    string partitionCount = srcFile.PartitionCount?.ToString() ?? tmpSrc.PartitionCount.ToString();
                    var fileFormat = _allFileFormats.FirstOrDefault(x => x.FileFormatID == srcFile.FileFormatID);

                    var jobParams = new string[]
                    {
                        "s3", this.RootBucket, queueItem.SourceFileName, country.CountryName,
                        queueItem.FileGUID.ToString(),
                        tableName,
                        srcFileUri, CurrentIntegration.TimeZoneString, srcFile.PartitionColumn, partitionCount,
                        dataSource.DataSourceName, sourceType.SourceTypeName.ToLower(), srcFile.FileDelimiter,
                        srcFile.HasHeader.ToString(), fileFormat.FileFormatName
                    };

                    var msg = string.Format(
                        "{3} - Submitting spark job for integration: {0}; source: {1}; with parameters {2}",
                        CurrentIntegration.IntegrationID, queueItem.SourceFileName,
                        JsonConvert.SerializeObject(jobParams),
                        queueItem.FileGUID);
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, msg));

                    var job = Task.Run(async () => await base.SubmitSparkJobDatabricks(etlJob.DatabricksJobID,
                        queueItem, isFirstQueueItem, true, false, jobParams));
                    job.Wait();

                    var jsonResult = JsonConvert.SerializeObject(job.Result);
                    if (job.Result != ResultState.SUCCESS)
                    {
                        string errMessage = PrefixJobGuid($"ERROR->Spark job queue id: {queueItem.ID} returned job status: {job.Result.ToString()}");
                        throw new DatabricksResultNotSuccessfulException(errMessage);
                    }
                    else
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"SUCCESS->Spark job for integration: {CurrentIntegration.IntegrationID};queue id: {queueItem.ID}; source: {queueItem.SourceFileName}; Summary: {job.Result.ToString()}")));
                    }

                    isFirstQueueItem = false;
                    failurePoint = "cleanup";
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                        string.Format(
                            "{0} - Archive for {1}/{2}/{3} successfully processed, deleting local and s3 stage files.",
                            JobGUID,
                            fileDatePartition, fileDateHourPartition, fileTypePartition)));

                    if (stageS3IntegrationRoot.Exists)
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                            string.Format("{0} - Deleting s3 stage directory queueID {1}. Path {2} for file guid: {3}",
                                JobGUID, _queueItem.ID, stageS3IntegrationRoot.Uri.AbsolutePath, _fileGuid)));
                        stageS3IntegrationRoot.Delete(true);
                    }

                    if (localImportDirectory.Exists)
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                            string.Format(
                                "{0} - Deleting local import directory queueID {1}. Path {2} for file guid: {3}",
                                JobGUID, _queueItem.ID, localImportDirectory.Uri.AbsolutePath, _fileGuid)));
                        localImportDirectory.Delete(true);
                    }

                    if (localTransformDirectory.Exists)
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                            string.Format(
                                "{0} - Deleting local transform directory queueID {1}. Path {2} for file guid: {3}",
                                JobGUID, _queueItem.ID, localTransformDirectory.Uri.AbsolutePath, _fileGuid)));
                        localTransformDirectory.Delete(true);
                    }
                }
            }
            catch (Exception exc)
            {
                logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                    string.Format("{0} - Failed during: {1} for FileGUID: {2}", JobGUID, failurePoint, _fileGuid),
                    exc));
                Data.Services.JobService.UpdateQueueStatus(_queueItem.ID, Constants.JobStatus.Error);
                throw;
            }
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

        ~DataLoadJob()
        {
            Dispose(false);
        }
    }
}