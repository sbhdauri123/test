using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.Data.DataSource.Pathmatics;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Jobs.Framework;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Net;

namespace Greenhouse.Jobs.Aggregate.Pathmatics;

[Export("Pathmatics-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : BaseFrameworkJob, IDragoJob
{
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private RemoteAccessClient RAC;
    private Uri baseRawDestUri;
    private Uri baseStageDestUri;
    private Action<string> logInfo;
    private Action<string> logError;

    public void PreExecute()
    {
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        baseRawDestUri = GetDestinationFolder();
        baseStageDestUri = new Uri(baseRawDestUri.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));
        logInfo = (msg) => logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid(msg)));
        logError = (msg) => logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuid(msg)));

        logInfo($"IMPORT-PREEXECUTE {DefaultJobCacheKey}");
    }

    public void Execute()
    {
        logInfo($"EXECUTE START {DefaultJobCacheKey}");

        try
        {
            // retrieve s3 creds from pathmatics saved under a different aws profile
            RAC = GetRemoteAccessClient(CurrentIntegration, CurrentSource.SourceName.ToLower());
            RegexCodec regCod = new RegexCodec(CurrentIntegration.RegexMask);

            // the path in the "latest.txt" file is the location of the latest batch of files to be processed
            // there are four parts to the path (example: (s3://ym-reports/pm-pfx-collective-pathmatics/10688/006ea07c-f76c-4b75-82b3-fbe3f52bda1d/))
            // s3://root-bucket/entity-id/job-id/random-id/
            // we save the "job-id/random-id" in queue.FileName to check if we have downloaded the latest
            (DeliveryPath deliveryPath, IFile latestFile) = GetDeliveryPath(regCod);

            var sourceFile = base.SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(latestFile.Name) && s.IsDoneFile);
            var sourceFileName = sourceFile?.SourceFileName ?? "latest";

            var processedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);
            var processedFileNames = processedFiles.Select(f => f.FileName);

            if (processedFileNames.Contains(deliveryPath.FileName))
            {
                logInfo($"{sourceFileName} files have already been processed");
                return;
            }

            (List<IFile> parquetFiles, Uri destUri, string stageFilePath) = GetParquetFiles(deliveryPath, latestFile);

            if (parquetFiles.Count == 0)
            {
                logInfo($"No parquet files to process");
                return;
            }

            var totalBytesOut = UtilsText.GetFormattedSize(Convert.ToDouble(bytesOut));
            logInfo($"Staging {filesOut} parquet files ({totalBytesOut}) here: {stageFilePath}");

            // clean up local folder in case previous jobs were cut short during import
            CleanupLocalImportFolder();

            var fileCollection = new List<FileCollectionItem>();
            foreach (var incomingFile in parquetFiles)
            {
                Uri stageFileUri = RemoteUri.CombineUri(destUri, new string[] { incomingFile.Name });
                var destFile = new S3File(stageFileUri, GreenhouseS3Creds);
                var localPaths = new string[] { this.Stage.ToString().ToLower(), CurrentSource.SourceName.Replace(" ", string.Empty).ToLower(), CurrentIntegration.IntegrationID.ToString(), incomingFile.Name };
                base.UploadToS3(incomingFile, (S3File)destFile, localPaths, forceCopyToLocal: true);
                filesIn++;
                bytesIn += destFile.Length;

                var fileItem = new FileCollectionItem { SourceFileName = sourceFileName, FilePath = incomingFile.Name, FileSize = incomingFile.Length };
                fileCollection.Add(fileItem);
            }

            var totalBytesIn = UtilsText.GetFormattedSize(Convert.ToDouble(bytesIn));
            if (bytesOut != bytesIn || filesOut != filesIn)
                throw new FileSizeMismatchException($"stage files are different size/count than reported at intake|Outgoing at Pathmatics>Files:{filesOut};Bytes:{totalBytesOut}||Incoming at Raw Folder>Files:{filesIn};Bytes:{totalBytesIn}");

            var importFile = new Queue()
            {
                FileGUID = Guid.NewGuid(),
                FileName = deliveryPath.FileName,
                SourceFileName = sourceFileName,
                FileSize = bytesOut,
                IntegrationID = CurrentIntegration.IntegrationID,
                SourceID = CurrentSource.SourceID,
                JobLogID = this.JobLogger.JobLog.JobLogID,
                EntityID = deliveryPath.Entity,
                Step = JED.Step.ToString(),
                FileCollectionJSON = JsonConvert.SerializeObject(fileCollection),
                FileDate = latestFile.LastWriteTimeUtc.Date,
                DeliveryFileDate = latestFile.LastWriteTimeUtc
            };

            // processing job will use manifest file to perform Copy command
            // files are in parquet format and Redshift-Copy requires the "Meta" property in its Manifest file
            var manifest = new RedshiftManifest();
            fileCollection.ForEach(stageFile => manifest.AddEntryWithMeta($"{stageFilePath}/{stageFile.FilePath}", stageFile.FileSize, true));

            var manifestPath = new string[] { baseStageDestUri.AbsolutePath, deliveryPath.Entity, GetDatedPartition(latestFile.LastWriteTimeUtc.Date), $"{importFile.FileGUID}.manifest" };
            var manifestFilePath = ETLProvider.GenerateManifestFile(manifest, this.RootBucket, manifestPath);
            logInfo($"Successfully created manifest file at: {manifestFilePath}");

            logInfo($"Adding to queue: {JsonConvert.SerializeObject(importFile)}");
            importFile.StatusId = (int)Constants.JobStatus.Complete;
            importFile.Status = Common.Constants.JobStatus.Complete.ToString();
            Data.Services.JobService.Add<IFileItem>(importFile);

            CleanupLocalImportFolder();

            logInfo("EXECUTE END {DefaultJobCacheKey}");
        }
        catch (HttpClientProviderRequestException exc)
        {
            logError($"EXECUTE ERROR {DefaultJobCacheKey}|Exception details : {exc}");
            throw;
        }
        catch (Exception exc)
        {
            logError($"EXECUTE ERROR {DefaultJobCacheKey}|Exception Message:{exc.Message}|STACK:{exc.StackTrace}");
            throw;
        }
    }

    private (List<IFile>, Uri, string) GetParquetFiles(DeliveryPath deliveryPath, IFile latestFile)
    {
        var parquetFiles = new List<IFile>();
        Uri baseUri = RemoteUri.GetServiceUri(Constants.URI_SCHEME_S3, Greenhouse.Configuration.Settings.Current.AWS.Region, deliveryPath.Bucket);
        var latestDeliveryUri = RemoteUri.CombineUri(baseUri, deliveryPath.AbsolutePath);
        parquetFiles = RAC.WithDirectory(latestDeliveryUri).GetFiles().ToList();
        bytesOut = parquetFiles.Sum(s => s.Length);
        filesOut = parquetFiles.Count;

        // save parquet files to Raw directory (basebucket/raw/source/entity/date)
        string[] paths = new string[] { deliveryPath.Entity, GetDatedPartition(latestFile.LastWriteTimeUtc.Date) };
        var destUri = RemoteUri.CombineUri(this.baseRawDestUri, paths);
        var stageFilePath = WebUtility.UrlDecode($"{destUri.OriginalString.Trim('/')}");
        return (parquetFiles, destUri, stageFilePath);
    }

    /// <summary>
    /// Get the "latest" file containing the delivery path to parquet files from latest Pathmatics job
    /// </summary>
    /// <param name="regCod"></param>
    /// <returns>
    /// DeliveryPath object with delivery path components as its properties
    /// </returns>
    private (DeliveryPath, IFile) GetDeliveryPath(RegexCodec regCod)
    {
        logInfo($"Integration:{CurrentIntegration.IntegrationName}|fetching Pathmatic latest file against regex:{regCod.FileNameRegex}");
        var latestFile = RAC.WithDirectory().GetFiles().FirstOrDefault(f => regCod.FileNameRegex.IsMatch(f.Name));
        if (latestFile == null)
            throw new FileNotFoundException($"Latest file is missing!");

        var latestFileContents = string.Empty;
        using (StreamReader reader = new StreamReader(latestFile.Get()))
        {
            latestFileContents = reader.ReadToEnd();
        }

        if (string.IsNullOrEmpty(latestFileContents))
            throw new FileNotFoundException($"Latest file is missing the s3 delivery path in its contents");

        logInfo($"Latest delivery path:{latestFileContents}");

        var deliveryPath = new DeliveryPath(latestFileContents);
        logInfo(deliveryPath.ToString());
        return (deliveryPath, latestFile);
    }

    private void CleanupLocalImportFolder()
    {
        var localDirectoryPaths = new string[] { this.Stage.ToString().ToLower(), CurrentSource.SourceName.Replace(" ", string.Empty).ToLower(), CurrentIntegration.IntegrationID.ToString() };
        var tempLocalImportUri = RemoteUri.CombineUri(new Uri(Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseImportPath), localDirectoryPaths);
        FileSystemDirectory localImportDirectory = new FileSystemDirectory(tempLocalImportUri);
        if (localImportDirectory.Exists)
        {
            localImportDirectory.Delete(true);
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
            RAC?.Dispose();
        }
    }

    ~ImportJob()
    {
        Dispose(false);
    }
}
