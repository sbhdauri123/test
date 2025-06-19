using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.Data.Model.Core;
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

namespace Greenhouse.Jobs.IAS;

[Export("IAS-DeliveryImportJob", typeof(IDragoJob))]
public class DeliveryImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private RemoteAccessClient remoteAccessClient;
    private Queue importQueueFile;
    private Uri baseDestUri;

    public void PreExecute()
    {
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        baseDestUri = GetDestinationFolder();
    }

    public void Execute()
    {
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuidAndIntegrationInfo($"EXECUTE START {this.GetJobCacheKey()}")));

        var exceptions = new List<string>();

        //Get all processed files.
        var processedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);

        //Initialize the appropriate client for this integration.
        remoteAccessClient = GetRemoteAccessClient();
        RegexCodec regCod = new RegexCodec(CurrentIntegration.RegexMask);
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuidAndIntegrationInfo($"Match source files against regex: {regCod.FileNameRegex}.")));

        //list of IAS files filtered by integration's regex mask -- all types (eg log and checksum)
        var importFiles = remoteAccessClient.WithDirectory().GetFiles().Where(f =>
                regCod.FileNameRegex.IsMatch(f.Name) && regCod.TryParse(f.Name) && regCod.FileNameDate >= CurrentIntegration.FileStartDate).
                OrderByDescending(f => f.Name).ToList();

        //list of log files only
        var logFiles = importFiles.Where(f => f.Extension.Equals(Constants.GZ_EXT, StringComparison.CurrentCultureIgnoreCase)).ToList();

        //list of file names - check-sum files without their ".md5" extension
        var checkSumFiles = importFiles.Where(f => f.Extension.Equals(Constants.CHECK_SUM_EXT, StringComparison.CurrentCultureIgnoreCase)).Select(file => file.Name.Replace(Constants.CHECK_SUM_EXT, "")).ToList();

        //list of log files that have a checksum file (files matched with previous list)
        var pendingFiles = logFiles.Where(file => checkSumFiles.Any(checksumFile => file.Name == checksumFile)).ToList();

        //check for log files with no ".md5" check sum file
        //add exception logging when present
        var filesMissingCheckSum = logFiles.Except(pendingFiles).OrderBy(p => p.LastWriteTimeUtc).ToList();

        if (filesMissingCheckSum.Count != 0)
        {
            var missingMd5Count = 0;
            foreach (var file in filesMissingCheckSum)
            {
                logger.Log(Msg.Create(LogLevel.Warn, logger.Name,
                    PrefixJobGuidAndIntegrationInfo(
                        $"Checksum file (md5 file extension) not found for log file {file.Name} - Size: {file.Length} " +
                        $"- Please note: this file will not be processed until corresponding checksum md5 file is present")));
                missingMd5Count++;
            }
            LogAndAddException(exceptions, $"Total files missing .md5 file: {missingMd5Count} ");
            return;
        }

        //list of log files that need to be processed
        var whatsMissing = pendingFiles.Where(s => !processedFiles.Select(p => p.FileName).Contains(s.Name)).OrderBy(p => p.LastWriteTimeUtc).ToList();

        //Group files by file prefix (thru ".gz" extension) using log file name as key
        //Please note: We are importing all IAS file types
        //but only the single log file will display in the Queue to be processed
        //ie. no file collection recorded
        var pendingFilesDictionary = importFiles.Where(file => whatsMissing.Any(w => file.Name.Contains(w.Name))).GroupBy(file => string.Concat(file.Name.AsSpan(0, file.Name.IndexOf(Constants.GZ_EXT, StringComparison.CurrentCultureIgnoreCase)), Constants.GZ_EXT)).ToDictionary(x => x.Key.ToLower(), files => files.ToList());

        var totalBytes = Greenhouse.Utilities.UtilsText.GetFormattedSize((double)whatsMissing.Sum(s => s.Length));

        logger.Log(Msg.Create(LogLevel.Info, logger.Name,
            PrefixJobGuidAndIntegrationInfo($"Source Files ({remoteAccessClient.ClientType}) with Check Sum: {pendingFiles.Count}" +
                                            $", Destination Files (S3): {processedFiles.Count()} Preparing: {whatsMissing.Count} files for import. " +
                                            $"{totalBytes} total bytes.")));

        ImportFileBatches(pendingFilesDictionary, exceptions);

        logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuidAndIntegrationInfo($"EXECUTE END {this.GetJobCacheKey()}")));

        if (exceptions.Count > 0)
        {
            throw new ErrorsFoundException($"Total errors: {exceptions.Count}; Please check Splunk for more detail.");
        }
    }

    private void ImportFileBatches(Dictionary<string, List<IFile>> pendingFilesDictionary, List<string> exceptions)
    {
        foreach (var fileBatch in pendingFilesDictionary)
        {
            try
            {
                var logFile = fileBatch.Value.Find(x => x.Name == fileBatch.Key);

                var logSourceFile = SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(fileBatch.Key) && s.FileRegexCodec.TryParse(fileBatch.Key));

                if (logSourceFile == null)
                {
                    LogAndAddException(exceptions, $"Skipping log file {fileBatch.Key} ({importQueueFile.FileDate}) because no matching source file found.");
                    continue;
                }

                importQueueFile = new Queue()
                {
                    FileGUID = Guid.NewGuid(),
                    FileName = fileBatch.Key,
                    IntegrationID = CurrentIntegration.IntegrationID,
                    SourceID = CurrentSource.SourceID,
                    Status = Constants.JobStatus.Pending.ToString(),
                    StatusId = (int)Constants.JobStatus.Running,
                    JobLogID = this.JobLogger.JobLog.JobLogID,
                    Step = JED.Step.ToString(),
                    DeliveryFileDate = logFile.LastWriteTimeUtc,
                    SourceFileName = logSourceFile.SourceFileName ?? "log",
                    FileDate = logSourceFile.FileRegexCodec.FileNameDate.Value != default
                        ? logSourceFile.FileRegexCodec.FileNameDate.Value
                        : logFile.LastWriteTimeUtc,
                    FileDateHour = logSourceFile.FileRegexCodec.FileNameHour,
                    EntityID = logSourceFile.FileRegexCodec.EntityId,
                    FileSize = logFile.Length
                };

                var totalBytes = Greenhouse.Utilities.UtilsText.GetFormattedSize(fileBatch.Value.Sum(s => s.Length));
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuidAndIntegrationInfo(
                    $"Source Files ({remoteAccessClient.ClientType}): {fileBatch.Value.Count}" +
                    $", Destination Files (S3): {fileBatch.Value.Count} files for import. {totalBytes} total bytes.")));

                var checksumIsValid = ValidateChecksum(fileBatch, exceptions);

                if (!checksumIsValid)
                {
                    LogAndAddException(exceptions, $"Checksum is not valid for {fileBatch.Key} ({importQueueFile.FileDate}).");
                    continue;
                }

                ImportPendingFiles(fileBatch, exceptions);

                importQueueFile.Status = Common.Constants.JobStatus.Complete.ToString();
                importQueueFile.StatusId = (int)Constants.JobStatus.Complete;
                Data.Services.JobService.Add(importQueueFile);

                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                    PrefixJobGuidAndIntegrationInfo(
                        $"Successfully queued {fileBatch.Key} - date: {importQueueFile.FileDate} - hour: {importQueueFile.FileDateHour}.")));
            }
            catch (HttpClientProviderRequestException exc)
            {
                LogAndAddException(exceptions, $"Error downloading log file -> failed on: {fileBatch.Key} -> Exception details : {exc}");
            }
            catch (Exception exc)
            {
                LogAndAddException(exceptions, $"Error downloading log file -> failed on: {fileBatch.Key} -> Exception: {exc.Message} - STACK {exc.StackTrace}");
            }
        }
    }

    private void LogAndAddException(List<string> exceptions, string errorMessage)
    {
        logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuidAndIntegrationInfo(errorMessage)));
        exceptions.Add(errorMessage);
    }

    private void ImportPendingFiles(KeyValuePair<string, List<IFile>> fileBatch, List<string> exceptions)
    {
        foreach (var incomingFile in fileBatch.Value)
        {
            try
            {
                //log file source file match checked previously in ImportFileBatches
                //check here is for all other file types (eg checksum file)
                if (incomingFile.Name != fileBatch.Key)
                {
                    var matchingSourceFile = SourceFiles.SingleOrDefault(s =>
                        s.FileRegexCodec.FileNameRegex.IsMatch(incomingFile.Name));

                    if (matchingSourceFile == null)
                    {
                        LogAndAddException(exceptions,
                            $"Filename: {incomingFile.Name} skipped because no matching source file found.");
                        continue;
                    }
                }

                ///raw/ias-delivery/{advertiserId}/date/file
                string[] paths = new string[]
                    {importQueueFile.EntityID.ToLower(), GetDatedPartition(importQueueFile.FileDate), incomingFile.Name};
                Uri destUri = RemoteUri.CombineUri(this.baseDestUri, paths);
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                    PrefixJobGuidAndIntegrationInfo(
                        $"DestUri: {JsonConvert.SerializeObject(destUri)}. paths: {JsonConvert.SerializeObject(paths)}.")));
                IFile destFile = new S3File(destUri, GreenhouseS3Creds);

                base.UploadToS3(incomingFile, (S3File)destFile, paths);

                bytesIn += incomingFile.Length;
            }
            catch (HttpClientProviderRequestException exc)
            {
                LogAndAddException(exceptions, $"Import failed on file {incomingFile.Uri} - Size: {incomingFile.Length} -> Exception details : {exc}");
            }
            catch (Exception exc)
            {
                LogAndAddException(exceptions, $"Import failed on file {incomingFile.Uri} - Size: {incomingFile.Length} -> Exception: {exc.Message} - STACK {exc.StackTrace}.");
            }
        }
    }

    private bool ValidateChecksum(KeyValuePair<string, List<IFile>> fileBatch, List<string> exceptions)
    {
        //validate log file - compare md5 hash with one provided in checksum file
        var logFile = fileBatch.Value.Find(x => x.Extension == Constants.GZ_EXT);

        if (logFile == null)
        {
            LogAndAddException(exceptions, $"Log file unavailable for {fileBatch.Key}.");
            return false;
        }

        var logMd5Hash = UtilsIO.ComputeHashMd5(logFile.Get());

        var checkSumFile = fileBatch.Value.Find(x =>
            x.Extension.Equals(Constants.CHECK_SUM_EXT, StringComparison.CurrentCultureIgnoreCase));

        if (checkSumFile == null)
        {
            LogAndAddException(exceptions, $"Checksum file unavailable for {fileBatch.Key}.");
            return false;
        }

        string checkSumFileContents;
        using (var reader = new StreamReader(checkSumFile.Get()))
        {
            checkSumFileContents = reader.ReadToEnd();
        }
        var firstSpaceIndex = checkSumFileContents.IndexOf(" ", StringComparison.CurrentCultureIgnoreCase);
        var checkSumMd5Hash = checkSumFileContents.Substring(0, firstSpaceIndex);

        var logChecksumIsValid = checkSumMd5Hash == logMd5Hash;

        return logChecksumIsValid;
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
            remoteAccessClient?.Dispose();
        }
    }

    ~DeliveryImportJob()
    {
        Dispose(false);
    }

    public string GetJobCacheKey()
    {
        return DefaultJobCacheKey;
    }

    /// <summary>
    /// Appends information about JobGuid, integrationName and integrationStarDate to the beginning of the [message]
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public string PrefixJobGuidAndIntegrationInfo(string message)
    {
        var PrefixedMessage =
            $"Integration name: {CurrentIntegration.IntegrationName} - " +
            $"Integration start date: {CurrentIntegration.FileStartDate} - " +
            message;

        return PrefixJobGuid(PrefixedMessage);
    }
}
