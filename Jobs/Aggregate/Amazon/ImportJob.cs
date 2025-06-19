using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;

namespace Greenhouse.Jobs.Aggregate.AMC;

[Export("AMC-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private RemoteAccessClient remoteAccessClient;
    private string invalidFileCharacters;

    private Uri baseDestUri;
    private List<Greenhouse.Data.Model.Aggregate.APIReport<Data.DataSource.Amazon.ReportSettings>> reports;
    private readonly List<System.Tuple<System.Guid, Exception>> exceptions = new List<System.Tuple<System.Guid, Exception>>();

    public void PreExecute()
    {
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        baseDestUri = GetDestinationFolder();
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}")));
        remoteAccessClient = base.GetS3RemoteAccessClient();
        reports = Data.Services.JobService.GetAllActiveAPIReports<Data.DataSource.Amazon.ReportSettings>(base.SourceId)?.ToList();
        invalidFileCharacters = LookupService.GetLookupValueWithDefault(Constants.AMC_AGGREGATE_INVALID_FILE_CHARACTERS, defaultValue: ":/\\;|*");
    }

    public void Execute()
    {
        var processedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);
        var entities = Data.Services.JobService.GetAllActiveAPIEntities(CurrentSource.SourceID, CurrentIntegration.IntegrationID);

        Dictionary<string, RegexCodec> lookup = new Dictionary<string, RegexCodec>();

        foreach (var entity in entities)
        {
            //Since it's one integration for all APIEntity, filter to current entity
            var processFiledByEntity = processedFiles.Where(x => x.EntityID == entity.APIEntityCode);
            foreach (var report in reports)
            {
                RegexCodec reportRegex = null;
                if (!lookup.TryGetValue(report.APIReportName, out reportRegex))
                {
                    reportRegex = new RegexCodec(report.ReportSettings.FileNameRegex);
                    lookup.Add(report.APIReportName, reportRegex);
                }

                var src = GetSourceFolder(entity.APIEntityCode.ToLower(), report.ReportSettings.FilePath);

                //Observe the APIEntity's StartDate
                var importFiles = remoteAccessClient.WithDirectory(src)
                                                    .GetFiles()
                                                    .Where(x => (reportRegex.TryParse(x.Name)) && reportRegex.FileNameDate.Value.Subtract(entity.StartDate.Value).Days > -1);

                //Get files that haven't been imported yet
                var whatsMissing = importFiles
                                        .Where(s => !processFiledByEntity.Any(p => p.FileName.Contains(s.Name)))
                                        .OrderBy(p => p.LastWriteTimeUtc)
                                        .ToList();

                foreach (var incomingFile in whatsMissing)
                {
                    var regex = new RegexCodec(report.ReportSettings.FileNameRegex);
                    if (regex.TryParse(incomingFile.Name))
                    {
                        try
                        {
                            string replacedFileName = new string(incomingFile.Name.Select(c => invalidFileCharacters.Contains(c) ? '_' : c).ToArray());
                            var importFile = new Queue()
                            {
                                FileGUID = Guid.NewGuid(),
                                FileName = replacedFileName,
                                FileSize = incomingFile.Length,
                                IntegrationID = CurrentIntegration.IntegrationID,
                                SourceID = CurrentSource.SourceID,
                                Status = Constants.JobStatus.Complete.ToString(),
                                StatusId = (int)Constants.JobStatus.Complete,
                                JobLogID = this.JobLogger.JobLog.JobLogID,
                                Step = JED.Step.ToString(),
                                DeliveryFileDate = incomingFile.LastWriteTimeUtc,
                                FileDate = regex.FileNameDate.Value,
                                EntityID = entity.APIEntityCode,
                                SourceFileName = report.APIReportName
                            };

                            string[] paths = new string[] { importFile.EntityID.ToLower(), GetDatedPartition(importFile.FileDate), importFile.FileName };
                            S3File destFile = new S3File(RemoteUri.CombineUri(this.baseDestUri, paths), GreenhouseS3Creds);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Start downloading to S3: {destFile.ToString()}")));
                            base.UploadToS3(incomingFile, destFile, paths);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"End downloading to S3")));

                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Start Adding to queue: {JsonConvert.SerializeObject(importFile)}")));
                            Data.Services.JobService.Add<IFileItem>(importFile);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"End Adding to queue")));
                        }
                        catch (HttpClientProviderRequestException exc)
                        {
                            HandleException<HttpClientProviderRequestException>(exc, incomingFile.Uri);
                        }
                        catch (Exception exc)
                        {
                            HandleException<HttpClientProviderRequestException>(exc, incomingFile.Uri);
                        }
                    }
                }
            }

            if (exceptions.Count > 0)
            {
                throw new ErrorsFoundException($"Total errors: {exceptions.Count}; Please check Splunk for more detail.");
            }

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid("Import job complete")));
        } //end foreach entity
    }
    private void HandleException<TException>(Exception ex, Uri uri) where TException : Exception
    {
        exceptions.Add(Tuple.Create(JED.JobGUID, ex));

        // Build log message
        string msg = BuildLogMessage(ex, uri);
        logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuid(msg), ex));
    }

    private static string BuildLogMessage<TException>(TException ex, Uri uri) where TException : Exception
    {
        return ex switch
        {
            HttpClientProviderRequestException =>
               $"Error trying to download report: {uri}. Exception details: {ex}",
            _ => $"Error trying to download report: {uri}. Exception: {ex.Message} - STACK {ex.StackTrace}"
        };
    }

    protected static Uri GetSourceFolder(string bucketName, string path)
    {
        Uri baseUri = RemoteUri.GetServiceUri(Constants.URI_SCHEME_S3, Greenhouse.Configuration.Settings.Current.AWS.Region, bucketName);

        string[] paths = new string[] { path.ToLower() };
        return RemoteUri.CombineUri(baseUri, paths);
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

    ~ImportJob()
    {
        Dispose(false);
    }
}
