using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
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

namespace Greenhouse.Jobs.Aggregate.InMarket
{
    [Export("InMarket-AggregateImportJob", typeof(IDragoJob))]
    public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private RemoteAccessClient remoteAccessClient;
        private Dictionary<string, string> dateFormats;
        private Uri baseDestUri;
        private readonly List<Tuple<Guid, Exception>> exceptions = new List<Tuple<Guid, Exception>>();

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            baseDestUri = GetDestinationFolder();
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}")));
            remoteAccessClient = base.GetRemoteAccessClient();
            dateFormats = string.IsNullOrEmpty(Data.Services.JobService.GetById<Lookup>(Constants.INMARKET_DATE_FORMAT)?.Value) ? new Dictionary<string, string>() : ETLProvider.DeserializeType<Dictionary<string, string>>(Data.Services.JobService.GetById<Lookup>(Constants.INMARKET_DATE_FORMAT)?.Value);
        }

        public void Execute()
        {
            var fileLogs = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);

            foreach (var sourceFile in base.SourceFiles)
            {
                var alreadyCreated = fileLogs
                    .Where(x => x.SourceFileName == sourceFile.SourceFileName)
                    .Select(p => p.FileDate)
                    .ToList();

                var reportRegex = new RegexCodec(sourceFile.RegexMask);

                Func<string, bool> RegexTryParse = (fileName) =>
                    dateFormats.TryGetValue(sourceFile.SourceFileName, out string value) ? reportRegex.TryParse(fileName, true, value)
                        : reportRegex.TryParse(fileName);

                //Observe the APIEntity's StartDate and the report waitPeriod
                var whatsMissingByDate = remoteAccessClient.WithDirectory()
                                                    .GetFiles()// get the files from S3
                                                    .GroupBy(f => RegexTryParse(f.Name) ? reportRegex.FileNameDate.Value : (DateTime?)null)// group by date
                                                    .Where(f => f.Key != null //removing files not matching the report regex
                                                                && DateTime.Now.Date.Subtract(f.Key.Value).Days > sourceFile.DeliveryOffsetOverride //only selecting reports that are ready (some reports require a 2 days wait period)
                                                                && !alreadyCreated.Contains(f.Key.Value)) // for a date that has not been imported and processed yet
                                                    .ToDictionary(f => f.Key, f => f.ToList());

                foreach (var date in whatsMissingByDate.Keys)
                {
                    var files = whatsMissingByDate[date];

                    try
                    {
                        var fileCollection = new List<FileCollectionItem>();

                        foreach (var file in files)
                        {
                            string[] paths = new string[] { sourceFile.SourceFileName, GetDatedPartition(date.Value), file.Name };
                            S3File destFile = new S3File(RemoteUri.CombineUri(this.baseDestUri, paths), GreenhouseS3Creds);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Start downloading to S3: {destFile}")));
                            base.UploadToS3(file, destFile, paths);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"End downloading to S3")));
                            fileCollection.Add(new FileCollectionItem
                            {
                                FileSize = file.Length,
                                SourceFileName = sourceFile.SourceFileName,
                                FilePath = destFile.ToString().TrimStart('/')
                            });
                        }

                        var importFile = new Queue()
                        {
                            FileGUID = Guid.NewGuid(),
                            FileName = $"{sourceFile.SourceFileName}_{date.Value.ToString("yyyyMMdd")}",
                            FileSize = fileCollection.Sum(f => f.FileSize),
                            IntegrationID = CurrentIntegration.IntegrationID,
                            SourceID = CurrentSource.SourceID,
                            Status = Constants.JobStatus.Complete.ToString(),
                            StatusId = (int)Constants.JobStatus.Complete,
                            JobLogID = this.JobLogger.JobLog.JobLogID,
                            Step = Constants.ExecutionType.Import.ToString(),
                            DeliveryFileDate = files.Max(f => f.LastWriteTimeUtc),
                            FileDate = date.Value,
                            EntityID = sourceFile.SourceFileName,
                            SourceFileName = sourceFile.SourceFileName
                        };

                        var manifestFiles = ETLProvider.CreateManifestFiles(importFile, fileCollection, baseDestUri, GetDatedPartition);

                        importFile.FileCollectionJSON = JsonConvert.SerializeObject(manifestFiles);

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Start Adding to queue: {JsonConvert.SerializeObject(importFile)}")));
                        Data.Services.JobService.Add<IFileItem>(importFile);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"End Adding to queue")));
                    }
                    catch (Exception exc)
                    {
                        exceptions.Add(System.Tuple.Create<System.Guid, Exception>(base.JED.JobGUID, exc));
                        logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                        base.PrefixJobGuid($"Error Queue not created for Report type = {sourceFile.SourceFileName} for date {date.Value.ToString("yyyyMMdd")}. Exception: {exc.Message} - STACK {exc.StackTrace}"), exc));
                    }
                }
            }

            if (exceptions.Count > 0)
            {
                throw new ErrorsFoundException($"Total errors: {exceptions.Count}; Please check Splunk for more detail.");
            }

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid("Import job complete")));
        }

        protected static Uri GetSourceFolder(string path)
        {
            Uri baseUri = RemoteUri.GetServiceUri(Constants.URI_SCHEME_S3, Greenhouse.Configuration.Settings.Current.AWS.Region);
            return RemoteUri.CombineUri(baseUri, new string[] { path });
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
}
