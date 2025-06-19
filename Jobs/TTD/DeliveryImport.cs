using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.TTD
{
    [Export("TTD-DeliveryImportJob", typeof(IDragoJob))]
    public class DeliveryImport : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        private Action<LogLevel, string> _log;
        private Action<LogLevel, string, Exception> _logEx;
        private RemoteAccessClient RAC;
        private Uri baseDestUri;
        private readonly Stopwatch _runTime = new();
        private TimeSpan _maxRuntime;
        private ParallelOptions _downloadParallelOptions;
        private int _exceptionCount;
        private int _warningCount;
        private sealed record LogPartition(DateTime EventDate, int EventHour, string FileType);

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            _log = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg)));
            _logEx = (logLevel, msg, ex) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex));
            baseDestUri = GetDestinationFolder();
            _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.TTD_DELIVERY_IMPORT_MAX_RUNTIME, new TimeSpan(0, 3, 0, 0));
            int maxConcurrentQueues = LookupService.GetLookupValueWithDefault(Constants.TTD_DELIVERY_IMPORT_MAX_CONCURRENT_QUEUES, 1);
            _downloadParallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxConcurrentQueues };
        }

        public void Execute()
        {
            _runTime.Start();

            RAC = GetRemoteAccessClient();
            var directories = RAC.WithDirectory().GetDirectories().ToList();
            var folderDates = directories.ConvertAll(x => Convert.ToDateTime(x.Name.Split("=".ToCharArray()).Last()));
            var datesToConsider = folderDates.Where(x => x >= base.CurrentIntegration.FileStartDate).OrderBy(x => x);

            var fileLogDatesAndHours = JobService.GetDistinctFileLogDateAndHour(base.CurrentIntegration.IntegrationID).ToList();/*FileDate, FileDateHour, SourceFileName*/
            var fileTypes = GetFileTypes();

            _log(LogLevel.Info, "Getting partitions to import..");
            List<LogPartition> partitionsToImport = GetPartitionsToImport(datesToConsider, fileLogDatesAndHours, fileTypes);
            _log(LogLevel.Info, $"Retrieved partitions to import - total to import: {partitionsToImport.Count}");

            if (partitionsToImport.Count == 0)
            {
                _log(LogLevel.Info, "No trade desk logs to import. Exiting job..");
                return;
            }

            try
            {
                ImportFiles(partitionsToImport);
            }
            catch (TimeoutException)
            {
                _log(LogLevel.Warn, $"Runtime exceeded time allotted - {_runTime.ElapsedMilliseconds}ms");
                _warningCount++;
            }
            catch (Exception ex)
            {
                _logEx(LogLevel.Error, $"TTD ImportFiles exception encountered. Message:{ex.Message} - STACK {ex.StackTrace}", ex);
                _exceptionCount++;
            }

            _runTime.Stop();

            if (_exceptionCount > 0)
            {
                throw new ErrorsFoundException($"Total errors: {_exceptionCount}; Please check Splunk for more detail.");
            }
            else if (_warningCount > 0)
            {
                JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
                JobLogger.JobLog.Message = $"Total warnings: {_warningCount}; For full list search for Warnings in splunk";
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

        ~DeliveryImport()
        {
            Dispose(false);
        }

        /// <summary>
        ///  Downloads partitions concurrently using Parallel.ForEach and lookup TTD_DELIVERY_IMPORT_MAX_CONCURRENT_QUEUES setting the degree of parallelization
        /// </summary>
        private void ImportFiles(IEnumerable<LogPartition> logPartitions)
        {
            ConcurrentQueue<Exception> exceptions = new();

            Parallel.ForEach(logPartitions, _downloadParallelOptions, new Action<LogPartition, ParallelLoopState>((partition, state) =>
            {
                try
                {
                    if (TimeSpan.Compare(_runTime.Elapsed, _maxRuntime) == 1)
                        throw new TimeoutException();

                    DownloadPartitionFiles(partition);
                }
                catch (Exception exc)
                {
                    _logEx(LogLevel.Error, $"Exception in downloading partition files-> date:{partition.EventDate:yyyy-MM-dd}; hour:{partition.EventHour}; " +
                        $"filetype:{partition.FileType}-stopping Parallel.ForEach loop|" +
                        $"Exception:{exc.GetType().FullName}|Message:{exc.Message}|InnerExceptionMessage:{exc.InnerException?.Message}|STACK {exc.StackTrace}", exc);
                    exceptions.Enqueue(exc);
                    state.Stop();
                }
            }));

            if (!exceptions.IsEmpty)
            {
                bool hasTimeoutException = exceptions.Any(exception => exception is TimeoutException);

                if (hasTimeoutException)
                    throw new TimeoutException();

                ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
            }
        }

        /// <summary>
        /// Download all files under a single date-hour partition
        /// </summary>
        private void DownloadPartitionFiles(LogPartition partition)
        {
            _log(LogLevel.Info, $"Downloading Log Files at following partition => date:{partition.EventDate:yyyy-MM-dd}; hour:{partition.EventHour}; filetype:{partition.FileType}");
            var datePartition = GetDatedPartition(partition.EventDate);
            var hourPartition = GetHourPartition(partition.EventHour);
            Uri sourceDateDir = RemoteUri.CombineUri(CurrentIntegration.EndpointURI, datePartition);
            Uri sourceHourDir = RemoteUri.CombineUri(sourceDateDir, hourPartition);

            SourceFile sf = base.SourceFiles.SingleOrDefault(s => s.SourceFileName.Equals(partition.FileType, StringComparison.CurrentCultureIgnoreCase));

            if (sf == null)
            {
                _log(LogLevel.Info, $"FileType: {partition.FileType} skipped because no matching source file found");
                return;
            }

            S3Directory ttdS3Partition = (S3Directory)RAC.WithDirectory(sourceHourDir);

            List<S3File> copiedFiles = ttdS3Partition.CopyFiles(
                fileFilterFunc: (ttdLogFiles) =>
                {
                    var eventTypeFiles = ttdLogFiles.Where(x => x.Name.IndexOf(partition.FileType, StringComparison.InvariantCultureIgnoreCase) > -1).ToList();
                    _log(LogLevel.Debug, $"Total files: {eventTypeFiles.Count} for event type:{partition.FileType} date: {partition.EventDate.ToShortDateString()}, hour: {partition.EventHour}");
                    return eventTypeFiles;
                },
                getDestUriFunc: (incomingFile) =>
                {
                    string entityID = GetEntityID(incomingFile, sf);
                    if (string.IsNullOrEmpty(entityID))
                    {
                        throw new RegexException($"Error parsing filename - {incomingFile.Name} via FileRegexCodec for filetype: {partition.FileType}");
                    }

                    //basebucket/raw/ttd-delivery/entityid/date/fileType
                    string[] paths = new string[] { entityID.ToLower(), datePartition, hourPartition, partition.FileType, incomingFile.Name };
                    return RemoteUri.CombineUri(this.baseDestUri, paths);
                });

            if (copiedFiles.Count == 0)
            {
                _log(LogLevel.Debug, $"No Files Copied for date:{partition.EventDate:yyyy-MM-dd}; hour:{partition.EventHour}; filetype:{partition.FileType}");
                return;
            }

            string queueEntityID = GetEntityID(copiedFiles[0], sf);

            string[] paths = new string[] { queueEntityID.ToLower(), GetDatedPartition(partition.EventDate), GetHourPartition(partition.EventHour), partition.FileType };
            var importedFiles = new Data.Model.Core.Queue()
            {
                FileGUID = Guid.NewGuid(),
                EntityID = queueEntityID,
                FileName = String.Join("/", paths),
                FileCollectionJSON = String.Empty,
                IntegrationID = CurrentIntegration.IntegrationID,
                SourceID = CurrentSource.SourceID,
                Status = Constants.JobStatus.Complete.ToString(),
                StatusId = (int)Constants.JobStatus.Complete,
                JobLogID = this.JobLogger.JobLog.JobLogID,
                Step = JED.Step.ToString(),
                SourceFileName = partition.FileType,
                FileDate = partition.EventDate,
                FileDateHour = partition.EventHour,
                FileSize = copiedFiles.Sum(f => f.Length),
                DeliveryFileDate = copiedFiles.Max(f => f.LastWriteTimeUtc)
            };
            JobService.Add(importedFiles);
            _log(LogLevel.Debug, $"Successfully queued TTD {partition.FileType} delivery files(TOTAL:{copiedFiles.Count}) - {JsonConvert.SerializeObject(importedFiles)}");
        }

        /// <summary>
        /// Returns entity ID after applying regex codec to file name
        /// </summary>
        private static string GetEntityID(S3File incomingFile, SourceFile sf)
        {
            string entityID = string.Empty;
            if (sf.FileRegexCodec.TryParse(incomingFile.Name))
            {
                entityID = sf.FileRegexCodec.EntityId;
            }

            return entityID;
        }

        /// <summary>
        /// Returns a list of hour partitions that need to be downloaded
        /// </summary>
        private List<LogPartition> GetPartitionsToImport(IOrderedEnumerable<DateTime> datesToConsider, List<IFileItem> fileLogDatesAndHours, List<string> fileTypes)
        {
            List<LogPartition> partitionsToImport = new();
            foreach (var date in datesToConsider)
            {
                foreach (var fileType in fileTypes)
                {
                    var hoursFromDb = fileLogDatesAndHours.Where(x => x.FileDate == date && x.SourceFileName == fileType).Select(x => x.FileDateHour.Value).OrderBy(x => x);
                    var hoursFromS3 = GetHoursFromS3(date);
                    var hourFoldersNotImported = hoursFromS3.Except(hoursFromDb).ToList();
                    //continue with next date if all hours have been imported for that date.
                    if (hourFoldersNotImported.Count == 0) continue;

                    hourFoldersNotImported.ForEach(hour =>
                    {
                        if (ShouldImportFilesForHour(fileType, hour, date))
                        {
                            LogPartition logPartition = new(date, hour, fileType);
                            partitionsToImport.Add(logPartition);
                        }
                    });
                }
            }

            return partitionsToImport;
        }

        /// <summary>
        /// Only pick up files from the Hour-2 folder.
        /// That is to say, if it is 8:30 AM when the import job runs and there is a folder there for 6 AM it should only process that folder if the 7 AM folder also exists.
        /// However, for video event files we are picking up Hour-3 folder. Else, when the processing job picks up the most recent video event file from Q it assigns an EMR cluster
        /// for file processing. The processing code then looks for the following hour's impression file. Since there will be no impression file for the hour+1 folder the job does nothing
        /// and holds up the EMRCluster from being assigned for any other file processing in the Q till the impression files for the following hour show up.
        /// </summary>
        private bool ShouldImportFilesForHour(string fileType, int hour, DateTime dateToImport)
        {
            var currentDate = DateTime.UtcNow;
            var dateToCompare = new DateTime(dateToImport.Year, dateToImport.Month, dateToImport.Day, hour, 0, 0);
            TimeSpan timeSpanImportFileDateAndNow = currentDate - dateToCompare;
            var timeSpanTotalHours = timeSpanImportFileDateAndNow.TotalHours;
            if (fileType.Equals(Constants.VIDEO_EVENT, StringComparison.InvariantCultureIgnoreCase) && (timeSpanTotalHours > 7))
            {
                _log(LogLevel.Debug, $"Importing video event file for hour - {hour}");
                return true;
            }
            else if (!fileType.Equals(Constants.VIDEO_EVENT, StringComparison.InvariantCultureIgnoreCase) && timeSpanTotalHours > 6)
            {
                return true;
            }
            _log(LogLevel.Debug, $"Skipping import for {fileType} file for hour - {hour}");
            return false;
        }

        private List<int> GetHoursFromS3(DateTime date)
        {
            //Get hour folders in this date directory
            Uri sourceDateDir = RemoteUri.CombineUri(CurrentIntegration.EndpointURI, String.Format("date={0}", date.ToString("yyyy-MM-dd")));
            var hourFoldersInS3 = RAC.WithDirectory(sourceDateDir).GetDirectories().ToList();
            return hourFoldersInS3.Select(x => Convert.ToInt32(x.Name.Split("=".ToCharArray()).Last())).OrderBy(x => x).ToList();
        }

        /// <summary>
        ///Returns the dbo.SourceFile.SourceFileName
        ///Hive table names are singular,but the filetype in TTD are plural eg. clicks, videoevents etc.
        /// </summary>
        private List<string> GetFileTypes()
        {
            return base.SourceFiles.Select(x => x.SourceFileName).OrderBy(x => x).ToList();
        }
    }
}