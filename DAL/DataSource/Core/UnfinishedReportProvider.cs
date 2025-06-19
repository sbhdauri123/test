using Greenhouse.Data.Model.Core;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.DAL.DataSource.Core
{
    public class UnfinishedReportProvider<T> : BaseReportProvider<T>
    {
        private const string UNFINISHED_REPORT_DIRECTORY = "unfinished_reports";
        private const string UNFINISHED_REPORT_SUFFIX = "unfinished_report";
        private const string UNFINISHED_REPORT_EXTENSION = ".json";
        private const char FIELD_DELIMITER = '_';
        public string FileGuid { get; set; }
        public Action<LogLevel, string> Logger { get; set; }
        public Action<LogLevel, string, Exception> ExceptionLogger { get; set; }

        public UnfinishedReportProvider(Uri destUri, Action<LogLevel, string> logger, Action<LogLevel, string, Exception> exceptionLogger)
        {
            BaseRawUri = destUri;
            Logger = logger;
            ExceptionLogger = exceptionLogger;
            ReportDirectory = UNFINISHED_REPORT_DIRECTORY;
        }

        public Uri BaseRawUri { get; set; }
        public override Uri UriPath => this.BaseRawUri;

        public override string GetReportName()
        {
            return $"{FileGuid.ToLower()}{FIELD_DELIMITER}{UNFINISHED_REPORT_SUFFIX}{UNFINISHED_REPORT_EXTENSION}";
        }

        public List<string> GetGuidListing(Uri sourceUri)
        {
            List<string> guidList = new();

            var RAC = new RemoteAccessClient(sourceUri, GreenhouseAWSCredential);

            var path = new string[] { UNFINISHED_REPORT_DIRECTORY };

            Uri unfinishedReportUri = RemoteUri.CombineUri(sourceUri, path);

            var dir = RAC.WithDirectory(unfinishedReportUri);

            if (!dir.Exists)
                return guidList;

            var files = dir.GetFiles();

            foreach (var unfinishedReport in files)
            {
                if (unfinishedReport.Extension == ".json")
                {
                    string guidPrefix = unfinishedReport.Name.Split('_')[0];
                    guidList.Add(guidPrefix);
                }
            }

            return guidList;
        }

        public void CleanupReports(Uri sourceUri, IEnumerable<Guid> activeGuidListing)
        {
            var guidListing = GetGuidListing(sourceUri);

            if (guidListing.Count == 0)
                return;

            var currentGuidStringList = activeGuidListing.Select(fileGuid => fileGuid.ToString().ToLower());

            var inactiveGuids = guidListing.Where(guid => !currentGuidStringList.Contains(guid)).ToList();

            if (inactiveGuids.Count == 0)
                return;

            inactiveGuids.ForEach(guid =>
            {
                DeleteReport(guid);
            });
        }

        public void SaveReport<U>(string fileGuid, U reportContents)
        {
            FileGuid = fileGuid;

            SaveReport(reportContents);

            Logger(LogLevel.Info, $"SaveReport: Saved file {GetReportName()}");
        }

        public void SaveReport(string fileGuid, T reportContents)
        {
            FileGuid = fileGuid;

            SaveReport(reportContents);

            Logger(LogLevel.Info, $"SaveReport: Saved file {GetReportName()}");
        }

        public void DeleteReport(string fileGuid)
        {
            FileGuid = fileGuid;

            try
            {
                Logger(LogLevel.Info, $"DeleteReport: Deleting file {GetReportName()}");
                // when multiple integration of a same source run at the same time, it could happen that a file is deleted
                // before another integration reach this line (race condition), instead of introducing another locking mechanism
                // we fail gracefully
                DeleteReport();
            }
            catch (Exception ex)
            {
                ExceptionLogger(LogLevel.Error, $"Error while deleting file {GetReportName()}", ex);
            }
        }

        public IEnumerable<T> GetReports(string fileGuid)
        {
            FileGuid = fileGuid;

            return GetReports();
        }

        public T GetReport(string fileGuid)
        {
            FileGuid = fileGuid;

            return GetReport();
        }

        public List<T> LoadUnfinishedReportsFile(IEnumerable<IFileItem> queues)
        {
            var unfinishedReports = new List<T>();

            foreach (var queueItem in queues)
            {
                var reports = GetReports(queueItem.FileGUID.ToString());
                if (reports != null) { unfinishedReports.AddRange(reports); }
            }

            return unfinishedReports;
        }
    }
}
