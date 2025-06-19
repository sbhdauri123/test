using Greenhouse.Common;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Greenhouse.Data.Services
{
    public static class LookupService
    {
        private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private static CompositeFormat reportLookupString = CompositeFormat.Parse(Constants.LOOKUP_SERVICE_REPORT_OPT_IN_NAME_FORMAT);

        public static TimeSpan GetProcessingMaxRuntime(int sourceID)
        {
            TimeSpan maxRuntime;
            if (!TimeSpan.TryParse(SetupService.GetById<Lookup>($"{Constants.PROCESSING_MAX_RUNTIME_SOURCE_PREFIX}{sourceID}")?.Value, out maxRuntime))
            {
                //if not set for that source, use the global value
                if (!TimeSpan.TryParse(SetupService.GetById<Lookup>(Constants.PROCESSING_MAX_RUNTIME_GLOBAL)?.Value, out maxRuntime))
                {
                    //if not set, use default: 1 hour
                    maxRuntime = new TimeSpan(0, 1, 0, 0);
                }
            }

            return maxRuntime;
        }

        public static int GetQueueNBTopResultsForSource(int sourceID)
        {
            int nbTopResult;
            if (int.TryParse(SetupService.GetById<Lookup>($"{Constants.NBTOPRESULT_FOR_SOURCE_PREFIX}{sourceID}")?.Value, out nbTopResult))
            {
                _logger.Log(LogLevel.Info, _logger.Name, $"Lookup value : {Constants.NBTOPRESULT_FOR_SOURCE_PREFIX}{sourceID} found. Number of queues being picked up is {nbTopResult}");
                return nbTopResult;
            }
            else
            {
                nbTopResult = int.Parse(SetupService.GetById<Lookup>(Constants.NBTOPRESULT_FOR_AGGREGATE).Value);
                _logger.Log(LogLevel.Info, _logger.Name, $"Using Aggregate Lookup value : {Constants.NBTOPRESULT_FOR_AGGREGATE} . Number of queues being picked up is {nbTopResult}");
                return nbTopResult;
            }
        }

        public static int GetNbResultsForProcessing(int sourceID)
        {
            int nbTopResult;
            if (int.TryParse(SetupService.GetById<Lookup>($"{Constants.NBTOPRESULT_FOR_PROCESSING_PREFIX}_{sourceID}")?.Value, out nbTopResult))
            {
                _logger.Log(LogLevel.Info, _logger.Name, $"Lookup value : {Constants.NBTOPRESULT_FOR_PROCESSING_PREFIX}_{sourceID} found. Number of queues being picked up is {nbTopResult}");
                return nbTopResult;
            }
            else
            {
                nbTopResult = int.Parse(SetupService.GetById<Lookup>(Constants.NBTOPRESULT_FOR_PROCESSING_PREFIX).Value);
                _logger.Log(LogLevel.Info, _logger.Name, $"Using Aggregate Lookup value : {Constants.NBTOPRESULT_FOR_PROCESSING_PREFIX} . Number of queues being picked up is {nbTopResult}");
                return nbTopResult;
            }
        }

        public static List<string> GetHttpErrorMessages(int sourceID)
        {
            List<string> errors = new List<string>();

            string sourceLookupName = $"{Constants.LOOKUP_SERVICE_HTTP_ERROR_MESSAGES}_{sourceID}";

            Lookup lookupForSource = SetupService.GetById<Lookup>(sourceLookupName);
            if (lookupForSource?.Value != null)
            {
                errors = lookupForSource.Value.Split('|').ToList();
                _logger.Log(LogLevel.Info, _logger.Name, $"HttpErrorMessages Lookup for source-{sourceLookupName} found={string.Join(",", errors)};");
                return errors;
            }

            Lookup globalLookup = SetupService.GetById<Lookup>($"{Constants.LOOKUP_SERVICE_HTTP_ERROR_MESSAGES}");
            if (globalLookup?.Value != null)
            {
                errors = globalLookup.Value.Split('|').ToList();
                _logger.Log(LogLevel.Info, _logger.Name, $"HttpErrorMessages GLOBAL Lookup-{Constants.LOOKUP_SERVICE_HTTP_ERROR_MESSAGES} found={string.Join(",", errors)};");
                return errors;
            }

            return errors;
        }

        public static List<int> GetHttpErrorCodes(int sourceID)
        {
            List<int> errors = new List<int>();

            string sourceLookupName = $"{Constants.LOOKUP_SERVICE_HTTP_ERROR_CODES}_{sourceID}";

            Lookup lookupForSource = SetupService.GetById<Lookup>(sourceLookupName);
            if (lookupForSource?.Value != null)
            {
                // convert lookup string to list of integers
                List<string> codeTextList = lookupForSource.Value.Split(',').ToList();
                errors = codeTextList.ConvertAll(int.Parse);
                _logger.Log(LogLevel.Info, _logger.Name, $"HttpErrorCodes Lookup for source-{sourceLookupName} found={lookupForSource};");
                return errors;
            }

            Lookup globalLookup = SetupService.GetById<Lookup>($"{Constants.LOOKUP_SERVICE_HTTP_ERROR_CODES}");
            if (globalLookup?.Value != null)
            {
                List<string> codeTextList = globalLookup.Value.Split(',').ToList();
                errors = codeTextList.ConvertAll(int.Parse);
                _logger.Log(LogLevel.Info, _logger.Name, $"HttpErrorCodes GLOBAL Lookup-{Constants.LOOKUP_SERVICE_HTTP_ERROR_CODES} found={globalLookup};");
                return errors;
            }

            return errors;
        }

        public static int GetRollingMonthStartForBackfills(int sourceID, int defaultValue)
        {
            if (!int.TryParse(SetupService.GetById<Lookup>($"{Constants.DATA_CERTIFICATION_ROLLING_MONTHS_START_PREFIX}_{sourceID}")?.Value, out int totalMonths))
            {
                if (!int.TryParse(SetupService.GetById<Lookup>($"{Constants.DATA_CERTIFICATION_ROLLING_MONTHS_START_PREFIX}")?.Value, out totalMonths))
                {
                    //if not set, use default
                    totalMonths = defaultValue;
                }
            }

            return totalMonths;
        }

        public static int GetGracePeriodForBackfills(int sourceID, int defaultValue)
        {
            if (!int.TryParse(SetupService.GetById<Lookup>($"{Constants.DATA_CERTIFICATION_GRACE_PERIOD_PREFIX}_{sourceID}")?.Value, out int totalDays))
            {
                if (!int.TryParse(SetupService.GetById<Lookup>($"{Constants.DATA_CERTIFICATION_GRACE_PERIOD_PREFIX}")?.Value, out totalDays))
                {
                    //if not set, use default
                    totalDays = defaultValue;
                }
            }

            return totalDays;
        }

        public static int GetMaxRequestsForBackfills(int sourceID, int defaultValue)
        {
            if (!int.TryParse(SetupService.GetById<Lookup>($"{Constants.DATA_CERTIFICATION_MAX_REQUESTS}_{sourceID}")?.Value, out int totalRequests))
            {
                if (!int.TryParse(SetupService.GetById<Lookup>($"{Constants.DATA_CERTIFICATION_MAX_REQUESTS}")?.Value, out totalRequests))
                {
                    //if not set, use default
                    totalRequests = defaultValue;
                }
            }

            return totalRequests;
        }

        /// <summary>
        /// Checks for any lookups that match format "LOOKUP_SERVICE_REPORT_OPT_IN_SOURCEID_{sourceID}_REPORTID_{apiReport.APIReportID}"
        /// and adds their list of opt-ins to a dictionary using APIReportID as the key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sourceID"></param>
        /// <param name="apiReports"></param>
        /// <returns></returns>
        public static Dictionary<int, List<string>> GetReportOptIns<T>(int sourceID, List<APIReport<T>> apiReports)
        {
            Dictionary<int, List<string>> allOptIns = new Dictionary<int, List<string>>();

            foreach (var report in apiReports)
            {
                string reportLookupName = string.Format(null, reportLookupString, sourceID, report.APIReportID);

                Lookup reportLookup = SetupService.GetById<Lookup>(reportLookupName);
                if (reportLookup?.Value != null)
                {
                    var reportOptIns = reportLookup.Value.Split(',').Where(x => !string.IsNullOrEmpty(x)).ToList();
                    _logger.Log(LogLevel.Info, _logger.Name, $"Report Opt-Ins for Lookup-{reportLookupName} found:{string.Join(",", reportOptIns)};");

                    allOptIns[report.APIReportID] = reportOptIns;
                }
            }

            return allOptIns;
        }

        public static int GetGlobalLookupValueWithDefault(string globalLookupKey, int sourceID, int defaultValue)
        {
            if (int.TryParse(SetupService.GetById<Lookup>($"{globalLookupKey}_{sourceID}")?.Value, out int val))
            {
                return val;
            }
            else if (int.TryParse(SetupService.GetById<Lookup>(globalLookupKey)?.Value, out val))
            {
                return val;
            }

            return defaultValue;
        }

        public static bool GetGlobalLookupValueWithDefault(string globalLookupKey, int sourceID, bool defaultValue)
        {
            if (bool.TryParse(SetupService.GetById<Lookup>($"{globalLookupKey}_{sourceID}")?.Value, out bool val))
            {
                return val;
            }
            else if (bool.TryParse(SetupService.GetById<Lookup>(globalLookupKey)?.Value, out val))
            {
                return val;
            }

            return defaultValue;
        }

        #region GetLookupValueWithDefault
        public static int GetLookupValueWithDefault(string lookupKey, int defaultValue)
        {
            int val;
            if (!int.TryParse(SetupService.GetById<Lookup>(lookupKey)?.Value, out val))
            {
                val = defaultValue;
            }
            return val;
        }

        public static bool GetLookupValueWithDefault(string lookupKey, bool defaultValue)
        {
            bool val;
            if (!bool.TryParse(SetupService.GetById<Lookup>(lookupKey)?.Value, out val))
            {
                val = defaultValue;
            }
            return val;
        }

        public static TimeSpan GetLookupValueWithDefault(string lookupKey, TimeSpan defaultValue)
        {
            TimeSpan val;
            if (!TimeSpan.TryParse(SetupService.GetById<Lookup>(lookupKey)?.Value, out val))
            {
                val = defaultValue;
            }
            return val;
        }

        public static string GetLookupValueWithDefault(string lookupKey, string defaultValue = null)
        {
            Lookup lookup = SetupService.GetById<Lookup>(lookupKey);

            return lookup?.Value ?? defaultValue;
        }

        /// <summary>
        /// Retrieves a json string from the Lookup table and deserializes it
        /// </summary>
        /// <returns>The deserialized json lookup value</returns>
        public static T GetAndDeserializeLookupValueWithDefault<T>(string lookupKey, T defaultValue)
        {
            string reportStateValue = SetupService.GetById<Lookup>(lookupKey)?.Value;
            if (!string.IsNullOrEmpty(reportStateValue))
            {
                return UtilsJson.DeserializeType<T>(reportStateValue);
            }
            else
            {
                return defaultValue;
            }
        }

        public static TimeSpan GetGlobalLookupValueWithDefault(string globalLookupKey, int sourceID, TimeSpan defaultValue)
        {
            if (TimeSpan.TryParse(SetupService.GetById<Lookup>($"{globalLookupKey}_{sourceID}")?.Value, out TimeSpan val))
            {
                return val;
            }
            else if (TimeSpan.TryParse(SetupService.GetById<Lookup>(globalLookupKey)?.Value, out val))
            {
                return val;
            }

            return defaultValue;
        }

        #endregion

        /// <summary>
        /// Saves json object to lookup table after serializing it
        /// </summary>
        /// <param name="lookupKey"></param>
        /// <param name="jsonObject"></param>
        public static void SaveJsonObject(string lookupKey, object jsonObject)
        {
            Lookup lookupValue = SetupService.GetById<Lookup>(lookupKey);

            if (lookupValue != null)
            {
                var newLookupValue = new Lookup
                {
                    Name = lookupKey,
                    Value = UtilsJson.SerializeType(jsonObject)
                };
                SetupService.Update(newLookupValue);
                return;
            }

            SetupService.InsertIntoLookup(lookupKey, UtilsJson.SerializeType(jsonObject));
        }
    }
}
