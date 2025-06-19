using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;

namespace Greenhouse.Jobs.Internal
{
    [Export("AggDataStatusRefresh", typeof(IDragoJob))]
    public class AggDataStatusJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        private readonly DataAvailabilityConfigurationRepository _dataAvailabilityConfigurationRepository = new DataAvailabilityConfigurationRepository();
        private readonly SourceRepository _sourceRepository = new SourceRepository();
        private readonly AggDataStatusRepository _aggDataStatusRepository = new AggDataStatusRepository();
        private Action<string> _logInfo;
        private Action<string> _logWarning;
        private IBackOffStrategy _backoff;
        private string _getProcessedAdvertisersQuery;
        private string _cleanAdvertiserDataDateQuery;
        private string _updateAggDateReportQuery;

        public void PreExecute()
        {
            _logInfo = (msg) => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid(msg)));
            _logWarning = (msg) => _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, PrefixJobGuid(msg)));

            _backoff = new ExponentialBackOffStrategy()
            {
                Counter = 0,
                MaxRetry = 6
            };

            _getProcessedAdvertisersQuery = Data.Services.SetupService.GetById<Lookup>(Constants.LOOKUP_AGGDATASTATUS_REDSHIFTQUERY).Value;
            _cleanAdvertiserDataDateQuery = Data.Services.SetupService.GetById<Lookup>(Constants.LOOKUP_CLEANAGGDATADATE_REDSHIFTQUERY).Value;
            _updateAggDateReportQuery = Data.Services.SetupService.GetById<Lookup>(Constants.LOOKUP_UPDATEAGGDATEREPORT_REDSHIFTQUERY).Value;
        }

        public void PollyAction(Action call, string logName)
        {
            GetPollyPolicy<Exception>("AggDataStatusJob", _backoff)
                .Execute((_) => { call(); },
                    new Dictionary<string, object> { { "methodName", logName } });
        }

        public T PollyFunc<T>(Func<Polly.Context, T> call, string logName)
        {
            var getPolicy = GetPollyPolicy<Exception>("AggDataStatusJob", _backoff);
            T results = getPolicy.Execute(call,
                new Dictionary<string, object> { { "methodName", "logName" } });
            return results;
        }

        public void Execute()
        {
            //Aggregate
            UpdateAggDataTables();
        }

        private void UpdateAggDataTables()
        {
            //a placeholder is a record in AggDataStatus for a FileLog that is not Process Complete yet
            //it is an assumption that an advertiser will be part of the file(s) containing delivery data based on its past import in redshift
            //Once a FileLog is Process Complete, the placeholders are updated with the final data
            //some will be updated from IsPlaceholder= 1 to 0 (if there are part of the file loaded in redshift)
            //the others will be deleted if they are no part of the file(s) loaded in redshift

            //  Yes                  |       No                  :   Final data - the advertisers loaded in redshift are associated with the fileLog on FileGUID 

            // Steps:
            // 1 - Getting list of SourceID + FileGUID from FileLogs Processing Complete that need to be added to AggDataStatus or has been updated since last run
            // 2 - Get List of advertisers from Redshift matching the FileGUIDs + List of all advertisers with latest Fileguid to build Placeholders
            // 4 - Create Placeholders in stage table (FileLog not Processing Complete)
            // 5 - Create final records in stage table (FileLog Processing Complete)
            // 6 - Insert / Update / Delete AggDataStatus with stage table

            _logInfo("Start of UpdateAggDataStatus");

            InitializeDataSets(
                out IEnumerable<SourceInfo> allSourceInfo,
                out IEnumerable<DataAvailabilityConfiguration> configurations);

            // for each active configuration in DataAvailabilityConfiguration (1 per source)
            // we query Redshift to retrieve all the AdvertiserIds for that source and move them to dti.advertiser_data_date_export_temp
            // for each advertiser we retrieve the Max Data Date from the latest fileLog processed
            // and their associated FileGUID >> Used for placeholders
            // + the list of Advertisers for the FileGUIDs specified >> used to insert/update any record that is now Processing Complete (non Placeholder)

            // 1 - Getting the Data from RS
            foreach (var config in configurations)
            {
                if (string.IsNullOrEmpty(config.RedshiftSchema))
                {
                    _logWarning($"Skipping config.SourceID={config.SourceID} because config.RedshiftSchema doesn't have a value");
                    continue;
                }

                _logInfo("Start Aggregate for config.SourceID=" + config.SourceID);

                var sourceInfo = allSourceInfo.FirstOrDefault(s => s.SourceID == config.SourceID);
                if (sourceInfo == null)
                {
                    _logWarning($@"Source ID={config.SourceID} was not returned by SP GetAllSourceInfo and will be ignored");
                    continue;
                }

                // Copy advertiser data to dti.advertiser_data_date_export_temp
                GetAdvertisersFromRedshift(config);

                // Call SP UpdateAggDataStatus
                _aggDataStatusRepository.UpdateAggDataTables(config.SourceID, this.JED.JobGUID.ToString());

                // Call Redshift SP UpdateAggDateReport
                CallUpdateAggDateReport(config);

                //execute a query to move the data to a log table
                //an aggregate etl will use that table to remove that data from <source>.advertiser_data_date

                _logInfo($"Running clean advertiser data date query for SourceID = {config.SourceID}");
                PollyAction(() =>
                {
                    RedshiftRepository.ExecuteRedshiftCommand(_cleanAdvertiserDataDateQuery);
                }, "CleanAdvertiserDataDate");

                _logInfo("End Aggregate Update for config.SourceID=" + config.SourceID);
            }

            _logInfo("End of AggDataStatus Update");
        }

        //copy the list of advertisers from redshit into a redshift table
        private void GetAdvertisersFromRedshift(DataAvailabilityConfiguration config)
        {
            string redshiftTableName = string.IsNullOrEmpty(config.RedshiftTable) ? "advertiser_data_date" : config.RedshiftTable;

            string query = _getProcessedAdvertisersQuery.Replace("@SourceSchema", config.RedshiftSchema)
                            .Replace("@SourceTable", redshiftTableName);

            _logInfo($"Running Redshift query for SourceID = {config.SourceID}: {query}");

            PollyAction(() =>
            {
                _logInfo("Retrieving Data availability from Redshift");
                RedshiftRepository.ExecuteRedshiftCommand(query, 300);
            }, "GetFileLogStatus");
        }

        private void CallUpdateAggDateReport(DataAvailabilityConfiguration config)
        {
            string query = _updateAggDateReportQuery.Replace("@SourceID", config.SourceID.ToString());

            _logInfo($"Running update-AggDateReport stored procedure for for SourceID = {config.SourceID}: {query}");

            PollyAction(() =>
            {
                RedshiftRepository.ExecuteRedshiftCommand(query, 300);
            }, "UpdateAggDateReport");
        }

        private void InitializeDataSets(
            out IEnumerable<SourceInfo> allSources,
            out IEnumerable<DataAvailabilityConfiguration> configurations)
        {
            //retrieve all the DactaSources and sources in one call
            allSources = _sourceRepository.GetAllSourceInfo();

            configurations = _dataAvailabilityConfigurationRepository
                .GetItems(new { IsActive = true });

            _logInfo($"Completed - InitializeDataSets");
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

        ~AggDataStatusJob()
        {
            Dispose(false);
        }
    }
}
