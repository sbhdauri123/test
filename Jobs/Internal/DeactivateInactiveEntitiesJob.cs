using Greenhouse.Data.Model.APIEntity;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;

namespace Greenhouse.Jobs.Internal
{
    [Export("DeactivateInactiveEntitiesJob", typeof(IDragoJob))]
    public class DeactivateInactiveEntitiesJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        private Action<string> _logInfo;

        private readonly APIEntityStatusQueryRepository _apiEntityStatusQueryRepository = new APIEntityStatusQueryRepository();
        private readonly APIEntityRepository _apiEntityRepository = new APIEntityRepository();
        private readonly AuditLogRepository _auditLogRepository = new AuditLogRepository();

        private IEnumerable<APIEntityStatusQuery> _apiEntityStatusQueries;

        private const string DEFAULT_MONTH_OFFSET_VALUE = "18";
        private const string DEFAULT_DEACTIVATION_RETRY = "6";
        private IBackOffStrategy _backoff;

        private Lookup _offsetLookup;

        public void PreExecute()
        {
            _logInfo = (msg) => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid(msg)));

            var maxRetryLookup = Data.Services.SetupService.GetById<Lookup>(Common.Constants.APIENTITY_DEACTIVATION_RETRY);
            string maxRetry = string.IsNullOrEmpty(maxRetryLookup?.Value) ? DEFAULT_DEACTIVATION_RETRY : maxRetryLookup.Value;

            _offsetLookup = Data.Services.SetupService.GetById<Lookup>(Common.Constants.APIENTITY_STATUS_MONTH_OFFSET);

            _backoff = new BackOffStrategy()
            {
                Counter = 0,
                MaxRetry = Int32.Parse(maxRetry)
            };
        }

        public void Execute()
        {
            RetrieveQueries();
            DeactivateInactiveEntities();
        }

        private void RetrieveQueries()
        {
            _logInfo($"Retrieving Queries.");

            //Retrieve all of the Redshift queries to be run from the Configuration DB
            _apiEntityStatusQueries = _apiEntityStatusQueryRepository.GetQueries();

            string monthOffset = string.IsNullOrEmpty(_offsetLookup?.Value) ? DEFAULT_MONTH_OFFSET_VALUE : _offsetLookup.Value;

            foreach (var query in _apiEntityStatusQueries)
            {
                query.RedshiftQuery = query.RedshiftQuery.Replace("@offset", monthOffset).Replace("@sourceid", query.SourceID);
            }

            _logInfo($"Completed - Retrieving Queries.");
        }

        private void DeactivateInactiveEntities()
        {
            _logInfo($"Deactivating Inactive API Entities.");
            var today = DateTime.Now;

            foreach (var apiEntityStatusQuery in _apiEntityStatusQueries)
            {
                var sourceID = apiEntityStatusQuery.SourceID;

                List<InactiveEntity> inactiveEntities = new List<InactiveEntity>();
                PollyAction(() =>
                {
                    inactiveEntities = RedshiftRepository.ExecuteRedshiftDataReader<InactiveEntity>(apiEntityStatusQuery.RedshiftQuery).ToList();
                }, "QueryRedshiftForInactiveEntities");

                _logInfo($"Completed - Executed Redshift Query: {apiEntityStatusQuery.RedshiftQuery}.");

                inactiveEntities.RemoveAll(x => x.Entity_ID == null);

                if (inactiveEntities.Count == 0)
                {
                    continue;
                }

                //Identify the API Entities that belong to more than one integration
                var duplicateAPIEntityCodes = new List<string>();

                PollyAction(() =>
                {
                    duplicateAPIEntityCodes = _apiEntityRepository.GetDuplicateAPIEntityCodes(sourceID).ToList();
                }, "QueryConfigurationForDuplicateEntities");

                //API Entities belonging to more than one integration will not be deactivated. Remove from the list.
                inactiveEntities.RemoveAll(x => duplicateAPIEntityCodes.Contains(x.Entity_ID));

                if (inactiveEntities.Count != 0)
                {
                    PollyAction(() =>
                    {
                        _apiEntityRepository.DeactivateInactiveAPIEntities(inactiveEntities, sourceID);
                    }, "DeactivateAPIEntities");

                    _logInfo($"Completed - Deactivated Inactive Entities for SourceID: {sourceID}/ SourceName: {apiEntityStatusQuery.SourceName}. Deactivated the following entities: {string.Join(", ", inactiveEntities.Select(x => x.Entity_ID))}");

                    AddToAuditLog(today, sourceID, inactiveEntities);
                }
            }

            _logInfo($"DeactivateInactiveEntitiesJob completed.");
        }

        private void AddToAuditLog(DateTime date, string sourceID, IEnumerable<InactiveEntity> deactivatedEntities)
        {
            var deactivatedInactiveEntitesAudit = new DeactivatedInactiveEntitesAudit
            {
                JobRunDateTime = date.ToString(),
                Entities = deactivatedEntities.Select(x => x.Entity_ID)
            };

            var deactivatedInactiveEntitesAuditJsonString = JsonConvert.SerializeObject(deactivatedInactiveEntitesAudit);

            var auditLog = new AuditLog
            {
                AppComponent = $"Deactivate Inactive Entities Job_{sourceID}",
                Action = "Update",
                AdditionalDetails = deactivatedInactiveEntitesAuditJsonString,
                ModifiedBy = "Deactivate Inactive Entities Job",
                CreatedDate = date
            };

            PollyAction(() =>
            {
                _auditLogRepository.Add(auditLog);
            }, "AddToAuditLog");

            _logInfo($"Completed - Added to Audit Log.");
        }

        public void PollyAction(Action call, string logName)
        {
            GetPollyPolicy<Exception>("DeactivateInactiveEntitiesJob", _backoff)
                .Execute((_) => { call(); },
                    new Dictionary<string, object> { { "methodName", logName } });
        }

        public void PostExecute() { }

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

        ~DeactivateInactiveEntitiesJob()
        {
            Dispose(false);
        }
    }
}
