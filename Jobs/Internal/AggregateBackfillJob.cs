using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.Data.DataSource.DataCertification;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json.Linq;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;

namespace Greenhouse.Jobs.Internal
{
    [Export("DataCertificationBackfillJob", typeof(IDragoJob))]
    public class AggregateBackfillJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        // Guardrail: constant values used only when there is no global lookup set
        private const int DEFAULT_GRACE_PERIOD = 15;
        private const int DEFAULT_MAX_REQUESTS = 100;
        private const int DEFAULT_ROLLING_MONTHS_START = 21;

        private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        private Uri _baseRawDestUri;
        private Uri _baseStageDestUri;
        private Guid _guid;
        private readonly QueueRepository _queueRepository = new();
        private readonly SourceRepository _sourceRepository = new();
        private readonly IntegrationRepository _integrationRepository = new();
        private int _exceptionCounter;
        private int _warningCounter;
        private List<int> _whitelist;

        private Action<LogLevel, string> _log;
        private Action<LogLevel, string, Exception> _logEx;

        public void PreExecute()
        {
            base.Initialize();

            _baseRawDestUri = GetDestinationFolder();
            _baseStageDestUri = new Uri(_baseRawDestUri.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));
            _guid = Guid.NewGuid();

            _log = (logLevel, msg) =>
            {
                _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid($"GUID={_guid}|{msg}")));
                if (logLevel == LogLevel.Warn)
                    _warningCounter++;
                else if (logLevel == LogLevel.Error)
                    _exceptionCounter++;
            };
            _logEx = (logLevel, msg, exc) =>
            {
                _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid($"GUID={_guid}|{msg}"), exc));
                _exceptionCounter++;
            };

            _log(LogLevel.Info, $"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

            CurrentIntegration = SetupService.GetItems<Integration>(new { SourceId = CurrentSource.SourceID }).FirstOrDefault();

            Lookup whitelistLookup = SetupService.GetById<Lookup>(Constants.DATA_CERTIFICATION_WHITELIST_SOURCE_ID);
            var whitelist = new List<string>();
            if (whitelistLookup?.Value != null)
            {
                whitelist = whitelistLookup.Value.Split(',').ToList();
            }

            _whitelist = whitelist.Where(sourceIdString => !string.IsNullOrEmpty(sourceIdString)).Select(int.Parse).ToList();

            _log(LogLevel.Info, $"Pre-execute INFO:whitelist:{string.Join(',', whitelist)}|");
        }

        public void Execute()
        {
            _log(LogLevel.Info, $"EXECUTE START {base.DefaultJobCacheKey}");

            var backfillRequests = GetBackfillRequests();

            if (backfillRequests.Count == 0)
            {
                _log(LogLevel.Info, "No backfill requests to process. Check to make sure your source is whitelisted on DATA_CERTIFICATION_WHITELIST_SOURCE_ID and Alteryx workflow is enabled.");
                return;
            }

            var validatedRequests = ValidateRequestIntegration(backfillRequests);

            var requestsBySource = validatedRequests.GroupBy(grp => grp.SourceID).Select(x => new { sourceId = x.Key, requests = x.ToList() });

            foreach (var sourceRequests in requestsBySource)
            {
                try
                {
                    var source = SetupService.GetById<Source>(sourceRequests.sourceId);
                    if (source == null)
                    {
                        _log(LogLevel.Error, $"Source ID (dti_dataquality.data_certification_aggregated_bf) does not exist in DataLake- Source-ID:{sourceRequests.sourceId}. Skipping total requests:{sourceRequests.requests.Count}");
                        continue;
                    }

                    // "Max-Requests" is a lookup that determines how many backfill requests are allowed at any given time per source
                    var maxRequests = LookupService.GetMaxRequestsForBackfills(source.SourceID, DEFAULT_MAX_REQUESTS);

                    var requestsByEntity = sourceRequests.requests.GroupBy(g => g.EntityID).Select(x => new { entityId = x.Key, requests = x.ToList() });
                    var allApiEntities = JobService.GetAllActiveAPIEntities(source.SourceID);

                    var activeEntityCodes = allApiEntities.Select(a => a.APIEntityCode);

                    // log entities that are disabled
                    var disabledEntityRequests = requestsByEntity.Where(x => !activeEntityCodes.Contains(x.entityId));

                    if (disabledEntityRequests.Any())
                    {
                        var groupByEntity = disabledEntityRequests.Select(group => $"Entity={group.entityId};Total={group.requests.Count}");
                        _log(LogLevel.Warn, $"INACTIVE ENTITY LIST:{string.Join('|', groupByEntity)}|source={source.SourceName}({source.SourceID})");
                    }

                    var newQueues = new List<Data.Model.Core.Queue>();
                    var currentQueues = new List<Data.Model.Core.Queue>();
                    int requestCounter = 0;

                    // "pendingRequests" is a combined list of "new" requests from the Data Certification table and any backfill-queues in Configuration database (that are not in "alreadyQueuedBackfills")
                    // we combine all backfills together in order to sort all requests (new and existing) by api entitty priority order
                    // and stop when we have reached the maximum backfill requests for the source
                    // NOTE: if what is currently backfilled in Queue exceeds the Maximum-Request-Lookup value, then no new backfill requests should be made
                    var (pendingBackfills, alreadyQueuedBackfills) = GetPendingBackfills(source.SourceID, sourceRequests.requests, allApiEntities, maxRequests);

                    // "alreadyQueuedBackfills" is a list of existing queues to be uploaded back to Redshift and have their fileguids recorded in the Data Certification table
                    // And we increase the request-counter by the list count to see if we are still under the maximum allowed backfill requests for this source
                    currentQueues.AddRange(alreadyQueuedBackfills);
                    requestCounter += alreadyQueuedBackfills.Count;

                    foreach (var request in pendingBackfills.OrderByDescending(r => r.EntityPriorityOrder.HasValue).ThenBy(r => r.EntityPriorityOrder).ThenBy(x => x.FileDate))
                    {
                        if (requestCounter >= maxRequests)
                            break;

                        if (request.IsCurrentQueue)
                        {
                            requestCounter++;
                            continue;
                        }

                        Data.Model.Core.Queue queueItem = new()
                        {
                            FileGUID = Guid.NewGuid(),
                            FileSize = 0,
                            FileDate = request.FileDate,
                            SourceFileName = $"{source.SourceName}Reports",
                            FileName = $"{source.SourceName}Reports_{request.FileDate.ToString("yyyyMMdd")}_{request.EntityID}",
                            IntegrationID = request.IntegrationID,
                            SourceID = source.SourceID,
                            Status = Constants.JobStatus.Pending.ToString(),
                            StatusId = (int)Constants.JobStatus.Pending,
                            JobLogID = this.JobLogger.JobLog.JobLogID,
                            Step = Constants.JobStep.Import.ToString(),
                            EntityID = request.EntityID,
                            IsBackfill = true,
                            IsDimOnly = false
                        };

                        newQueues.Add(queueItem);
                        request.BackfillScheduled = true;
                        requestCounter++;

                        _log(LogLevel.Info, $"Queue item ({queueItem.FileGUID}) created in list - SourceID={source.SourceID} - IntegrationID={request.IntegrationID} - EntityID={request.EntityID} - FileDate={request.FileDate.ToString("yyyyMMdd")}");
                    }

                    var remainingRequests = pendingBackfills.Where(x => !x.IsCurrentQueue && !x.BackfillScheduled);

                    if (remainingRequests.Any())
                    {
                        var groupByEntity = remainingRequests.GroupBy(x => x.EntityID).Select(group => $"Entity={group.Key};Total={group.Count()}");
                        _log(LogLevel.Info, $"Unfulfilled requests due to Max-requests(DATA_CERTIFICATION_MAX_REQUESTS) allowed:{string.Join('|', groupByEntity)}|source={source.SourceName}({source.SourceID})|maxRequest={maxRequests}");
                    }

                    if (newQueues.Count > 0)
                    {
                        _queueRepository.BulkInsert(newQueues);
                        _log(LogLevel.Info, $"Backfill Queue items inserted into database - Total:{newQueues.Count} - SourceID={source.SourceID}");
                    }

                    currentQueues.AddRange(newQueues);

                    if (currentQueues.Count == 0)
                        continue;

                    _log(LogLevel.Info, $"Adding Fileguid and updating scheduled-flag for total queue items:{currentQueues.Count} (new queues:{newQueues.Count}) - SourceID={source.SourceID}");
                    UpdateBackfillRequests(source, currentQueues);

                    var dirPath = new string[] { source.SourceID.ToString() };
                    DeleteStageFiles(dirPath, _guid, _guid.ToString());
                }
                catch (Exception exc)
                {
                    _logEx(LogLevel.Error, $"Error - Import failed for SourceID: {sourceRequests.sourceId} -> Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
                }
            }

            if (_exceptionCounter > 0)
            {
                throw new ErrorsFoundException($"Total errors: {_exceptionCounter} and Total warnings: {_warningCounter}");
            }
            else if (_warningCounter > 0)
            {
                JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
                JobLogger.JobLog.Message = "Pending Requests-Search splunk for INACTIVE entity/integration list.";
            }

            _log(LogLevel.Info, "DataCertificationBackfillJob complete");
        }

        /// <summary>
        /// Gets 1) a list of backfills to be used to insert new backfill queues (where "isCurrentQueue" is false)
        /// And also outputs 2) a list of queues that exist already in database that match the request from Redshift Data Certification table
        /// Both these lists will then be uploaded back to Redshift to update the Redshift Data Certification table
        /// by marking the Redshift records as scheduled and listing fileguid
        /// </summary>
        /// <param name="sourceID"></param>
        /// <param name="sourceRequests"></param>
        /// <param name="apiEntities"></param>
        /// <param name="maxRequests"></param>
        /// <returns></returns>
        private (List<BackfillQueue> pendingBackfills, List<Data.Model.Core.Queue> alreadyQueuedBackfills) GetPendingBackfills(int sourceID, List<BackfillQueue> sourceRequests, IEnumerable<Greenhouse.Data.Model.Aggregate.APIEntity> apiEntities, int maxRequests)
        {
            //"pendingBackfills" is a list of backfills to be used to insert new backfill queues
            //"alreadyQueuedBackfills" is a list of queues that exist already in database that match the request from Redshift Data Certification table
            var backfillTuple = (pendingBackfills: new List<BackfillQueue>(), alreadyQueuedBackfills: new List<Data.Model.Core.Queue>());

            var activeEntityCodes = apiEntities.Select(a => a.APIEntityCode);

            // step 1) get new backfill requests (from Redshift-data certification table) that are linked to "Active" api entities
            var activeRequests = sourceRequests.Where(r => activeEntityCodes.Contains(r.EntityID));
            if (!activeRequests.Any())
                return backfillTuple;

            // step 2) get current records in queue in order to avoid scheduling multiple backfills for the same entity and file date
            // NOTE: check queue only (not file log) because the data certification process certifies what is in Redshift, ie what has been processed in filelog as Processed-Complete
            var currentBackfillQueues = _queueRepository.GetAllQueuesDataLight(sourceID).Where(queue => queue.IsBackfill && activeEntityCodes.Contains(queue.EntityID));

            if (!currentBackfillQueues.Any())
            {
                backfillTuple.pendingBackfills.AddRange(activeRequests);
                return backfillTuple;
            }

            bool hasMaxBackfills = false;

            // check for total number of backfills that already exist and if it's more than lookup value - do not create new backfills until all clear or the number of backfills is less than lookup value 
            if (currentBackfillQueues.Count() >= maxRequests)
                hasMaxBackfills = true;

            // step 3) determine if any of the "new" backfill requests from Redshift already have a backfill queue record in the Configuration Database
            // and output this list ("alreadyQueuedBackfills") of queues so that their fileguids can be recorded in the Data Certification table in Redshift
            foreach (var request in activeRequests)
            {
                var existingQueue = currentBackfillQueues.FirstOrDefault(queue => queue.IntegrationID == request.IntegrationID && queue.EntityID == request.EntityID && queue.FileDate == request.FileDate);
                if (existingQueue != null)
                {
                    request.BackfillScheduled = true;
                    backfillTuple.alreadyQueuedBackfills.Add(existingQueue);
                }

                if (!hasMaxBackfills)
                    backfillTuple.pendingBackfills.Add(request);
            }

            if (hasMaxBackfills)
            {
                _log(LogLevel.Warn, $"No new backfills allowed - Total backfills currently in Queue: {currentBackfillQueues.Count()} exceed the Maximum Backfill Requests allowed ({maxRequests}) - sourceID: {sourceID}");
                return backfillTuple;
            }

            // step 4) combine any other existing backfill queue records in the Configuration database (not added to "alreadyQueuedBackfills" which are matches to Data Certification table in Redshift)
            // with "active" pending backfill requests (step 1) in order to loop through and prioritize all backfill requests accordingly
            // NOTE: the flag "IsCurrentQueue" is marked true for these current backfills in order to prevent re-scheduling them and avoiding a duplicate request
            var remainingManualBackfills = currentBackfillQueues.Where(queue => !backfillTuple.alreadyQueuedBackfills.Select(x => x.ID).Contains(queue.ID));

            if (!remainingManualBackfills.Any())
                return backfillTuple;

            foreach (var currentQueue in remainingManualBackfills)
            {
                var existingBackfill = new BackfillQueue { IsCurrentQueue = true, QueueID = currentQueue.ID, Fileguid = currentQueue.FileGUID.ToString(), IntegrationID = currentQueue.IntegrationID, EntityID = currentQueue.EntityID, FileDate = currentQueue.FileDate };

                var apiEntity = apiEntities.FirstOrDefault(entity => entity.APIEntityCode == currentQueue.EntityID);
                if (apiEntity != null)
                    existingBackfill.EntityPriorityOrder = apiEntity.EntityPriorityOrder;
                backfillTuple.pendingBackfills.Add(existingBackfill);
            }

            return backfillTuple;
        }

        /// <summary>
        /// Validate if pending request's integration is disabled or simply does not exist
        /// </summary>
        /// <param name="backfillRequests"></param>
        /// <returns>
        /// List of pending requests with active integrations
        /// </returns>
        private List<BackfillQueue> ValidateRequestIntegration(List<BackfillQueue> backfillRequests)
        {
            List<BackfillQueue> pendingBackfills = new();

            var requestsByIntegration = backfillRequests.GroupBy(grp => (grp.SourceID, grp.IntegrationID)).Select(x => new { sourceId = x.Key.SourceID, integrationId = x.Key.IntegrationID, requests = x.ToList() });

            var allIntegrations = _integrationRepository.GetAllIntegrations();

            foreach (var request in requestsByIntegration)
            {
                var integration = allIntegrations.FirstOrDefault(x => x.SourceID == request.sourceId && x.IntegrationID == request.integrationId);

                if (integration == null)
                {
                    _log(LogLevel.Error, $"MISSING INTEGRATION: Integration ID {request.integrationId} does not exist for source ID: {request.sourceId}|Total requests pending={request.requests.Count}");
                    continue;
                }

                if (integration.IsActive == false)
                {
                    _log(LogLevel.Warn, $"INACTIVE INTEGRATION: Integration ID {request.integrationId} is inactive for source ID: {request.sourceId}|Total requests pending={request.requests.Count}");
                }

                pendingBackfills.AddRange(request.requests);
            }

            return pendingBackfills;
        }

        /// <summary>
        /// Updates dti_dataquality.data_certification_backfill_staging with fileguids from old and new queues
        /// </summary>
        /// <param name="source"></param>
        /// <param name="currentQueues"></param>
        private void UpdateBackfillRequests(Source source, List<Data.Model.Core.Queue> currentQueues)
        {
            string[] sourceDirPath = new string[] { source.SourceID.ToString() };

            DeleteStageFiles(sourceDirPath, _guid);

            var newQueueFileName = $"{_guid}_new_backfill_queues.json";

            string[] paths = new string[]
            {
                source.SourceID.ToString(), newQueueFileName
            };

            var RAC = new RemoteAccessClient(_baseStageDestUri, GreenhouseS3Creds);

            IFile stageFile = RAC.WithFile(RemoteUri.CombineUri(_baseStageDestUri, paths));
            if (stageFile.Exists)
            {
                stageFile.Delete();
            }

            var queuesForUpdate = currentQueues.Select(q => new { q.ID, q.FileGUID, q.SourceID, q.IntegrationID, q.EntityID, q.FileDate, q.Step, q.Status });

            var newQueuesObject = JArray.FromObject(queuesForUpdate);

            ETLProvider.SerializeRedshiftJson(newQueuesObject, stageFile, new UTF8Encoding(false));

            var stageFilePath = System.Net.WebUtility.UrlDecode($"{RemoteUri.CombineUri(_baseStageDestUri, paths)}");
            var odbcParams = base.GetScriptParameters(stageFilePath, _guid.ToString()).ToList();

            odbcParams.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "region", Value = Greenhouse.Configuration.Settings.Current.AWS.Region });
            odbcParams.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "iamrole", Value = Greenhouse.Configuration.Settings.Current.AWS.IamRoleRedshiftCopyS3 });

            DataCertificationRepository.ProcessNewQueues(odbcParams);
        }

        /// <summary>
        /// Gets new backfill requests from Data Certification backfill table in Redshift
        /// </summary>
        /// <returns></returns>
        private List<BackfillQueue> GetBackfillRequests()
        {
            var backfillRequests = new List<BackfillQueue>();

            _log(LogLevel.Info, $"Getting new backfill requests..");

            try
            {
                var sourceIdList = _sourceRepository.GetAllSourceID().Select(s => s.SourceID);

                var allSourceSettings = sourceIdList.Select(sourceID =>
                new
                {
                    source_id = sourceID,
                    grace_period = LookupService.GetGracePeriodForBackfills(sourceID, DEFAULT_GRACE_PERIOD),
                    is_whitelisted = _whitelist.Contains(sourceID),
                    rolling_month_start = LookupService.GetRollingMonthStartForBackfills(sourceID, DEFAULT_ROLLING_MONTHS_START)
                });

                var rac = GetS3RemoteAccessClient();
                var fileName = $"{CurrentSource.SourceName.ToLower()}_source_settings.json";
                IFile sourceSettingsFile = rac.WithFile(Utilities.RemoteUri.CombineUri(_baseStageDestUri, fileName));

                if (sourceSettingsFile.Exists)
                    sourceSettingsFile.Delete();

                ETLProvider.SerializeRedshiftJson(JArray.FromObject(allSourceSettings), sourceSettingsFile);

                _log(LogLevel.Debug, $"Source settings has been created and is available at: {sourceSettingsFile.FullName}. File contains settings for grace period and blacklisting.");

                var stageFilePath = System.Net.WebUtility.UrlDecode($"{RemoteUri.CombineUri(_baseStageDestUri, fileName)}");
                var odbcParams = base.GetScriptParameters(stageFilePath, _guid.ToString()).ToList();

                odbcParams.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "region", Value = Greenhouse.Configuration.Settings.Current.AWS.Region });
                odbcParams.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "iamrole", Value = Greenhouse.Configuration.Settings.Current.AWS.IamRoleRedshiftCopyS3 });

                DataCertificationRepository.LoadSourceSettings(odbcParams);

                _log(LogLevel.Debug, $"Source settings has been loaded into Redshift.");

                var newBackfillRequests = DataCertificationRepository.GetPendingRequests(_guid);
                if (newBackfillRequests.Any())
                    backfillRequests.AddRange(newBackfillRequests);
            }
            catch (Exception exc)
            {
                _logEx(LogLevel.Error, $"Failed to retrieve new Backfill requests -> Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
            }

            return backfillRequests;
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

        ~AggregateBackfillJob()
        {
            Dispose(false);
        }
    }
}
