using Greenhouse.Common;
using Greenhouse.Common.Extensions;
using Greenhouse.Contracts.Messages;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Services;
using MassTransit;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Infrastructure;

public class JobExecutionHandler : IJobExecutionHandler
{
    private readonly ILogger<JobExecutionHandler> _logger;
    private readonly IBus _bus;
    private readonly ILookupService _lookupService;

    public JobExecutionHandler(ILogger<JobExecutionHandler> logger, IBus bus, ILookupService lookupService)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(bus);
        ArgumentNullException.ThrowIfNull(lookupService);

        _logger = logger;
        _bus = bus;
        _lookupService = lookupService;
    }

    public async Task<bool> TryPublishJobExecutionMessage(ExecuteJob executeJob,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(executeJob);

        try
        {
            if (CanHandle(executeJob.ContractKey))
            {
                _logger.LogMessage(LogLevel.Debug, "--> Trying to publish job execution message for ContractKey", executeJob.ContractKey);

                await _bus.Publish(executeJob, cancellationToken).ConfigureAwait(false);

                _logger.LogMessage(LogLevel.Debug, "--> Job execution message successfully published for ContractKey", executeJob.ContractKey);

                return true;
            }

            //_logger.LogMessage(LogLevel.Information, "--> Feature '{featureFlag}' not enabled for the ContractKey", executeJob.ContractKey);
            string message = string.Format("--> Feature '{0}' not enabled for the ContractKey '{1}' ", Constants.FEATURE_ROUTE_JOBS_TO_MESSAGE_BROKER, executeJob.ContractKey);
            _logger.LogMessage(LogLevel.Information, message);

            return false;
        }
        catch (NotImplementedException e)
        {
            _logger.LogMessage(LogLevel.Debug, e, "--> Bus is not configured.");
            return false;
        }
        catch (Exception e)
        {
            _logger.LogMessage(LogLevel.Error, e, "--> Error while publishing message");
            return false;
        }
    }

    public ExecuteJob CreateExecuteJob(JobExecutionDetails jed)
    {
        ArgumentNullException.ThrowIfNull(jed);

        try
        {
            if (jed.Source?.SourceID == null)
            {
                _logger.LogMessage(LogLevel.Warning, "--> Source or SourceID is null for contract key", jed.ContractKey);
                return null;
            }

            if (jed.JobServer?.ServerID == null)
            {
                _logger.LogMessage(LogLevel.Warning, "--> JobServer or ServerID is null for contract key", jed.ContractKey);
                return null;
            }

            int? integrationId = GetIntegrationId(jed);
            if (integrationId == null)
            {
                return null;
            }

            return new ExecuteJob
            {
                ContractKey = jed.ContractKey,
                JobGuid = jed.JobGUID,
                Step = jed.Step,
                SourceId = jed.Source.SourceID,
                IntegrationId = integrationId.Value,
                ServerId = jed.JobServer.ServerID,
                TimeZoneString = GetTimeZoneString(jed)
            };
        }
        catch (Exception ex)
        {
            string message = string.Format("--> Failed to create execute job for contract key '{0}'", jed.ContractKey);
            _logger.LogMessage(LogLevel.Error, ex, message);

            return null;
        }
    }

    private bool CanHandle(string contractKey)
    {
        bool canHandle =
            _lookupService.GetAndDeserializeLookupValueWithDefault(Constants.FEATURE_ROUTE_JOBS_TO_MESSAGE_BROKER,
                new List<string>())?.Any(key => key == contractKey) ?? false;

        if (!canHandle)
        {
            string message = string.Format("--> Contract key '{0}' not configured for message broker routing", contractKey);
            _logger.LogMessage(LogLevel.Debug, message);
        }

        return canHandle;
    }

    private int? GetIntegrationId(JobExecutionDetails jed)
    {
        try
        {
            if (!jed.JobProperties.Contains(Constants.US_INTEGRATION_ID))
            {
                _logger.LogMessage(LogLevel.Warning, "--> Integration ID not found in job properties for contract key", jed.ContractKey);
                return null;
            }

            object integrationIdValue = jed.JobProperties[Constants.US_INTEGRATION_ID];
            if (integrationIdValue != null)
            {
                return Convert.ToInt32(integrationIdValue);
            }

            _logger.LogMessage(LogLevel.Warning, "--> Integration ID is null for contract key", jed.ContractKey);
            return null;
        }
        catch (FormatException ex)
        {
            string message = string.Format("--> Invalid integration ID format for contract key '{0}'", jed.ContractKey);
            _logger.LogMessage(LogLevel.Warning, ex, message);
            return null;
        }
        catch (Exception ex)
        {
            string message = string.Format("--> Error getting integration ID for contract key '{0}'", jed.ContractKey);
            _logger.LogMessage(LogLevel.Warning, ex, message);
            return null;
        }
    }

    private static string GetTimeZoneString(JobExecutionDetails jed)
    {
        return jed.JobProperties[Constants.CP_TIMEZONE_STRING]?.ToString();
    }
}