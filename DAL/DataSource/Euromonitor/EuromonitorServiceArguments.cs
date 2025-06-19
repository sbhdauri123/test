using Greenhouse.Auth;
using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Greenhouse.DAL.DataSource.Euromonitor
{
    public class EuromonitorServiceArguments
    {
        public IHttpClientProvider HttpClientProvider { get; }
        public Credential GreenhouseS3Credential { get; }
        public Credential Credential { get; }
        public ITokenApiClient TokenApiClient { get; }
        public string EndpointUri { get; }
        public Func<string, DateTime, string, string> GetS3PathHelper { get; }
        public Func<DateTime, string> GetDatedPartition { get; }
        public Action<IFile, S3File, string[], long, bool> UploadToS3 { get; }
        public Action<string[], string> DeleteRawFiles { get; }
        public Action<LogLevel, string> LogMessage { get; }
        public Action<LogLevel, string, Exception> LogException { get; }
        public CancellationToken CancellationToken { get; }

        public readonly string ApiVersion;
        public readonly string GeographiesToIgnore;
        public readonly int JobHistoryDays;
        public readonly int RequestsPerHour;
        public readonly int CheckStatusInSeconds;
        public readonly long BatchSize;

        public EuromonitorServiceArguments(
            IHttpClientProvider httpClientProvider,
            Credential greenhouseS3Credential,
            Credential credential,
            ITokenApiClient tokenApiClient,
            string endpointUri,
            Func<string, DateTime, string, string> getS3PathHelper,
            Func<DateTime, string> getDatedPartition,
            Action<IFile, S3File, string[], long, bool> uploadToS3,
            Action<string[], string> deleteRawFiles,
            Action<LogLevel, string> logMessage,
            Action<LogLevel, string, Exception> logException,
            CancellationToken cancellationToken)
        {
            HttpClientProvider = httpClientProvider;
            GreenhouseS3Credential = greenhouseS3Credential;
            Credential = credential;
            TokenApiClient = tokenApiClient;
            EndpointUri = endpointUri;
            GetS3PathHelper = getS3PathHelper;
            GetDatedPartition = getDatedPartition;
            UploadToS3 = uploadToS3;
            DeleteRawFiles = deleteRawFiles;
            LogMessage = logMessage;
            LogException = logException;
            CancellationToken = cancellationToken;

            ApiVersion = LookupService.GetAndDeserializeLookupValueWithDefault(Constants.EUROMONITOR_API_VERSION, "0.1");
            GeographiesToIgnore = LookupService.GetLookupValueWithDefault(Constants.EUROMONITOR_GEOGRAPHIES_TO_IGNORE, "");
            JobHistoryDays = LookupService.GetLookupValueWithDefault(Constants.EUROMONITOR_JOB_HISTORY_DAYS, 1);
            RequestsPerHour = LookupService.GetLookupValueWithDefault(Constants.EUROMONITOR_REQUESTS_PER_HOUR, 20);
            CheckStatusInSeconds = LookupService.GetLookupValueWithDefault(Constants.EUROMONITOR_CHECK_JOB_STATUS_SECONDS, 180);
            BatchSize = LookupService.GetLookupValueWithDefault(Constants.EUROMONITOR_BATCH_SIZE_MB, 100) * 1024 * 1024;
        }

        public void Validate()
        {
            ArgumentNullException.ThrowIfNull(HttpClientProvider);
            ArgumentNullException.ThrowIfNull(GreenhouseS3Credential);
            ArgumentNullException.ThrowIfNull(Credential);
            ArgumentNullException.ThrowIfNull(TokenApiClient);

            if (string.IsNullOrWhiteSpace(EndpointUri))
            {
                throw new ArgumentException("EndpointUri cannot be empty or whitespace.", nameof(EndpointUri));
            }

            if (!Uri.TryCreate(EndpointUri, UriKind.Absolute, out _))
            {
                throw new UriFormatException($"EndpointUri '{EndpointUri}' is not a valid absolute URI.");
            }
        }
    }
}
