using Greenhouse.Auth;
using Greenhouse.Data.DataSource.Euromonitor;
using Greenhouse.Data.DataSource.Euromonitor.Requests;
using Greenhouse.Data.DataSource.Euromonitor.Responses;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Mime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Euromonitor
{
    public class EuromonitorService
    {
        private const string _reportExtension = ".json";
        private readonly IHttpClientProvider _httpClientProvider;
        private readonly Credential _greenhouseS3Credential;
        private readonly Credential _credential;
        private readonly ITokenApiClient _tokenApiClient;
        private readonly string _endpointUri;
        private readonly Func<string, DateTime, string, string> _getS3PathHelper;
        private readonly Func<DateTime, string> _getDatedPartition;
        private readonly Action<IFile, S3File, string[], long, bool> _uploadToS3;
        private readonly Action<string[], string> _deleteRawFiles;
        private readonly Action<LogLevel, string> _logMessage;
        private readonly Action<LogLevel, string, Exception> _logException;
        private readonly CancellationToken _cancellationToken;
        private readonly string _apiVersion;
        private readonly string _geographiesToIgnore;
        private readonly int _jobHistoryDays;
        private readonly int _requestsPerHour;
        private readonly int _checkStatusInSeconds;
        private readonly long _batchSize;
        private static readonly CompositeFormat _jobDownloadEndpoint = CompositeFormat.Parse(Endpoints.JobDownload);
        private static readonly CompositeFormat _jobHistoryEndpoint = CompositeFormat.Parse(Endpoints.JobHistory);

        public EuromonitorService(EuromonitorServiceArguments serviceArguments)
        {
            serviceArguments.Validate();

            _httpClientProvider = serviceArguments.HttpClientProvider;
            _greenhouseS3Credential = serviceArguments.GreenhouseS3Credential;
            _credential = serviceArguments.Credential;
            _tokenApiClient = serviceArguments.TokenApiClient;
            _endpointUri = serviceArguments.EndpointUri;
            _getS3PathHelper = serviceArguments.GetS3PathHelper;
            _getDatedPartition = serviceArguments.GetDatedPartition;
            _uploadToS3 = serviceArguments.UploadToS3;
            _deleteRawFiles = serviceArguments.DeleteRawFiles;
            _logMessage = serviceArguments.LogMessage;
            _logException = serviceArguments.LogException;
            _cancellationToken = serviceArguments.CancellationToken;
            _apiVersion = serviceArguments.ApiVersion;
            _geographiesToIgnore = serviceArguments.GeographiesToIgnore;
            _jobHistoryDays = serviceArguments.JobHistoryDays;
            _requestsPerHour = serviceArguments.RequestsPerHour;
            _checkStatusInSeconds = serviceArguments.CheckStatusInSeconds;
            _batchSize = serviceArguments.BatchSize;
        }

        public IEnumerable<string> GenerateCategoryReport(OrderedQueue queueItem, APIReport<ReportSettings> apiReport, Action<Action> cancellableRetry)
        {
            _logMessage(LogLevel.Info, $"{nameof(ReportName.Category)} initialized.");
            var industryCodes = Enumerable.Empty<string>();

            try
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    return Enumerable.Empty<string>();
                }

                var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.Category}".ToLower(), _reportExtension);
                var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
                var uri = string.Join("/", _endpointUri, apiReport.ReportSettings.Endpoint);

                if (reportFile.Exists)
                {
                    using Stream reportFileStream = reportFile.GetAsync().GetAwaiter().GetResult();
                    using (StreamReader streamReader = new(reportFileStream))
                    {
                        string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                        industryCodes = JsonConvert.DeserializeObject<List<Category>>(content).Select(_ => _.IndustryCode).Distinct();
                    }
                }
                else
                {
                    cancellableRetry(() =>
                    {
                        using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                        {
                            Uri = uri,
                            Method = HttpMethod.Get,
                            ContentType = "application/json",
                            Headers = new Dictionary<string, string>() {
                                { "Authorization", $"Bearer {_tokenApiClient.GetAccessTokenAsync(TokenDataSource.Euromonitor).GetAwaiter().GetResult()}" },
                                { "Ocp-Apim-Subscription-Key", _credential.CredentialSet.SubscriptionKey },
                                { "Accept", $"application/json; api-version={_apiVersion}" }
                            }
                        }, _cancellationToken).GetAwaiter().GetResult();

                        if (responseStream.Length <= 0)
                        {
                            return;
                        }

                        using StreamReader streamReader = new(responseStream);
                        string response = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                        industryCodes = JsonConvert.DeserializeObject<List<Category>>(response).Select(_ => _.IndustryCode).Distinct() ?? [];

                        responseStream.Seek(0, SeekOrigin.Begin);
                        StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                        _uploadToS3(incomingFile, reportFile, null, 0, false);
                    });
                }
            }
            catch (HttpClientProviderRequestException ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.Category)} failed.|Http Exception details: {ex}", ex);
                throw;
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.Category)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }

            _logMessage(LogLevel.Info, $"{nameof(ReportName.Category)} finalized.");
            return industryCodes;
        }

        public IEnumerable<ShareType> GenerateShareTypeReport(OrderedQueue queueItem, IEnumerable<string> industryCodes, APIReport<ReportSettings> apiReport, Action<Action> cancellableRetry)
        {
            _logMessage(LogLevel.Info, $"{nameof(ReportName.ShareType)} initialized.");
            var shareTypes = new List<ShareType>();

            try
            {
                var measureTypes = new string[] { nameof(ReportName.CompanyShare), nameof(ReportName.BrandShare) };

                foreach (var measureType in measureTypes)
                {
                    if (_cancellationToken.IsCancellationRequested)
                    {
                        return Enumerable.Empty<ShareType>();
                    }

                    var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.ShareType}-{measureType}".ToLower(), _reportExtension);
                    var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                    var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
                    var uri = string.Join("/", _endpointUri, string.Format(apiReport.ReportSettings.Endpoint, measureType));

                    if (reportFile.Exists)
                    {
                        using Stream reportFileStream = reportFile.GetAsync().GetAwaiter().GetResult();
                        using (StreamReader streamReader = new(reportFileStream))
                        {
                            string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                            var shareType = JsonConvert.DeserializeObject<List<ShareType>>(content);
                            shareType.ForEach(_ => _.Type = measureType);
                            shareTypes.AddRange(shareType);
                        }
                    }
                    else
                    {
                        cancellableRetry(() =>
                        {
                            var shareTypeRequest = new ShareTypeRequest { IndustryCodes = industryCodes.ToArray() };
                            using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                            {
                                Uri = uri,
                                ContentType = "application/json",
                                Method = HttpMethod.Post,
                                Headers = new Dictionary<string, string>() {
                                    { "Authorization", $"Bearer {_tokenApiClient.GetAccessTokenAsync(TokenDataSource.Euromonitor).GetAwaiter().GetResult()}" },
                                    { "Ocp-Apim-Subscription-Key", _credential.CredentialSet.SubscriptionKey },
                                    { "Accept", $"application/json; api-version={_apiVersion}" }
                                },
                                Content = new StringContent(JsonConvert.SerializeObject(shareTypeRequest), Encoding.UTF8, MediaTypeNames.Application.Json)
                            }, _cancellationToken).GetAwaiter().GetResult();

                            if (responseStream.Length <= 0)
                            {
                                return;
                            }

                            using StreamReader streamReader = new(responseStream);
                            string response = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                            var shareTypeResponse = JsonConvert.DeserializeObject<List<ShareType>>(response) ?? [];
                            shareTypeResponse.ForEach(_ => _.Type = measureType);
                            shareTypes.AddRange(shareTypeResponse);

                            responseStream.Seek(0, SeekOrigin.Begin);
                            StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                            _uploadToS3(incomingFile, reportFile, null, 0, false);
                        });
                    }
                };
            }
            catch (HttpClientProviderRequestException ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.ShareType)} failed.|Http Exception details: {ex}", ex);
                throw;
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.ShareType)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }

            _logMessage(LogLevel.Info, $"{nameof(ReportName.ShareType)} finalized.");
            return shareTypes;
        }

        public IEnumerable<int> GenerateGeographyReport(OrderedQueue queueItem, APIReport<ReportSettings> apiReport, Action<Action> cancellableRetry)
        {
            _logMessage(LogLevel.Info, $"{nameof(ReportName.Geography)} initialized.");
            var geographies = Enumerable.Empty<Geography>();

            try
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    return Enumerable.Empty<int>();
                }

                var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.Geography}".ToLower(), _reportExtension);
                var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
                var uri = string.Join("/", _endpointUri, apiReport.ReportSettings.Endpoint);

                if (reportFile.Exists)
                {
                    using Stream reportFileStream = reportFile.GetAsync().GetAwaiter().GetResult();
                    using (StreamReader streamReader = new(reportFileStream))
                    {
                        string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                        geographies = JsonConvert.DeserializeObject<List<Geography>>(content);
                        geographies = geographies.Where(g => !_geographiesToIgnore.Split(',', StringSplitOptions.RemoveEmptyEntries).Any(_ => _ == g.Id.ToString())).ToList();
                    }
                }
                else
                {
                    cancellableRetry(() =>
                    {
                        using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                        {
                            Uri = uri,
                            ContentType = "application/json",
                            Method = HttpMethod.Get,
                            Headers = new Dictionary<string, string>() {
                                { "Authorization", $"Bearer {_tokenApiClient.GetAccessTokenAsync(TokenDataSource.Euromonitor).GetAwaiter().GetResult()}" },
                                { "Ocp-Apim-Subscription-Key", _credential.CredentialSet.SubscriptionKey },
                                { "Accept", $"application/json; api-version={_apiVersion}" }
                            }
                        }, _cancellationToken).GetAwaiter().GetResult();

                        if (responseStream.Length <= 0)
                        {
                            return;
                        }

                        using StreamReader streamReader = new(responseStream);
                        string response = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                        geographies = JsonConvert.DeserializeObject<List<Geography>>(response) ?? [];
                        geographies = geographies.Where(g => !_geographiesToIgnore.Split(',', StringSplitOptions.RemoveEmptyEntries).Any(_ => _ == g.Id.ToString())).ToList();

                        responseStream.Seek(0, SeekOrigin.Begin);
                        StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                        _uploadToS3(incomingFile, reportFile, null, 0, false);
                    });
                }
            }
            catch (HttpClientProviderRequestException ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.Geography)} failed.|Http Exception details: {ex}", ex);
                throw;
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.Geography)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }

            _logMessage(LogLevel.Info, $"{nameof(ReportName.Geography)} finalized.");
            return geographies.Select(g => g.Id);
        }

        public void GenerateDataTypeReport(OrderedQueue queueItem, IEnumerable<string> industryCodes, APIReport<ReportSettings> apiReport, Action<Action> cancellableRetry)
        {
            _logMessage(LogLevel.Info, $"{nameof(ReportName.DataType)} initialized.");

            try
            {
                var measureTypes = new string[] { nameof(ReportName.CompanyShare), nameof(ReportName.BrandShare), nameof(ReportName.MarketSize) };

                foreach (var measureType in measureTypes)
                {
                    if (_cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }

                    var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.DataType}-{measureType}".ToLower(), _reportExtension);
                    var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                    var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
                    var uri = string.Join("/", _endpointUri, string.Format(apiReport.ReportSettings.Endpoint, measureType));

                    if (reportFile.Exists)
                    {
                        continue;
                    }

                    cancellableRetry(() =>
                    {
                        var dataTypeRequest = new DataTypeRequest { IndustryCodes = industryCodes.ToArray() };
                        using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                        {
                            Uri = uri,
                            ContentType = "application/json",
                            Method = HttpMethod.Post,
                            Headers = new Dictionary<string, string>() {
                                { "Authorization", $"Bearer {_tokenApiClient.GetAccessTokenAsync(TokenDataSource.Euromonitor).GetAwaiter().GetResult()}" },
                                { "Ocp-Apim-Subscription-Key", _credential.CredentialSet.SubscriptionKey },
                                { "Accept", $"application/json; api-version={_apiVersion}" }
                            },
                            Content = new StringContent(JsonConvert.SerializeObject(dataTypeRequest), Encoding.UTF8, MediaTypeNames.Application.Json)
                        }, _cancellationToken).GetAwaiter().GetResult();

                        if (responseStream.Length <= 0)
                        {
                            return;
                        }

                        using StreamReader streamReader = new(responseStream);
                        string response = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();

                        responseStream.Seek(0, SeekOrigin.Begin);
                        StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                        _uploadToS3(incomingFile, reportFile, null, 0, false);
                    });
                };
            }
            catch (HttpClientProviderRequestException ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.DataType)} failed.|Http Exception details: {ex}", ex);
                throw;
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.DataType)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }

            _logMessage(LogLevel.Info, $"{nameof(ReportName.DataType)} finalized.");
        }

        public void GenerateMarketSizeReport(OrderedQueue queueItem, IEnumerable<string> industryCodes, IEnumerable<int> geographyIds, APIReport<ReportSettings> apiReport, Action<Action> cancellableRetry)
        {
            _logMessage(LogLevel.Info, $"{nameof(ReportName.MarketSize)} initialized.");

            try
            {
                var jobHistories = GetJobHistory(_jobHistoryDays, cancellableRetry);
                var executionPlans = GetMarketSizeExecutionPlans(queueItem, industryCodes, geographyIds, jobHistories);

                if (!executionPlans.Any())
                {
                    return;
                }

                DateTimeOffset? nextExecution = null;
                cancellableRetry(() =>
                {
                    while (executionPlans.Any(e => e.NotCreated || e.InProgress))
                    {
                        if (_cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }

                        if (DateTimeOffset.Now >= nextExecution.GetValueOrDefault())
                        {
                            nextExecution = DateTimeOffset.Now.AddHours(1);
                            CreateJob(executionPlans, apiReport.ReportSettings.Endpoint, ref nextExecution);
                        }

                        CheckJobStatus(executionPlans, queueItem);

                        if (executionPlans.Any(e => e.NotCreated || e.InProgress))
                        {
                            _logMessage(LogLevel.Info, $"{nameof(ReportName.MarketSize)}. Some reports are still in progress. The process is going to sleep for {_checkStatusInSeconds} seconds.");
                            Task.Delay((int)TimeSpan.FromSeconds(_checkStatusInSeconds).TotalMilliseconds).Wait();
                        }
                    }
                });
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.MarketSize)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }

            _logMessage(LogLevel.Info, $"{nameof(ReportName.MarketSize)} finalized.");
        }

        public void GenerateCompanyShareReport(OrderedQueue queueItem, IEnumerable<string> industryCodes, IEnumerable<int> geographyIds, IEnumerable<ShareType> companyShareTypes, APIReport<ReportSettings> apiReport, Action<Action> cancellableRetry)
        {
            _logMessage(LogLevel.Info, $"{nameof(ReportName.CompanyShare)} initialized.");

            try
            {
                var jobHistories = GetJobHistory(_jobHistoryDays, cancellableRetry);
                var executionPlans = GetExecutionPlans<CompanyShareRequest>(queueItem, industryCodes, geographyIds, companyShareTypes, jobHistories, nameof(ReportName.CompanyShare));

                if (!executionPlans.Any())
                {
                    return;
                }

                DateTimeOffset? nextExecution = null;
                cancellableRetry(() =>
                {
                    while (executionPlans.Any(e => e.NotCreated || e.InProgress))
                    {
                        if (_cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }

                        if (DateTimeOffset.Now >= nextExecution.GetValueOrDefault())
                        {
                            nextExecution = DateTimeOffset.Now.AddHours(1);
                            CreateJob(executionPlans, apiReport.ReportSettings.Endpoint, ref nextExecution);
                        }

                        CheckJobStatus(executionPlans, queueItem);

                        if (executionPlans.Any(e => e.NotCreated || e.InProgress))
                        {
                            _logMessage(LogLevel.Info, $"{nameof(ReportName.CompanyShare)}. Some reports are still in progress. The process is going to sleep for {_checkStatusInSeconds} seconds.");
                            Task.Delay((int)TimeSpan.FromSeconds(_checkStatusInSeconds).TotalMilliseconds).Wait();
                        }
                    }
                });
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.CompanyShare)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }

            _logMessage(LogLevel.Info, $"{nameof(ReportName.CompanyShare)} finalized.");
        }

        public void GenerateBrandShareReport(OrderedQueue queueItem, IEnumerable<string> industryCodes, IEnumerable<int> geographyIds, IEnumerable<ShareType> brandShareTypes, APIReport<ReportSettings> apiReport, Action<Action> cancellableRetry)
        {
            _logMessage(LogLevel.Info, $"{nameof(ReportName.BrandShare)} initialized.");

            try
            {
                var jobHistories = GetJobHistory(_jobHistoryDays, cancellableRetry);
                var executionPlans = GetExecutionPlans<BrandShareRequest>(queueItem, industryCodes, geographyIds, brandShareTypes, jobHistories, nameof(ReportName.BrandShare));

                if (!executionPlans.Any())
                {
                    return;
                }

                DateTimeOffset? nextExecution = null;
                cancellableRetry(() =>
                {
                    while (executionPlans.Any(e => e.NotCreated || e.InProgress))
                    {
                        if (_cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }

                        if (DateTimeOffset.Now >= nextExecution.GetValueOrDefault())
                        {
                            nextExecution = DateTimeOffset.Now.AddHours(1);
                            CreateJob(executionPlans, apiReport.ReportSettings.Endpoint, ref nextExecution);
                        }

                        CheckJobStatus(executionPlans, queueItem);

                        if (executionPlans.Any(e => e.NotCreated || e.InProgress))
                        {
                            _logMessage(LogLevel.Info, $"{nameof(ReportName.BrandShare)}. Some reports are still in progress. The process is going to sleep for {_checkStatusInSeconds} seconds.");
                            Task.Delay((int)TimeSpan.FromSeconds(_checkStatusInSeconds).TotalMilliseconds).Wait();
                        }
                    }
                });
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.BrandShare)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }

            _logMessage(LogLevel.Info, $"{nameof(ReportName.BrandShare)} finalized.");
        }

        private IEnumerable<ExecutionPlan<T>> GetExecutionPlans<T>(OrderedQueue queueItem, IEnumerable<string> industryCodes, IEnumerable<int> geographyIds, IEnumerable<ShareType> shareTypes, IEnumerable<JobHistoryResponse> jobHistories, string reportType) where T : new()
        {
            var executionPlans = new List<ExecutionPlan<T>>();
            var unitTypes = new string[] { nameof(UnitType.Actual), nameof(UnitType.Percentage) };
            var requestData = (from unitType in unitTypes
                               from industryCode in industryCodes
                               from shareType in shareTypes
                               select new { unitType, industryCode, shareType }).ToList();

            try
            {
                foreach (var item in requestData)
                {
                    if (_cancellationToken.IsCancellationRequested)
                    {
                        return Enumerable.Empty<ExecutionPlan<T>>();
                    }

                    var reportName = string.Concat($"{queueItem.FileGUID}-{reportType}-{item.unitType}-{item.industryCode}-{item.shareType.Name}".ToLower()).Replace(" ", "");
                    var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, $"{reportName}-stamped{_reportExtension}");
                    var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);

                    if (reportFile.Exists)
                    {
                        continue;
                    }

                    dynamic request = new T();
                    request.IndustryCode = item.industryCode;
                    request.ShareTypeId = item.shareType.Id;
                    request.GeographyIds = geographyIds.ToArray();
                    request.UnitType = UnitType.GetValue(item.unitType);

                    var executionPlan = new ExecutionPlan<T>(request, reportName, _reportExtension);
                    var jobHistory = jobHistories.FirstOrDefault(j => j.MeasureType == reportType && j.ProcessingStatus != nameof(ProcessingStatus.Failure) && j.ExecutionList.IndustryCode == item.industryCode && j.ExecutionList.ShareTypeId == item.shareType.Id && j.ExecutionList.GeographyIds.SequenceEqual(geographyIds) && j.ExecutionList.UnitType == item.unitType.ToString());

                    if (jobHistory != null)
                    {
                        executionPlan.JobId = jobHistory.JobId;
                        executionPlan.ProcessingStatus = jobHistory.ProcessingStatus;
                        executionPlan.ScheduleJob = jobHistory.LogDateTime;
                    }

                    if (executionPlan.ProcessingStatus != nameof(ProcessingStatus.NoData))
                    {
                        executionPlans.Add(executionPlan);
                    }
                }
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"{typeof(T).Name}. GetExecutionPlans failed. Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }

            return executionPlans;
        }

        private IEnumerable<ExecutionPlan<MarketSizeRequest>> GetMarketSizeExecutionPlans(OrderedQueue queueItem, IEnumerable<string> industryCodes, IEnumerable<int> geographyIds, IEnumerable<JobHistoryResponse> jobHistories)
        {
            var executionPlans = new List<ExecutionPlan<MarketSizeRequest>>();

            try
            {
                foreach (var industryCode in industryCodes)
                {
                    if (_cancellationToken.IsCancellationRequested)
                    {
                        return Enumerable.Empty<ExecutionPlan<MarketSizeRequest>>();
                    }

                    var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.MarketSize}-{industryCode}".ToLower());
                    var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, $"{reportName}-stamped{_reportExtension}");
                    var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);

                    if (reportFile.Exists)
                    {
                        continue;
                    }

                    var executionPlan = new ExecutionPlan<MarketSizeRequest>(new MarketSizeRequest { IndustryCode = industryCode, GeographyIds = geographyIds.ToArray(), }, reportName, _reportExtension);
                    var jobHistory = jobHistories.FirstOrDefault(j => j.MeasureType == nameof(ReportName.MarketSize) && j.ProcessingStatus != nameof(ProcessingStatus.Failure) && j.ExecutionList.IndustryCode == industryCode && j.ExecutionList.GeographyIds.SequenceEqual(geographyIds));

                    if (jobHistory != null)
                    {
                        executionPlan.JobId = jobHistory.JobId;
                        executionPlan.ProcessingStatus = jobHistory.ProcessingStatus;
                        executionPlan.ScheduleJob = jobHistory.LogDateTime;
                    }

                    if (executionPlan.ProcessingStatus != nameof(ProcessingStatus.NoData))
                    {
                        executionPlans.Add(executionPlan);
                    }
                }
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"GetMarketSizeExecutionPlans failed. Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }

            return executionPlans;
        }

        private IEnumerable<JobHistoryResponse> GetJobHistory(int numberOfDays, Action<Action> cancellableRetry)
        {
            _logMessage(LogLevel.Info, $"{nameof(ReportName.JobHistory)} initialized.");
            var jobHistories = Enumerable.Empty<JobHistoryResponse>();

            try
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    return Enumerable.Empty<JobHistoryResponse>();
                }

                cancellableRetry(() =>
                {
                    using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                    {
                        Uri = string.Join("/", _endpointUri, string.Format(null, _jobHistoryEndpoint, numberOfDays)),
                        ContentType = "application/json",
                        Method = HttpMethod.Get,
                        Headers = new Dictionary<string, string>() {
                            { "Authorization", $"Bearer {_tokenApiClient.GetAccessTokenAsync(TokenDataSource.Euromonitor).GetAwaiter().GetResult()}" },
                            { "Ocp-Apim-Subscription-Key", _credential.CredentialSet.SubscriptionKey },
                            { "Accept", $"application/json; api-version={_apiVersion}" }
                        }
                    }, _cancellationToken).GetAwaiter().GetResult();

                    if (responseStream.Length <= 0)
                    {
                        return;
                    }

                    using StreamReader streamReader = new(responseStream);
                    string response = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                    jobHistories = JsonConvert.DeserializeObject<List<JobHistoryResponse>>(response) ?? [];
                    jobHistories.ToList().ForEach(e => e.ExecutionList = JsonConvert.DeserializeObject<ExecutionList>(e.ExecutionListString));
                });
            }
            catch (HttpClientProviderRequestException ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.JobHistory)} failed.|Http Exception details: {ex}", ex);
                throw;
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.JobHistory)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }

            _logMessage(LogLevel.Info, $"{nameof(ReportName.JobHistory)} finalized.");
            return jobHistories;
        }

        private void CreateJob<T>(IEnumerable<ExecutionPlan<T>> executionPlans, string endpoint, ref DateTimeOffset? nextExecution)
        {
            try
            {
                foreach (var executionPlan in executionPlans.Where(e => e.NotCreated).Take(_requestsPerHour))
                {
                    var requestOptions = new HttpRequestOptions
                    {
                        Uri = string.Join("/", _endpointUri, endpoint),
                        ContentType = "application/json",
                        Method = HttpMethod.Post,
                        Headers = new Dictionary<string, string>() {
                            { "Authorization", $"Bearer {_tokenApiClient.GetAccessTokenAsync(TokenDataSource.Euromonitor).GetAwaiter().GetResult()}" },
                            { "Ocp-Apim-Subscription-Key", _credential.CredentialSet.SubscriptionKey },
                            { "Accept", $"application/json; api-version={_apiVersion}" }
                        },
                        Content = new StringContent(JsonConvert.SerializeObject(executionPlan.Request), Encoding.UTF8, MediaTypeNames.Application.Json)
                    };

                    var response = _httpClientProvider.GetResponseAsync(requestOptions, false, _cancellationToken).GetAwaiter().GetResult();
                    if (response.IsSuccessStatusCode)
                    {
                        var passportResponse = JsonConvert.DeserializeObject<CreateJobResponse>(response.Content.ReadAsStringAsync().Result);
                        if (passportResponse != null)
                        {
                            executionPlan.ProcessingStatus = passportResponse.ProcessingStatus;
                            executionPlan.JobId = passportResponse.JobId;
                            executionPlan.ScheduleJob = DateTimeOffset.Now;
                        }
                    }
                    else
                    {
                        if (response.StatusCode == HttpStatusCode.TooManyRequests)
                        {
                            var timeLeftResponse = JsonConvert.DeserializeObject<CreateJobResponse>(response.Content.ReadAsStringAsync().Result);
                            nextExecution = DateTimeOffset.UtcNow.Add(timeLeftResponse.NextQuotaTime);
                            break;
                        }
                        else
                        {
                            throw new HttpClientProviderRequestException(response, response.ReasonPhrase, requestOptions.Content.ReadAsStringAsync().GetAwaiter().GetResult());
                        }
                    }
                }
            }
            catch (HttpClientProviderRequestException ex)
            {
                _logException(LogLevel.Error, $"{typeof(T).Name} CreateJob failed.|Http Exception details: {ex}", ex);
                throw;
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"{typeof(T).Name}. CreateJob failed. Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }
        }

        private void CheckJobStatus<T>(IEnumerable<ExecutionPlan<T>> executionPlans, OrderedQueue queueItem)
        {
            try
            {
                foreach (var executionPlan in executionPlans.Where(e => e.InProgress))
                {
                    var requestOptions = new HttpRequestOptions
                    {
                        Uri = string.Join("/", _endpointUri, string.Format(null, _jobDownloadEndpoint, executionPlan.JobId)),
                        ContentType = "application/json",
                        Method = HttpMethod.Get,
                        Headers = new Dictionary<string, string>() {
                            { "Authorization", $"Bearer {_tokenApiClient.GetAccessTokenAsync(TokenDataSource.Euromonitor).GetAwaiter().GetResult()}" },
                            { "Ocp-Apim-Subscription-Key", _credential.CredentialSet.SubscriptionKey },
                            { "Accept", $"application/json; api-version={_apiVersion}" }
                        }
                    };

                    var response = _httpClientProvider.GetResponseAsync(requestOptions, false, _cancellationToken).GetAwaiter().GetResult();
                    if (response.IsSuccessStatusCode)
                    {
                        var downloadResponse = JsonConvert.DeserializeObject<DownloadJobResponse>(response.Content.ReadAsStringAsync().Result);
                        if (downloadResponse != null)
                        {
                            executionPlan.ProcessingStatus = downloadResponse.Status;
                            executionPlan.DownloadLink = downloadResponse.JobDownloadUri;

                            if (executionPlan.DownloadLink != null)
                            {
                                DownloadJob(executionPlan, queueItem);
                            }
                        }
                    }
                    else
                    {
                        if (response.StatusCode == HttpStatusCode.TooManyRequests)
                        {
                            break;
                        }
                        else
                        {
                            throw new HttpClientProviderRequestException(response, response.ReasonPhrase);
                        }
                    }
                }
            }
            catch (HttpClientProviderRequestException ex)
            {
                _logException(LogLevel.Error, $"{typeof(T).Name} CheckJobStatus failed.|Http Exception details: {ex}", ex);
                throw;
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"{typeof(T).Name}. CheckJobStatus failed. Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }
        }

        private void DownloadJob<T>(ExecutionPlan<T> executionPlan, OrderedQueue queueItem)
        {
            try
            {
                using var headerResponse = _httpClientProvider.GetResponseAsync(new HttpRequestOptions
                {
                    Uri = executionPlan.DownloadLink,
                    ContentType = "application/json",
                    Method = HttpMethod.Head
                }, false, _cancellationToken).GetAwaiter().GetResult();

                var totalFileSize = headerResponse.Content.Headers.ContentLength;
                var shouldSplitFile = totalFileSize > _batchSize;
                var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, $"{executionPlan.ReportName}{(shouldSplitFile ? "" : "-stamped")}{executionPlan.ReportExtension}");
                var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);

                if (!shouldSplitFile)
                {
                    using var responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                    {
                        Uri = executionPlan.DownloadLink,
                        ContentType = "application/json",
                        Method = HttpMethod.Get
                    }, _cancellationToken).GetAwaiter().GetResult();

                    var incomingFile = new StreamFile(responseStream, _greenhouseS3Credential);
                    _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
                }
                else
                {
                    long bytesDownloaded = 0;
                    var fileIndex = 0;
                    var leftoverBytes = Array.Empty<byte>();

                    while (bytesDownloaded < totalFileSize)
                    {
                        var endRange = Math.Min(bytesDownloaded + _batchSize - 1, totalFileSize.GetValueOrDefault() - 1);
                        using var rangeResponse = _httpClientProvider.GetResponseAsync(new HttpRequestOptions
                        {
                            Uri = executionPlan.DownloadLink,
                            ContentType = "application/json",
                            Method = HttpMethod.Get,
                            Headers = new Dictionary<string, string>() { { "Range", $"bytes={bytesDownloaded}-{endRange}" } }
                        }, false, _cancellationToken).GetAwaiter().GetResult();

                        if (rangeResponse.IsSuccessStatusCode)
                        {
                            var validJsonString = GetValidJsonString(rangeResponse.Content.ReadAsStream(_cancellationToken), ref leftoverBytes);
                            UploadJsonBatchToS3(validJsonString, reportFile, fileIndex, leftoverBytes.Length == 0);
                            bytesDownloaded = endRange + 1 - leftoverBytes.Length;
                            fileIndex++;
                        }
                    }
                }

                executionPlan.ProcessingStatus = nameof(ProcessingStatus.Completed);
            }
            catch (HttpClientProviderRequestException ex)
            {
                _logException(LogLevel.Error, $"{typeof(T).Name}. DownloadJob failed.|Http Exception details: {ex}", ex);
                throw;
            }
            catch (Exception ex)
            {
                _deleteRawFiles([queueItem.EntityID.ToLower(), _getDatedPartition(queueItem.FileDate)], executionPlan.ReportName);
                _logException(LogLevel.Error, $"{typeof(T).Name}. DownloadJob failed. Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }
        }

        private static string GetValidJsonString(Stream jsonStream, ref byte[] leftoverBytes)
        {
            using var reader = new StreamReader(jsonStream, Encoding.UTF8);
            var jsonString = reader.ReadToEnd();

            // Find the position of the last valid JSON object
            var lastClosingBrace = jsonString.LastIndexOf('}');
            var lastClosingBracket = jsonString.LastIndexOf(']');
            var lastIndex = Math.Max(lastClosingBrace, lastClosingBracket);
            var validJsonString = jsonString.Substring(0, lastIndex + 1).Replace("}", "}\n");
            var leftoverJsonString = jsonString.Substring(lastIndex + 1).TrimStart(',').Replace("\n", string.Empty);

            if (!validJsonString.StartsWith('['))
            {
                validJsonString = "[" + validJsonString;
            }

            if (!validJsonString.EndsWith(']'))
            {
                validJsonString += "]";
            }

            leftoverBytes = Encoding.UTF8.GetBytes(leftoverJsonString);

            return validJsonString;
        }

        private void UploadJsonBatchToS3(string jsonString, S3File reportFile, int fileIndex, bool stamped)
        {
            var originalFilePath = reportFile.Uri.AbsoluteUri;
            var lastDotIndex = originalFilePath.LastIndexOf('.');
            var suffix = stamped ? $"-stamped" : $"-{fileIndex}";
            var batchFilePath = (lastDotIndex == -1)
                ? $"{originalFilePath}{suffix}"
                : $"{originalFilePath.Substring(0, lastDotIndex)}{suffix}{originalFilePath.Substring(lastDotIndex)}";

            // Create batch file
            var stream = new MemoryStream(Encoding.UTF8.GetBytes(jsonString));
            var batchDestinationFile = new S3File(new Uri(batchFilePath), _greenhouseS3Credential);
            var batchStreamFile = new StreamFile(stream, _greenhouseS3Credential);

            _uploadToS3(batchStreamFile, batchDestinationFile, [batchDestinationFile.FullName], 0, false);
            stream.Dispose();
        }
    }
}
