using Greenhouse.Data.DataSource.Lotame;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Lotame;

public class LotameService
{
    private const string _reportExtension = ".json";
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly Credential _credential;
    private readonly Credential _greenhouseS3Credential;
    private readonly Integration _integration;
    private readonly Func<string, DateTime, string, string> _getS3PathHelper;
    private readonly int _maxDegreeOfParallelism;
    private readonly int _pageSize;
    private readonly Action<IFile, S3File, string[], long, bool> _uploadToS3;
    private readonly Action<LogLevel, string> _logMessage;
    private readonly Action<LogLevel, string, Exception> _logException;
    private readonly Dictionary<string, string> _authenticationHeader;
    private readonly CancellationToken _cancellationToken;

    public LotameService(LotameServiceArguments serviceArguments)
    {
        _httpClientProvider = serviceArguments.HttpClientProvider;
        _credential = serviceArguments.Credential;
        _greenhouseS3Credential = serviceArguments.GreenhouseS3Credential;
        _integration = serviceArguments.Integration;
        _getS3PathHelper = serviceArguments.GetS3PathHelper;
        _maxDegreeOfParallelism = serviceArguments.MaxDegreeOfParallelism;
        _pageSize = serviceArguments.PageSize;
        _uploadToS3 = serviceArguments.UploadToS3;
        _logMessage = serviceArguments.LogMessage;
        _logException = serviceArguments.LogException;
        _cancellationToken = serviceArguments.cancellationToken;
        _authenticationHeader = new Dictionary<string, string>() { { "x-lotame-token", _credential.CredentialSet.lotameToken }, { "x-lotame-access", _credential.CredentialSet.lotameAccess } };
    }

    public List<Hierarchy> GenerateHierarchyListReport(OrderedQueue queueItem, APIReport<ReportSettings> apiReport, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.HierarchyList)} initialized.");

        var hierarchyIds = new List<Hierarchy>();
        var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.HierarchyList}-0".ToLower(), _reportExtension);
        var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
        var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);

        if (reportFile.Exists)
        {
            using Stream reportFileStream = reportFile.GetAsync().GetAwaiter().GetResult();
            using (StreamReader streamReader = new(reportFileStream))
            {
                string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                hierarchyIds = JsonConvert.DeserializeObject<HierarchyResponse>(content)
                    ?.Hierarchies.ToList() ?? [];
            }

            _logMessage(LogLevel.Info, $"{nameof(ReportName.HierarchyList)} finalized. Report has already been collected");
            return hierarchyIds;
        }

        var uri = string.Join("/", _integration.EndpointURI, string.Format(apiReport.ReportSettings.Endpoint, queueItem.EntityID));

        try
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                return Enumerable.Empty<Hierarchy>().ToList();
            }

            cancellableRetry(() =>
            {
                using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                {
                    Uri = uri,
                    Method = HttpMethod.Get,
                    Headers = _authenticationHeader
                }, _cancellationToken).GetAwaiter().GetResult();

                if (responseStream.Length <= 0)
                {
                    return;
                }

                using StreamReader streamReader = new(responseStream);
                string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                hierarchyIds = JsonConvert.DeserializeObject<HierarchyResponse>(content)?.Hierarchies.ToList() ?? [];

                responseStream.Seek(0, SeekOrigin.Begin);
                StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
            });
        }
        catch (HttpClientProviderRequestException ex)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.HierarchyList)} failed. |Exception details: {ex}", ex);
            throw;
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.HierarchyList)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.HierarchyList)} finalized.");
        return hierarchyIds;
    }

    public List<Node> GenerateHierarchyIdDetailReport(OrderedQueue queueItem, APIReport<ReportSettings> apiReport, IEnumerable<Hierarchy> hierarchyIds, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.HierarchyIdDetail)} initialized.");

        var nodeList = new ConcurrentBag<Node>();
        var exceptions = new ConcurrentQueue<Exception>();

        if (hierarchyIds != null && hierarchyIds.Any())
        {
            Parallel.ForEach(hierarchyIds, new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism }, (hierarchy, state) =>
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.HierarchyIdDetail}-{hierarchy.Id}-0".ToLower(), _reportExtension);
                var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);

                if (reportFile.Exists)
                {
                    using Stream reportFileStream = reportFile.GetAsync().GetAwaiter().GetResult();
                    using StreamReader streamReader = new(reportFileStream);
                    string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                    HierarchyDetail hierarchyDetail = JsonConvert.DeserializeObject<HierarchyDetail>(content);
                    hierarchyDetail?.Nodes.ForEach(node => nodeList.Add(node));

                    return;
                }

                var uri = string.Join("/", _integration.EndpointURI, string.Format(apiReport.ReportSettings.Endpoint, hierarchy.Id));

                try
                {
                    cancellableRetry(() =>
                    {
                        using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(
                            new HttpRequestOptions
                            {
                                Uri = uri,
                                Headers = _authenticationHeader,
                                Method = HttpMethod.Get
                            }, _cancellationToken).GetAwaiter().GetResult();

                        if (responseStream.Length <= 0)
                        {
                            return;
                        }

                        using StreamReader streamReader = new(responseStream);
                        string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                        HierarchyDetail hierarchyDetail = JsonConvert.DeserializeObject<HierarchyDetail>(content);
                        hierarchyDetail?.Nodes.ForEach(node => nodeList.Add(node));

                        responseStream.Seek(0, SeekOrigin.Begin);

                        StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                        _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
                    });
                }
                catch (HttpClientProviderRequestException ex)
                {
                    HandleExceptionAndStopLoop(ex, state, exceptions, ReportName.HierarchyIdDetail);
                }
                catch (Exception ex)
                {
                    HandleExceptionAndStopLoop(ex, state, exceptions, ReportName.HierarchyIdDetail);
                }
            });

            if (!exceptions.IsEmpty)
            {
                ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
            }
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.HierarchyIdDetail)} finalized.");
        return nodeList.ToList();
    }
    private void HandleExceptionAndStopLoop<TException>(TException ex, ParallelLoopState state, ConcurrentQueue<Exception> exceptions, ReportName reportName)
        where TException : Exception
    {
        // Build log message
        var logMessage = BuildLogMessage(ex, reportName);
        _logException(LogLevel.Error, logMessage, ex);

        exceptions.Enqueue(ex);
        state.Stop();
    }

    private static string BuildLogMessage<TException>(TException ex, ReportName reportName) where TException : Exception
    {
        return ex switch
        {
            HttpClientProviderRequestException httpEx => $"{reportName} failed. |Exception details: {httpEx}",
            _ => $"{reportName} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}"
        };
    }
    public void GenerateHierarchyGroupingReport(OrderedQueue queueItem, APIReport<ReportSettings> apiReport, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.HierarchyGrouping)} initialized.");

        var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.HierarchyGrouping}-0".ToLower(), _reportExtension);
        var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
        var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);

        if (reportFile.Exists)
        {
            _logMessage(LogLevel.Info, $"{nameof(ReportName.HierarchyGrouping)} finalized. Report has already been collected");
            return;
        }

        var uri = string.Join("/", _integration.EndpointURI, string.Format(apiReport.ReportSettings.Endpoint, queueItem.EntityID));

        try
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                return;
            }

            cancellableRetry(() =>
            {
                using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                {
                    Uri = uri,
                    Method = HttpMethod.Get,
                    Headers = _authenticationHeader,
                }, _cancellationToken).GetAwaiter().GetResult();

                if (responseStream.Length <= 0)
                {
                    return;
                }

                StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
            });
        }
        catch (HttpClientProviderRequestException ex)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.HierarchyGrouping)} failed. |Exception details: {ex}", ex);
            throw;
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.HierarchyGrouping)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.HierarchyGrouping)} finalized.");
    }

    public void GenerateHierarchyNodeDetailReport(OrderedQueue queueItem, APIReport<ReportSettings> apiReport, List<Node> nodes, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.HierarchyNodeDetail)} initialized.");
        var exceptions = new ConcurrentQueue<Exception>();

        Parallel.ForEach(nodes, new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism }, (node, state) =>
        {
            try
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                cancellableRetry(() =>
                {
                    GenerateHierarchyNode(queueItem, apiReport, node, cancellableRetry);
                });
            }
            catch (HttpClientProviderRequestException ex)
            {
                HandleExceptionAndStopLoop(ex, state, exceptions, ReportName.HierarchyNodeDetail);
            }
            catch (Exception ex)
            {
                HandleExceptionAndStopLoop(ex, state, exceptions, ReportName.HierarchyNodeDetail);
            }
        });

        if (!exceptions.IsEmpty)
        {
            ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.HierarchyNodeDetail)} finalized.");
    }

    public List<BehaviorType> GenerateBehaviorTypeListReport(OrderedQueue queueItem, APIReport<ReportSettings> apiReport, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.BehaviorTypeList)} initialized.");

        var behaviorTypes = new List<BehaviorType>();
        var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.BehaviorTypeList}-0".ToLower(), _reportExtension);
        var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
        var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);

        if (reportFile.Exists)
        {
            using Stream reportFileStream = reportFile.GetAsync().GetAwaiter().GetResult();
            using (StreamReader streamReader = new(reportFileStream))
            {
                string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                BehaviorTypes behaviorFile = JsonConvert.DeserializeObject<BehaviorTypes>(content);
                if (behaviorFile?.BehaviorType?.Count > 0)
                {
                    behaviorTypes = behaviorFile.BehaviorType;
                }
            }

            _logMessage(LogLevel.Info, $"{nameof(ReportName.BehaviorTypeList)} finalized. Report has already been collected");
            return behaviorTypes;
        }

        try
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                return behaviorTypes;
            }

            var uri = string.Join("/", _integration.EndpointURI, string.Format(apiReport.ReportSettings.Endpoint, queueItem.EntityID));

            cancellableRetry(() =>
            {
                using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                {
                    Uri = uri,
                    Method = HttpMethod.Get,
                    Headers = _authenticationHeader
                }, _cancellationToken).GetAwaiter().GetResult();

                if (responseStream.Length <= 0)
                {
                    return;
                }

                using StreamReader streamReader = new(responseStream);
                string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                BehaviorTypes response = JsonConvert.DeserializeObject<BehaviorTypes>(content);
                if (response?.BehaviorType?.Count > 0)
                {
                    behaviorTypes = response.BehaviorType;
                }

                responseStream.Seek(0, SeekOrigin.Begin);

                StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
            });
        }
        catch (HttpClientProviderRequestException ex)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.BehaviorTypeList)} failed. |Exception details: {ex}", ex);
            throw;
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.BehaviorTypeList)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.BehaviorTypeList)} finalized.");
        return behaviorTypes;
    }

    public void GenerateBehaviorSearchListReport(OrderedQueue queueItem, APIReport<ReportSettings> apiReport, List<BehaviorType> behaviorTypes, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.BehaviorSearchList)} initialized.");

        try
        {
            foreach (var behaviorType in behaviorTypes)
            {
                int page = 1;
                int totalPages = 1;

                while (page <= totalPages)
                {
                    if (_cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }

                    var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.BehaviorSearchList}-{behaviorType.Id}-{page - 1}".ToLower(), _reportExtension);
                    var behaviorSearchResponse = new BehaviorSearchResponse();
                    var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                    var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);

                    if (reportFile.Exists)
                    {
                        using Stream reportFileStream = reportFile.GetAsync().GetAwaiter().GetResult();
                        using StreamReader streamReader = new(reportFileStream);
                        string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                        behaviorSearchResponse = JsonConvert.DeserializeObject<BehaviorSearchResponse>(content);
                    }
                    else
                    {
                        cancellableRetry(() =>
                        {
                            string uri = string.Join("/", _integration.EndpointURI,
                                string.Format(apiReport.ReportSettings.Endpoint, queueItem.EntityID,
                                    behaviorType.Id, _pageSize, page));

                            using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                            {
                                Uri = uri,
                                Method = HttpMethod.Get,
                                Headers = _authenticationHeader
                            }, _cancellationToken).GetAwaiter().GetResult();

                            if (responseStream.Length <= 0)
                            {
                                return;
                            }

                            using StreamReader streamReader = new(responseStream);
                            string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                            behaviorSearchResponse = JsonConvert.DeserializeObject<BehaviorSearchResponse>(content);

                            responseStream.Seek(0, SeekOrigin.Begin);

                            StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                            _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
                        });
                    }

                    if (page == 1)
                    {
                        totalPages = decimal.ToInt32(decimal.Ceiling((decimal)behaviorSearchResponse.SetInfo.TotalCount / _pageSize));
                    }

                    page++;
                }
            }
        }
        catch (HttpClientProviderRequestException ex)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.BehaviorSearchList)} failed. |Exception details: {ex}", ex);
            throw;
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.BehaviorSearchList)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.BehaviorSearchList)} finalized.");
    }

    private void GenerateHierarchyNode(OrderedQueue queueItem, APIReport<ReportSettings> apiReport, Node node, Action<Action> cancellableRetry)
    {
        var hierarchyDetail = new NodeDetail();

        var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.HierarchyNodeDetail}-{node.Id}-0".ToLower(), _reportExtension);
        var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
        var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);

        if (reportFile.Exists)
        {
            using Stream reportFileStream = reportFile.GetAsync().GetAwaiter().GetResult();
            using StreamReader streamReader = new(reportFileStream);
            string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
            hierarchyDetail = JsonConvert.DeserializeObject<NodeDetail>(content);
        }
        else
        {
            var uri = string.Join("/", _integration.EndpointURI, string.Format(apiReport.ReportSettings.Endpoint, node.Id));

            cancellableRetry(() =>
            {
                using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                {
                    Uri = uri,
                    Method = HttpMethod.Get,
                    Headers = _authenticationHeader
                }, _cancellationToken).GetAwaiter().GetResult();

                using StreamReader streamReader = new(responseStream);
                string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                hierarchyDetail = JsonConvert.DeserializeObject<NodeDetail>(content);

                responseStream.Seek(0, SeekOrigin.Begin);

                StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
            });
        }

        foreach (var childNode in hierarchyDetail.ChildNodes)
        {
            try
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                GenerateHierarchyNode(queueItem, apiReport, childNode, cancellableRetry);
            }
            catch (HttpClientProviderRequestException ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.HierarchyNodeDetail)} failed. |Exception details: {ex}", ex);
                throw;
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.HierarchyNodeDetail)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                throw;
            }
        }
    }
}
