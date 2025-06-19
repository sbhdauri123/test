using Greenhouse.Data.DataSource.GWICore;
using Greenhouse.Data.DataSource.GWICore.Requests;
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
using System.Net.Mime;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.GWICore;

public class GWICoreService
{
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly Credential _credential;
    private readonly Credential _greenhouseS3Credential;
    private readonly Integration _integration;
    private readonly Func<string, DateTime, string, string> _getS3PathHelper;
    private readonly string _reportFormat = ".json";
    private readonly IEnumerable<APIReport<ReportSettings>> _apiReports;
    private readonly int _maxDegreeOfParallelism;
    private readonly Action<IFile, S3File, string[], long, bool> _uploadToS3;
    private readonly Action<LogLevel, string> _logMessage;
    private readonly Action<LogLevel, string, Exception> _logException;

    public GWICoreService(GWICoreServiceArguments serviceArguments)
    {
        _httpClientProvider = serviceArguments.HttpClientProvider;
        _credential = serviceArguments.Credential;
        _greenhouseS3Credential = serviceArguments.GreenhouseS3Credential;
        _integration = serviceArguments.Integration;
        _getS3PathHelper = serviceArguments.GetS3PathHelper;
        _apiReports = serviceArguments.ApiReports;
        _maxDegreeOfParallelism = serviceArguments.MaxDegreeOfParallelism;
        _uploadToS3 = serviceArguments.UploadToS3;
        _logMessage = serviceArguments.LogMessage;
        _logException = serviceArguments.LogException;
    }

    public List<CategoryFilter> GenerateCategoriesFilterReport(OrderedQueue queueItem, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.CategoriesFilter)} initialized.");

        var categories = new List<CategoryFilter>();
        var apiReport = _apiReports.Where(r => r.APIReportName == ReportName.CategoriesFilter.ToString()).SingleOrDefault();
        var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.CategoriesFilter}-0".ToLower(), _reportFormat);
        var uri = string.Join("/", _integration.EndpointURI, apiReport.ReportSettings.Endpoint);

        try
        {
            cancellableRetry(() =>
            {
                using Stream response = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                {
                    Uri = uri,
                    Method = HttpMethod.Post,
                    AuthToken = _credential.CredentialSet.token,
                    ContentType = MediaTypeNames.Application.Json,
                    Content = new StringContent(
                        JsonConvert.SerializeObject(new { include = new { datasets = true, lineage = true } }),
                        Encoding.UTF8, MediaTypeNames.Application.Json)
                }).GetAwaiter().GetResult();

                using StreamReader streamReader = new(response);
                if (response.Length <= 0)
                {
                    return;
                }

                string content = streamReader.ReadToEndAsync().GetAwaiter().GetResult();
                categories = JsonConvert.DeserializeObject<CategoriesFilterResponse>(content)?.Categories;
                var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
                var incomingFile = new StreamFile(response, _greenhouseS3Credential);
                _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
            });
        }
        catch (HttpClientProviderRequestException requestException)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.CategoriesFilter)} failed. |Exception details: {requestException}", requestException);
            throw;
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.CategoriesFilter)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.CategoriesFilter)} finalized.");
        return categories;
    }

    public void GenerateCategoryDetailReport(OrderedQueue queueItem, List<CategoryFilter> categories, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.CategoryDetail)} initialized.");

        if (categories.Count == 0)
        {
            categories = GenerateCategoriesFilterReport(queueItem, cancellableRetry);
        }

        if (categories.Count != 0)
        {
            ConcurrentQueue<Exception> exceptions = new();

            Parallel.ForEach(categories, new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism }, (item, state) =>
            {
                try
                {
                    GenerateCategories(queueItem, item.Id, cancellableRetry);
                }
                catch (Exception ex)
                {
                    _logException(LogLevel.Error, $"{ReportName.CategoryDetail} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                    exceptions.Enqueue(ex);
                    state.Stop();
                }
            });

            if (!exceptions.IsEmpty)
            {
                ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
            }
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.CategoryDetail)} finalized.");
    }

    public void GenerateQuestionFilterReport(OrderedQueue queueItem, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.QuestionFilter)} initialized.");

        var apiReport = _apiReports.Where(r => r.APIReportName == ReportName.QuestionFilter.ToString()).SingleOrDefault();
        var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.QuestionFilter}-0".ToLower(), _reportFormat);
        var uri = string.Join("/", _integration.EndpointURI, apiReport.ReportSettings.Endpoint);

        try
        {
            cancellableRetry(() =>
            {
                using Stream stream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                {
                    Uri = uri,
                    Method = HttpMethod.Post,
                    AuthToken = _credential.CredentialSet.token,
                    ContentType = MediaTypeNames.Application.Json,
                    Content = new StringContent(JsonConvert.SerializeObject(new QuestionFilterRequest()),
                        Encoding.UTF8, MediaTypeNames.Application.Json)
                }).GetAwaiter().GetResult();

                if (stream.Length <= 0)
                {
                    return;
                }

                var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
                var incomingFile = new StreamFile(stream, _greenhouseS3Credential);
                _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
            });
        }
        catch (HttpClientProviderRequestException requestException)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.QuestionFilter)} failed. |Exception details: {requestException}", requestException);
            throw;
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"{ReportName.QuestionFilter} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.QuestionFilter)} finalized.");
    }

    public List<Namespace> GenerateNamespacesFilterReport(OrderedQueue queueItem, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.NamespacesFilter)} initialized.");

        var namespaces = new List<Namespace>();
        var apiReport = _apiReports.Where(r => r.APIReportName == ReportName.NamespacesFilter.ToString()).SingleOrDefault();
        var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.NamespacesFilter}-0".ToLower(), _reportFormat);
        var uri = string.Join("/", _integration.EndpointURI, apiReport.ReportSettings.Endpoint);

        try
        {
            cancellableRetry(() =>
            {
                using Stream response = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                {
                    Uri = uri,
                    Method = HttpMethod.Post,
                    AuthToken = _credential.CredentialSet.token,
                    ContentType = MediaTypeNames.Application.Json,
                    Content = new StringContent(
                        JsonConvert.SerializeObject(new { namespaces = Array.Empty<string>() }), Encoding.UTF8,
                        MediaTypeNames.Application.Json),
                }).GetAwaiter().GetResult();

                using StreamReader streamReader = new(response);
                if (response.Length <= 0)
                {
                    return;
                }

                string content = streamReader.ReadToEndAsync().GetAwaiter().GetResult();
                namespaces = JsonConvert.DeserializeObject<NamespaceFilterResponse>(content)?.Namespaces;

                var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
                var incomingFile = new StreamFile(response, _greenhouseS3Credential);
                _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
            });
        }
        catch (HttpClientProviderRequestException requestException)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.NamespacesFilter)} failed. |Exception details: {requestException}", requestException);
            throw;
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"{ReportName.NamespacesFilter} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.NamespacesFilter)} finalized.");
        return namespaces;
    }

    public void GenerateLocationsFilterReport(OrderedQueue queueItem, List<Namespace> namespaces, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.LocationsFilter)} initialized.");

        if (namespaces.Count == 0)
        {
            namespaces = GenerateNamespacesFilterReport(queueItem, cancellableRetry);
        }

        ConcurrentQueue<Exception> exceptions = new();

        Parallel.ForEach(namespaces, new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism }, (item, state) =>
        {
            var apiReport = _apiReports.Where(r => r.APIReportName == ReportName.LocationsFilter.ToString()).SingleOrDefault();
            var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.LocationsFilter}-{item.Code}-0".ToLower(), _reportFormat);
            var uri = string.Join("/", _integration.EndpointURI, apiReport.ReportSettings.Endpoint);

            try
            {
                cancellableRetry(() =>
                {
                    using Stream stream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                    {
                        Uri = uri,
                        Method = HttpMethod.Post,
                        AuthToken = _credential.CredentialSet.token,
                        ContentType = MediaTypeNames.Application.Json,
                        Content = new StringContent(
                            JsonConvert.SerializeObject(new
                            {
                                include = new { regions = true },
                                namespaces = new[] { new { code = item.Code } }
                            }), Encoding.UTF8, MediaTypeNames.Application.Json),
                    }).GetAwaiter().GetResult();

                    if (stream.Length <= 0)
                    {
                        return;
                    }

                    var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                    var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
                    var incomingFile = new StreamFile(stream, _greenhouseS3Credential);
                    _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
                });
            }
            catch (HttpClientProviderRequestException requestException)
            {
                _logException(LogLevel.Error, $"{nameof(ReportName.LocationsFilter)} failed. |Exception details: {requestException}", requestException);
                exceptions.Enqueue(requestException);
                state.Stop();
            }
            catch (Exception ex)
            {
                _logException(LogLevel.Error, $"{ReportName.LocationsFilter} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                exceptions.Enqueue(ex);
                state.Stop();
            }
        });

        if (!exceptions.IsEmpty)
        {
            ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.LocationsFilter)} finalized.");
    }

    public void GenerateSplittersFiltersReport(OrderedQueue queueItem, List<Namespace> namespaces, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.SplittersFilters)} initialized.");

        if (namespaces.Count == 0)
        {
            namespaces = GenerateNamespacesFilterReport(queueItem, cancellableRetry);
        }

        var apiReport = _apiReports.Where(r => r.APIReportName == ReportName.SplittersFilters.ToString()).SingleOrDefault();
        var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.SplittersFilters}-0".ToLower(), _reportFormat);
        var uri = string.Join("/", _integration.EndpointURI, apiReport.ReportSettings.Endpoint);

        try
        {
            var splitterRequest = new SplitterFilterRequest();
            namespaces.ForEach(n => { splitterRequest.Splitters.Add(new Splitter { NamespaceCode = n.Code }); });

            cancellableRetry(() =>
            {
                using Stream stream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                {
                    Uri = uri,
                    Method = HttpMethod.Post,
                    AuthToken = _credential.CredentialSet.token,
                    ContentType = MediaTypeNames.Application.Json,
                    Content = new StringContent(JsonConvert.SerializeObject(splitterRequest), Encoding.UTF8, MediaTypeNames.Application.Json)
                }).GetAwaiter().GetResult();

                if (stream.Length <= 0)
                {
                    return;
                }

                var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
                var incomingFile = new StreamFile(stream, _greenhouseS3Credential);
                _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
            });
        }
        catch (HttpClientProviderRequestException requestException)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.SplittersFilters)} failed. |Exception details: {requestException}", requestException);
            throw;
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"{ReportName.SplittersFilters} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.SplittersFilters)} finalized.");
    }

    public void GenerateWavesFilterReport(OrderedQueue queueItem, List<Namespace> namespaces, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.WavesFilter)} initialized.");

        if (namespaces.Count == 0)
        {
            namespaces = GenerateNamespacesFilterReport(queueItem, cancellableRetry);
        }

        var apiReport = _apiReports.Where(r => r.APIReportName == ReportName.WavesFilter.ToString()).SingleOrDefault();
        var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.WavesFilter}-0".ToLower(), _reportFormat);
        var uri = string.Join("/", _integration.EndpointURI, apiReport.ReportSettings.Endpoint);

        try
        {
            var splitterRequest = new WavesFilterRequest();
            namespaces.ForEach(n => { splitterRequest.Namespaces.Add(new Namespace { Code = n.Code }); });

            cancellableRetry(() =>
            {
                using Stream stream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                {
                    Uri = uri,
                    Method = HttpMethod.Post,
                    AuthToken = _credential.CredentialSet.token,
                    ContentType = MediaTypeNames.Application.Json,
                    Content = new StringContent(JsonConvert.SerializeObject(splitterRequest), Encoding.UTF8, MediaTypeNames.Application.Json)
                }).GetAwaiter().GetResult();

                if (stream.Length <= 0)
                {
                    return;
                }

                var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
                var incomingFile = new StreamFile(stream, _greenhouseS3Credential);
                _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
            });
        }
        catch (HttpClientProviderRequestException requestException)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.WavesFilter)} failed. |Exception details: {requestException}", requestException);
            throw;
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"{ReportName.WavesFilter} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.WavesFilter)} finalized.");
    }

    private void GenerateCategories(OrderedQueue queueItem, int categoryId, Action<Action> cancellableRetry)
    {
        var category = new CategoryDetailResponse();
        var apiReport = _apiReports.Where(r => r.APIReportName == ReportName.CategoryDetail.ToString()).SingleOrDefault();
        var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.CategoryDetail}-{categoryId}-0".ToLower(), _reportFormat);
        var uri = string.Format(string.Join("/", _integration.EndpointURI, apiReport.ReportSettings.Endpoint), categoryId);

        try
        {
            cancellableRetry(() =>
            {
                using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                {
                    Uri = uri,
                    Method = HttpMethod.Get,
                    AuthToken = _credential.CredentialSet.token,
                    ContentType = MediaTypeNames.Application.Json,
                }).GetAwaiter().GetResult();

                using StreamReader streamReader = new(responseStream);
                if (responseStream.Length <= 0)
                {
                    return;
                }

                category = JsonConvert.DeserializeObject<CategoryDetailResponse>(streamReader.ReadToEnd());

                var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
                var incomingFile = new StreamFile(responseStream, _greenhouseS3Credential);
                _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
            });

            if (category?.CategoryDetail?.ChildCategories != null)
            {
                foreach (var childCategory in category.CategoryDetail.ChildCategories)
                {
                    GenerateCategories(queueItem, childCategory.Id, cancellableRetry);
                }
            }
        }
        catch (HttpClientProviderRequestException requestException)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.CategoryDetail)} failed. |Exception details: {requestException}", requestException);
            throw;
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.CategoryDetail)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }
    }
}
