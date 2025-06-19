using Greenhouse.Data.DataSource.Kantar;
using Greenhouse.Data.DataSource.Kantar.Responses;
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

namespace Greenhouse.DAL.DataSource.Kantar;

public class KantarService
{
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly Credential _credential;
    private readonly Credential _greenhouseS3Credential;
    private readonly Integration _integration;
    private readonly Func<string, DateTime, string, string> _getS3PathHelper;
    private readonly string _reportExtension = ".json";
    private readonly string _taxonomyRootLevel;
    private readonly IEnumerable<APIReport<ReportSettings>> _apiReports;
    private readonly int _maxDegreeOfParallelism;
    private readonly Action<IFile, S3File, string[], long, bool> _uploadToS3;
    private readonly Action<LogLevel, string> _logMessage;
    private readonly Action<LogLevel, string, Exception> _logException;
    private readonly CancellationToken _cancellationToken;

    public KantarService(KantarServiceArguments serviceArguments)
    {
        _httpClientProvider = serviceArguments.HttpClientProvider;
        _credential = serviceArguments.Credential;
        _greenhouseS3Credential = serviceArguments.GreenhouseS3Credential;
        _integration = serviceArguments.Integration;
        _getS3PathHelper = serviceArguments.GetS3PathHelper;
        _apiReports = serviceArguments.ApiReports;
        _taxonomyRootLevel = serviceArguments.TaxonomyRootLevel;
        _maxDegreeOfParallelism = serviceArguments.MaxDegreeOfParallelism;
        _uploadToS3 = serviceArguments.UploadToS3;
        _logMessage = serviceArguments.LogMessage;
        _logException = serviceArguments.LogException;
        _cancellationToken = serviceArguments.cancellationToken;
    }

    public List<Survey> GenerateSurveysReport(OrderedQueue queueItem, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.Surveys)} initialized.");

        var surveys = new List<Survey>();
        var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.Surveys}-0".ToLower(), _reportExtension);
        var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
        var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);

        if (reportFile.Exists)
        {
            using Stream reportFileStream = reportFile.GetAsync().GetAwaiter().GetResult();
            using (StreamReader streamReader = new(reportFileStream))
            {
                string content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                surveys = JsonConvert.DeserializeObject<SurveyResponse>(content)?.Surveys ?? [];
            }

            _logMessage(LogLevel.Info, $"{nameof(ReportName.Surveys)} finalized. Report has already been collected");
            return surveys;
        }

        var apiReport = _apiReports.SingleOrDefault(r => r.APIReportName == ReportName.Surveys.ToString());
        var uri = string.Join("/", _integration.EndpointURI, string.Format(apiReport.ReportSettings.Endpoint, _credential.CredentialSet.UserId, _credential.CredentialSet.ApiKey));

        try
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                return Enumerable.Empty<Survey>().ToList();
            }

            cancellableRetry(() =>
            {
                using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                {
                    Uri = uri,
                    Method = HttpMethod.Get,
                    ContentType = "application/json",
                }, _cancellationToken).GetAwaiter().GetResult();

                if (responseStream.Length <= 0)
                {
                    return;
                }

                using StreamReader streamReader = new(responseStream);
                string surveyResponse = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                surveys = JsonConvert.DeserializeObject<SurveyResponse>(surveyResponse)?.Surveys ?? [];

                responseStream.Seek(0, SeekOrigin.Begin);
                StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                _uploadToS3(incomingFile, reportFile, null, 0, false);
            });
        }
        catch (HttpClientProviderRequestException ex)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.Surveys)} failed. |Exception details: {ex}", ex);
            throw;
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error,
                $"{nameof(ReportName.Surveys)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.Surveys)} finalized.");
        return surveys;
    }

    public void GenerateSurveyInfoReport(OrderedQueue queueItem, List<Survey> surveys, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.Survey)} initialized.");

        if (surveys.Count == 0)
        {
            _logMessage(LogLevel.Info, $"{nameof(ReportName.Survey)} - No Surveys found.");
            return;
        }

        var apiReport = _apiReports.SingleOrDefault(r => r.APIReportName == ReportName.Survey.ToString());
        ConcurrentQueue<Exception> exceptions = new();

        Parallel.ForEach(surveys, new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism }, (item, state) =>
        {
            try
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    state.Stop();
                    return;
                }

                var reportName = string.Concat($"{queueItem.FileGUID}-{ReportName.Survey}-{item.SurveyFamily}-{item.WaveName}-0".ToLower(), _reportExtension);
                var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);

                if (reportFile.Exists)
                {
                    return;
                }

                var uri = string.Join("/", _integration.EndpointURI,
                    string.Format(apiReport.ReportSettings.Endpoint, item.SurveyFamily, item.WaveName,
                        _credential.CredentialSet.UserId, _credential.CredentialSet.ApiKey));

                cancellableRetry(() =>
                {
                    using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                    {
                        Uri = uri,
                        Method = HttpMethod.Get,
                        ContentType = "application/json",
                    }, _cancellationToken).GetAwaiter().GetResult();

                    if (responseStream.Length <= 0)
                    {
                        return;
                    }

                    StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                    _uploadToS3(incomingFile, reportFile, null, 0, false);
                });
            }
            catch (HttpClientProviderRequestException ex)
            {
                HandleExceptionAndStopLoop(ex, state, exceptions, ReportName.Survey);
            }
            catch (Exception ex)
            {
                HandleExceptionAndStopLoop(ex, state, exceptions, ReportName.Survey);
            }
        });

        if (!exceptions.IsEmpty)
        {
            ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.Survey)} finalized.");
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
    public void GenerateCategoriesReport(OrderedQueue queueItem, List<Survey> surveys, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.Category)} initialized.");

        if (surveys.Count == 0)
        {
            _logMessage(LogLevel.Info, $"{nameof(ReportName.Category)} - No Surveys found.");
            return;
        }

        var apiReport = _apiReports.SingleOrDefault(r => r.APIReportName == ReportName.Category.ToString());
        ConcurrentQueue<Exception> exceptions = new();

        Parallel.ForEach(surveys, new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism }, (item, state) =>
        {
            try
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    state.Stop();
                    return;
                }

                var categories = new List<Category>();
                var reportName = string.Concat($"{queueItem.FileGUID}-{item.SurveyFamily}-{item.WaveName}-{ReportName.Category}-{_taxonomyRootLevel}-0".ToLower(), _reportExtension);
                var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);

                if (reportFile.Exists)
                {
                    using Stream reportFileStream = reportFile.GetAsync().GetAwaiter().GetResult();
                    using (StreamReader streamReader = new(reportFileStream))
                    {
                        string categoryResponse = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                        categories = JsonConvert.DeserializeObject<CategoryResponse>(categoryResponse)?.Categories ?? [];
                    }

                    foreach (var category in categories)
                    {
                        item.Categories.Add(new Category() { Id = category.Id, QuestionId = category.QuestionId, Type = TaxonomyType.Category, Content = string.Empty });
                    }

                    return;
                }

                var uri = string.Join("/", _integration.EndpointURI, string.Format(apiReport.ReportSettings.Endpoint, item.SurveyFamily, item.WaveName, _taxonomyRootLevel, _credential.CredentialSet.UserId, _credential.CredentialSet.ApiKey));

                cancellableRetry(() =>
                {
                    using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                    {
                        Uri = uri,
                        Method = HttpMethod.Get,
                        ContentType = "application/json",
                    }, _cancellationToken).GetAwaiter().GetResult();

                    if (responseStream.Length <= 0)
                    {
                        return;
                    }

                    using StreamReader streamReader = new(responseStream);
                    string categoryResponse = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();
                    categories = JsonConvert.DeserializeObject<CategoryResponse>(categoryResponse)?.Categories ?? [];

                    foreach (Category category in categories)
                    {
                        item.Categories.Add(new Category() { Id = category.Id, QuestionId = category.QuestionId, Type = TaxonomyType.Category, Content = string.Empty });
                    }

                    responseStream.Seek(0, SeekOrigin.Begin);
                    StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                    _uploadToS3(incomingFile, reportFile, null, 0, false);
                });
            }
            catch (HttpClientProviderRequestException ex)
            {
                HandleExceptionAndStopLoop(ex, state, exceptions, ReportName.Category);
            }
            catch (Exception ex)
            {
                HandleExceptionAndStopLoop(ex, state, exceptions, ReportName.Category);
            }
        });

        if (!exceptions.IsEmpty)
        {
            ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.Category)} finalized.");
    }

    public void GenerateChildCategoriesReport(OrderedQueue queueItem, List<Survey> surveys, Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{nameof(ReportName.Question)} initialized.");

        if (surveys.Count == 0)
        {
            _logMessage(LogLevel.Info, $"{nameof(ReportName.Question)} - No Surveys found.");
            return;
        }

        ConcurrentQueue<Exception> exceptions = new();

        Parallel.ForEach(surveys, new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism }, (item, state) =>
        {
            try
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    state.Stop();
                    return;
                }

                GenerateCategories(queueItem, item, cancellableRetry);
            }
            catch (HttpClientProviderRequestException ex)
            {
                HandleExceptionAndStopLoop(ex, state, exceptions, ReportName.Question);
            }
            catch (Exception ex)
            {
                HandleExceptionAndStopLoop(ex, state, exceptions, ReportName.Question);
            }
        });

        if (!exceptions.IsEmpty)
        {
            ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
        }

        _logMessage(LogLevel.Info, $"{nameof(ReportName.Question)} finalized.");
    }

    private void GenerateCategories(OrderedQueue queueItem, Survey survey, Action<Action> cancellableRetry)
    {
        try
        {
            foreach (var category in survey.Categories)
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                var reportName = string.Concat($"{queueItem.FileGUID}-{survey.SurveyFamily}-{survey.WaveName}-{category.Type}-{category.Name}-0".ToLower(), _reportExtension);
                var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, reportName);
                var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);

                if (reportFile.Exists)
                {
                    continue;
                }

                var apiReport = new APIReport<ReportSettings>();
                if (category.Type == TaxonomyType.Question)
                {
                    apiReport = _apiReports.SingleOrDefault(r => r.APIReportName == ReportName.Question.ToString());
                }
                else
                {
                    apiReport = _apiReports.SingleOrDefault(r => r.APIReportName == ReportName.Category.ToString());
                }

                var uri = string.Format(string.Join("/", _integration.EndpointURI, apiReport.ReportSettings.Endpoint), survey.SurveyFamily, survey.WaveName, category.Name, _credential.CredentialSet.UserId, _credential.CredentialSet.ApiKey);

                cancellableRetry(() =>
                {
                    using Stream responseStream = _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                    {
                        Uri = uri,
                        Method = HttpMethod.Get,
                        ContentType = "application/json",
                    }, _cancellationToken).GetAwaiter().GetResult();

                    if (responseStream.Length <= 0)
                    {
                        return;
                    }

                    using StreamReader streamReader = new(responseStream);
                    category.Content = streamReader.ReadToEndAsync(_cancellationToken).GetAwaiter().GetResult();

                    responseStream.Seek(0, SeekOrigin.Begin);
                    StreamFile incomingFile = new(responseStream, _greenhouseS3Credential);
                    _uploadToS3(incomingFile, reportFile, null, 0, false);
                });

                if (category.Type != TaxonomyType.Question && category.Content.Length > 0)
                {
                    List<Category> categoryChild =
                        JsonConvert.DeserializeObject<CategoryResponse>(category.Content)?.Categories ?? [];

                    foreach (var child in categoryChild)
                    {
                        if (string.IsNullOrEmpty(child.QuestionId))
                            child.Type = TaxonomyType.SubCategory;
                        else
                            child.Type = TaxonomyType.Question;
                    }

                    var surveyChild = new Survey() { SurveyFamily = survey.SurveyFamily, WaveName = survey.WaveName, Categories = categoryChild };

                    GenerateCategories(queueItem, surveyChild, cancellableRetry);
                }
            }
        }
        catch (HttpClientProviderRequestException ex)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.Category)} failed. |Exception details: {ex}", ex);
            throw;
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"{nameof(ReportName.Category)} failed.|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }
    }
}
