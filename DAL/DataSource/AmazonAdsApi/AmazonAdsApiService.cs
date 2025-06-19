using Greenhouse.Data.DataSource.AmazonAdsApi;
using Greenhouse.Data.DataSource.AmazonAdsApi.Responses;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Mime;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;
using JsonSerializer = Newtonsoft.Json.JsonSerializer;
using ReportSettings = Greenhouse.Data.DataSource.AmazonAdsApi.ReportSettings;


namespace Greenhouse.DAL.DataSource.AmazonAdsApi;

public class AmazonAdsApiService : IAmazonAdsApiService
{
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly Credential _credential;
    private readonly Credential _greenhouseS3Credential;
    private readonly Integration _integration;
    private readonly Func<string, DateTime, string, string> _getS3PathHelper;
    private readonly Action<IFile, S3File, string[], long, bool> _uploadToS3;
    private readonly Action<LogLevel, string> _logMessage;
    private readonly Action<LogLevel, string, Exception> _logException;

    public AmazonAdsApiService(AmazonAdsApiServiceArguments serviceArguments)
    {
        ArgumentNullException.ThrowIfNull(serviceArguments.HttpClientProvider);
        ArgumentNullException.ThrowIfNull(serviceArguments.Credential);
        ArgumentNullException.ThrowIfNull(serviceArguments.GreenhouseS3Credential);
        ArgumentNullException.ThrowIfNull(serviceArguments.Integration);
        ArgumentNullException.ThrowIfNull(serviceArguments.GetS3PathHelper);
        ArgumentNullException.ThrowIfNull(serviceArguments.UploadToS3);
        ArgumentNullException.ThrowIfNull(serviceArguments.LogMessage);
        ArgumentNullException.ThrowIfNull(serviceArguments.LogException);

        _httpClientProvider = serviceArguments.HttpClientProvider;
        _credential = serviceArguments.Credential;
        _greenhouseS3Credential = serviceArguments.GreenhouseS3Credential;
        _integration = serviceArguments.Integration;
        _getS3PathHelper = serviceArguments.GetS3PathHelper;
        _uploadToS3 = serviceArguments.UploadToS3;
        _logMessage = serviceArguments.LogMessage;
        _logException = serviceArguments.LogException;
    }

    public async Task<List<ProfileResponse>> GetProfilesDataAsync(AmazonAdsApiOAuth amazonAdsApiOAuth)
    {
        _logMessage(LogLevel.Info, $"Get {ReportName.Profiles} initialized.");

        var profilesList = new List<ProfileResponse>();
        try
        {
            Dictionary<string, string> _authenticationHeader = new Dictionary<string, string>()
                                        { { "Authorization", "Bearer " + amazonAdsApiOAuth.AccessToken }
                                        , { "Amazon-Advertising-API-ClientId", _credential.CredentialSet.ClientId } };


            var httpRequestMessageSettings = new HttpRequestOptions()
            {
                Uri = _credential.CredentialSet.ProfileUrl.ToString(),
                Method = HttpMethod.Get,
                Headers = _authenticationHeader
            };
            var request = _httpClientProvider.BuildRequestMessage(httpRequestMessageSettings);

            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(MediaTypeNames.Application.Json));
            string responseString = _httpClientProvider.SendRequestAsync(request).GetAwaiter().GetResult();

            if ((!string.IsNullOrEmpty(responseString)) && responseString.Contains("profileId"))
            {
                profilesList = JsonConvert.DeserializeObject<List<ProfileResponse>>(responseString);
                _logMessage(LogLevel.Info, $"{ReportName.Profiles} finalized.");
            }
            else
            {
                _logMessage(LogLevel.Info, $"Status code showing not successful while calling Amazon Profiles API.");
            }
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"Error while getting the profiles data. " +
                $"|Exception:{ex.GetType().FullName}|Message:{ex.Message}" +
                $"|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }
        return await Task.FromResult(profilesList);
    }

    public async Task<string> MakeCreateReportApiCallAsync(AmazonAdsApiOAuth amazonAdsApiOAuth
                                                        , string jsonPayload, OrderedQueue queueItem
                                                        , APIReport<ReportSettings> apiReport
                                                        , Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"Create report {apiReport.APIReportName} initialized for the profileID {queueItem.EntityID}" +
            $" for the file date {queueItem.FileDate}, created date {queueItem.CreatedDate},  at {DateTime.Now}.");
        string newReportResponse = string.Empty;

        try
        {
            cancellableRetry(() =>
            {
                Dictionary<string, string> _authenticationHeader = new Dictionary<string, string>()
                                        { { "Authorization", "Bearer " + amazonAdsApiOAuth.AccessToken }
                                        , { "Amazon-Advertising-API-ClientId", _credential.CredentialSet.ClientId }
                                        , { "Amazon-Advertising-API-Scope", queueItem.EntityID }};

                var httpRequestMessageSettings = new HttpRequestOptions()
                {
                    Uri = _credential.CredentialSet.ReportsUrl.ToString(),
                    Method = HttpMethod.Post,
                    Headers = _authenticationHeader,
                    ContentType = "application/vnd.createasyncreportrequest.v3+json",
                    Content = new StringContent(jsonPayload, Encoding.UTF8, MediaTypeNames.Application.Json)
                };
                var request = _httpClientProvider.BuildRequestMessage(httpRequestMessageSettings);

                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(MediaTypeNames.Application.Json));
                string responseString = _httpClientProvider.SendRequestAsync(request).GetAwaiter().GetResult();

                // Ensure the response was successful
                if ((!string.IsNullOrEmpty(responseString)) && responseString.Contains("reportId"))
                {
                    newReportResponse = responseString;
                    _logMessage(LogLevel.Info, $"Successfully created report {apiReport.APIReportName} for the profileID {queueItem.EntityID}" +
                            $" for the file date {queueItem.FileDate}, created date {queueItem.CreatedDate},  at {DateTime.Now}.");
                }
                else
                {
                    _logMessage(LogLevel.Info, $"Create report failed for {apiReport.APIReportName} for the profileID {queueItem.EntityID}" +
                            $" for the file date {queueItem.FileDate}, created date {queueItem.CreatedDate},  at {DateTime.Now}.");
                }
            });
        }

        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"Error while creating a report for the profileID {queueItem.EntityID}." +
                            $"|Exception:{ex.GetType().FullName}" +
                            $"|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }
        return await Task.FromResult(newReportResponse);
    }

    public (string reportStatus, FileCollectionItem fileCollection) CheckReportStatusAndDownload(AmazonAdsApiOAuth amazonAdsApiOAuth
                                                            , APIReportItem apiReportItem
                                                            , Action<Action> cancellableRetry
                                                            , OrderedQueue queueItem
                                                            , int chunkSize
                                                            , string reportFailureReason)
    {
        _logMessage(LogLevel.Info, $"Checking report status for report {apiReportItem.ReportName} for the profileID {queueItem.EntityID}" +
                            $" for the file date {queueItem.FileDate}, created date {queueItem.CreatedDate},  at {DateTime.Now}.");

        string reportStatusResponse = string.Empty;
        FileCollectionItem fileCollectionItem = new FileCollectionItem();
        try
        {
            cancellableRetry(() =>
            {
                Dictionary<string, string> _authenticationHeader = new Dictionary<string, string>()
                                        { { "Authorization", "Bearer " + amazonAdsApiOAuth.AccessToken }
                                        , { "Amazon-Advertising-API-ClientId", _credential.CredentialSet.ClientId }
                                        , { "Amazon-Advertising-API-Scope", apiReportItem.ProfileID }};
                var reportsUrl = _credential.CredentialSet.ReportsUrl + "/" + apiReportItem.ReportId;

                var httpRequestMessageSettings = new HttpRequestOptions()
                {
                    Uri = reportsUrl.ToString(),
                    Method = HttpMethod.Get,
                    Headers = _authenticationHeader,
                    ContentType = "application/vnd.createasyncreportrequest.v3+json",
                };
                var request = _httpClientProvider.BuildRequestMessage(httpRequestMessageSettings);

                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(MediaTypeNames.Application.Json));
                string responseString = _httpClientProvider.SendRequestAsync(request).GetAwaiter().GetResult();

                if ((!string.IsNullOrEmpty(responseString)) && responseString.Contains("adProduct"))
                {
                    var jsonDocument = JsonDocument.Parse(responseString);
                    var root = jsonDocument.RootElement;

                    string reportStatus = root.GetProperty("status").GetString();
                    if (reportStatus == ReportStatus.COMPLETED.ToString())
                    {
                        string reportUrl = root.GetProperty("url").GetString();
                        Dictionary<string, string> emptyDictionary = new Dictionary<string, string>();
                        var httpRequestOptions = new HttpRequestOptions()
                        {
                            Uri = reportUrl.ToString(),
                            Method = HttpMethod.Get,
                        };
                        var httpRequestMessage = _httpClientProvider.BuildRequestMessage(httpRequestOptions);

                        Stream compressedStream = _httpClientProvider.DownloadFileStreamAsync(httpRequestMessage)
                                                                     .GetAwaiter().GetResult();

                        fileCollectionItem = UploadReportToS3(compressedStream, apiReportItem, queueItem, chunkSize);
                        reportStatusResponse = "COMPLETED";

                    _logMessage(LogLevel.Info, $"Successfully downloaded report and uploaded report data to the S3 bucket " +
                            $"for report {apiReportItem.ReportName} for the profileID {queueItem.EntityID}" +
                            $" and for the CreateReportId {apiReportItem.ReportId}" +
                            $" for the file date {queueItem.FileDate}, created date {queueItem.CreatedDate},  at {DateTime.Now}.");
                    }
                    else
                    {
                        string failureReason = root.GetProperty("failureReason").GetString();
                        if (reportStatus == ReportStatus.FAILED.ToString()
                            && failureReason.Contains(reportFailureReason))
                        {
                            reportStatusResponse = InternalReportStatus.INTERNAL_ERROR.ToString();
                        }
                        else
                        {
                            _logMessage(LogLevel.Info, $"Report is not yet ready : report {apiReportItem.ReportName}, " +
                            $"Report Status:  {reportStatus} for the profileID {queueItem.EntityID}" +
                            $" for the file date {queueItem.FileDate}, created date {queueItem.CreatedDate},  at {DateTime.Now}.");
                        }
                    }
                }
                else
                {
                    _logMessage(LogLevel.Info, $"Not able to connect to report url : {apiReportItem.ReportName} " +
                                                $"for the profileID {apiReportItem.ProfileID}.");
                }
            });
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"Error while downloading the report for the profileID " +
                            $"{queueItem.EntityID}.|Exception:{ex.GetType().FullName}" +
                            $"|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }
        return (reportStatusResponse, fileCollectionItem);
    }

    public FileCollectionItem UploadReportToS3(Stream compressedStream, APIReportItem apiReportItem
                                                    , OrderedQueue queueItem, int chunkSize)
    {
        long oneGB = 1L * 1024 * 1024 * 1024; // 1 GB in bytes (1,073,741,824)
        long reportFileSize = 0;
        using (var decompressedStream = new GZipStream(compressedStream, CompressionMode.Decompress))
        {
            string tempFilePath = Path.GetTempFileName();
            using (FileStream fileStream = new FileStream(tempFilePath, FileMode.Create, FileAccess.ReadWrite))
            {
                decompressedStream.CopyTo(fileStream);
                fileStream.Position = 0;
                reportFileSize = fileStream.Length;
                if (reportFileSize > oneGB)
                {
                    UploadReportChunksToS3(fileStream, apiReportItem, queueItem, chunkSize);
                }
                else
                {
                    UploadToS3(fileStream, apiReportItem, queueItem);
                }
            }
            File.Delete(tempFilePath);
        }

        FileCollectionItem fileCollectionItem = new FileCollectionItem()
        {
            FileSize = reportFileSize,
            SourceFileName = apiReportItem.FileName,
            FilePath = apiReportItem.FileName
        };
        return fileCollectionItem;
    }

    public void UploadReportChunksToS3(FileStream decompressedStream, APIReportItem apiReportItem
                                        , OrderedQueue queueItem, int chunkSize)
    {
        long length = decompressedStream.Length;
        using (var streamReader = new StreamReader(decompressedStream, Encoding.UTF8))
        {
            using (var jsonReader = new JsonTextReader(streamReader))
            {
                long chunkFileSize = chunkSize * 1024 * 1024;
                int chunkIndex = 0;
                long currentChunkSize = 0;

                MemoryStream memoryStream = new MemoryStream();
                StreamWriter writer = new StreamWriter(memoryStream, Encoding.UTF8);
                JsonTextWriter jsonWriter = new JsonTextWriter(writer);
                jsonWriter.WriteStartArray(); // Start writing JSON array

                JsonSerializer serializer = new JsonSerializer();

                while (jsonReader.Read())
                {
                    if (jsonReader.TokenType == JsonToken.StartObject)
                    {
                        JObject jsonObject = JObject.Load(jsonReader);
                        serializer.Serialize(jsonWriter, jsonObject);
                        jsonWriter.Flush(); // Ensure object is written to MemoryStream

                        currentChunkSize += Encoding.UTF8.GetByteCount(jsonObject.ToString());

                        if (currentChunkSize >= chunkFileSize)
                        {
                            jsonWriter.WriteEndArray(); // Close JSON array before upload
                            writer.Flush();
                            UploadToS3(memoryStream, apiReportItem, queueItem, chunkIndex++);

                            // Reset for the next chunk
                            memoryStream.Dispose(); // Dispose the old stream
                            memoryStream = new MemoryStream(); // Create a new stream
                            writer = new StreamWriter(memoryStream, Encoding.UTF8);
                            jsonWriter = new JsonTextWriter(writer);
                            jsonWriter.WriteStartArray();
                            currentChunkSize = 0;
                        }
                    }
                }

                // Write remaining JSON objects if any
                jsonWriter.WriteEndArray();
                writer.Flush();

                if (memoryStream.Length > 0)
                {
                    UploadToS3(memoryStream, apiReportItem, queueItem, chunkIndex);
                }
                writer.Dispose();
                memoryStream.Dispose();
            }
        }
    }

    public void UploadToS3(Stream compressedStream, APIReportItem apiReportItem, OrderedQueue queueItem, int? chunkIndex = null)
    {
        var incomingFile = new StreamFile(compressedStream, _greenhouseS3Credential);
        string fileName = string.Empty;
        string batchId = apiReportItem.BatchId?.ToString() ?? string.Empty;
        if (chunkIndex == null)
        {
            fileName = queueItem.FileGUID + "_" + apiReportItem.FileName.ToLower().Replace(" ", "_") +"_"+ batchId + ".json";
        }
        else
        {
            fileName = queueItem.FileGUID + "_" + apiReportItem.FileName.ToLower().Replace(" ", "_") + "_" + batchId + "_" + chunkIndex.ToString() + ".json";
        }

        var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, fileName);
        var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
        _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
    }

    public async Task<AdvertiserResponse> GetAdvertiserDataAsync(AmazonAdsApiOAuth amazonAdsApiOAuth
                                                                , OrderedQueue queueItem
                                                                , Action<Action> cancellableRetry
                                                                , string index)
    {
        _logMessage(LogLevel.Info, $"{ReportName.Advertiser} initialized for the profileID {queueItem.EntityID}.");
        var dspAdvertiser = new AdvertiserResponse();

        try
        {
            cancellableRetry(() =>
            {
                Dictionary<string, string> _authenticationHeader = new Dictionary<string, string>()
                                        { { "Authorization", "Bearer " + amazonAdsApiOAuth.AccessToken }
                                        , { "Amazon-Advertising-API-ClientId", _credential.CredentialSet.ClientId }
                                        , { "Amazon-Advertising-API-Scope", queueItem.EntityID }};

                var dspAdvertiserUrl = _credential.CredentialSet.DspAdvertiserUrl + index;

                var httpRequestMessageSettings = new HttpRequestOptions()
                {
                    Uri = dspAdvertiserUrl.ToString(),
                    Method = HttpMethod.Get,
                    Headers = _authenticationHeader,
                    ContentType = MediaTypeNames.Application.Json
                };
                var request = _httpClientProvider.BuildRequestMessage(httpRequestMessageSettings);

                string responseString = _httpClientProvider.SendRequestAsync(request).GetAwaiter().GetResult();

                // Ensure the response was successful
                if ((!string.IsNullOrEmpty(responseString)) && responseString.Contains("advertiserId"))
                {
                    dspAdvertiser = JsonConvert.DeserializeObject<AdvertiserResponse>(responseString);
                    _logMessage(LogLevel.Info, $"{ReportName.Advertiser} finalized for the profileID {queueItem.EntityID}.");
                }
                else
                {
                    _logMessage(LogLevel.Info, $"Status code showing not successful while calling Advertiser API.");
                }
            });
        }

        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"Error while getting the advertisers data. " +
                $"|Exception:{ex.GetType().FullName}|Message:{ex.Message}" +
                $"|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }
        return await Task.FromResult(dspAdvertiser);
    }
}
