using Greenhouse.Auth;
using Greenhouse.Data.DataSource.AmazonSellingPartnerApi;
using Greenhouse.Data.DataSource.AmazonSellingPartnerApi.Responses;
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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.AmazonSellingPartnerApi;

public partial class AmazonSellingPartnerApiService : IAmazonSellingPartnerApiService
{
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly Credential _credential;
    private readonly Credential _greenhouseS3Credential;
    private readonly Func<string, DateTime, string, string> _getS3PathHelper;
    private readonly Action<IFile, S3File, string[], long, bool> _uploadToS3;
    private readonly Action<LogLevel, string> _logMessage;
    private readonly Action<LogLevel, string, Exception> _logException;
    private readonly ITokenApiClient _tokenApiClient;

    public AmazonSellingPartnerApiService(AmazonSellingPartnerApiServiceArguments serviceArguments)
    {
        ArgumentNullException.ThrowIfNull(serviceArguments.HttpClientProvider);
        ArgumentNullException.ThrowIfNull(serviceArguments.Credential);
        ArgumentNullException.ThrowIfNull(serviceArguments.GreenhouseS3Credential);
        ArgumentNullException.ThrowIfNull(serviceArguments.GetS3PathHelper);
        ArgumentNullException.ThrowIfNull(serviceArguments.UploadToS3);
        ArgumentNullException.ThrowIfNull(serviceArguments.LogMessage);
        ArgumentNullException.ThrowIfNull(serviceArguments.LogException);
        ArgumentNullException.ThrowIfNull(serviceArguments.TokenApiClient);

        _httpClientProvider = serviceArguments.HttpClientProvider;
        _credential = serviceArguments.Credential;
        _greenhouseS3Credential = serviceArguments.GreenhouseS3Credential;
        _getS3PathHelper = serviceArguments.GetS3PathHelper;
        _uploadToS3 = serviceArguments.UploadToS3;
        _logMessage = serviceArguments.LogMessage;
        _logException = serviceArguments.LogException;
        _tokenApiClient = serviceArguments.TokenApiClient;
    }

    public async Task<CreateReportResponse> RequestReportAsync(string jsonPayload, OrderedQueue queueItem
                                                        , APIReport<ReportSettings> apiReport
                                                        , Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{apiReport.APIReportName} initialized for the MarketPlaceId {queueItem.EntityID}.");
        string newReportResponse = string.Empty;
        CreateReportResponse createReportResponse = null;
        try
        {
            cancellableRetry(() =>
            {
                Dictionary<string, string> _authenticationHeader = new Dictionary<string, string>()
                                        { { "x-amz-access-token", _tokenApiClient.GetAccessTokenAsync(TokenDataSource.AmazonSellingPartner).GetAwaiter().GetResult() } };

                var httpRequestMessageSettings = new HttpRequestOptions()
                {
                    Uri = _credential.CredentialSet.CreateReportsUrl,
                    Method = HttpMethod.Post,
                    Headers = _authenticationHeader,
                    Content = new StringContent(jsonPayload, Encoding.UTF8, MediaTypeNames.Application.Json)
                };
                var request = _httpClientProvider.BuildRequestMessage(httpRequestMessageSettings);

                CreateReportResponse reportResponse = _httpClientProvider.SendRequestAndDeserializeAsync<CreateReportResponse>(request).GetAwaiter().GetResult();

                // Ensure the response was successful
                if ((!string.IsNullOrEmpty(reportResponse.ReportId)))
                {
                    createReportResponse = reportResponse;
                    _logMessage(LogLevel.Info, $"Successfully created report {apiReport.APIReportName} for the " +
                        $"marketPlaceId {queueItem.EntityID}.");
                }
                else
                {
                    _logMessage(LogLevel.Info, $"Create report failed for the marketPlaceId {queueItem.EntityID}.");
                }
            });
        }

        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"Error while creating a report for the marketPlaceId {queueItem.EntityID}." +
                            $"|Exception:{ex.GetType().FullName}" +
                            $"|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }
        return await Task.FromResult(createReportResponse);
    }

    public async Task<ReportProcessingStatus> GetReportStatusAndDocumentIdAsync(string reportId, OrderedQueue queueItem
                                                        , APIReportItem apiReport
                                                        , Action<Action> cancellableRetry)
    {
        _logMessage(LogLevel.Info, $"{apiReport.ReportName} initialized for the MarketPlaceId {queueItem.EntityID}.");
        ReportProcessingStatus reportProcessingStatus = null;
        try
        {
            cancellableRetry(() =>
            {
                Dictionary<string, string> _authenticationHeader = new Dictionary<string, string>()
                                        { { "x-amz-access-token", _tokenApiClient.GetAccessTokenAsync(TokenDataSource.AmazonSellingPartner).GetAwaiter().GetResult() } };

                var reportsUrl = _credential.CredentialSet.CreateReportsUrl + "/" + reportId;
                var httpRequestMessageSettings = new HttpRequestOptions()
                {
                    Uri = reportsUrl.ToString(),
                    Method = HttpMethod.Get,
                    Headers = _authenticationHeader,
                };
                var request = _httpClientProvider.BuildRequestMessage(httpRequestMessageSettings);

                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(MediaTypeNames.Application.Json));
                ReportProcessingStatus reportResponse = _httpClientProvider.SendRequestAndDeserializeAsync<ReportProcessingStatus>(request).GetAwaiter().GetResult();

                // Ensure the response was successful
                if (reportResponse != null && !string.IsNullOrEmpty(reportResponse?.ReportDocumentId))
                {
                    reportProcessingStatus = reportResponse;
                    _logMessage(LogLevel.Info, $"Successfully checked report status of {apiReport.ReportName} for the " +
                        $"marketPlaceId {queueItem.EntityID}.");
                }
                else
                {
                    reportProcessingStatus = new ReportProcessingStatus();
                    _logMessage(LogLevel.Info, $"Checking report status failed for the marketPlaceId {queueItem.EntityID}.");
                }
            });
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"Error while checking report status for the marketPlaceId {queueItem.EntityID}." +
                            $"|Exception:{ex.GetType().FullName}" +
                            $"|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }
        return await Task.FromResult(reportProcessingStatus);
    }

    public (string reportStatus, FileCollectionItem fileCollection) CheckReportStatusAndDownload(APIReportItem apiReportItem
                                                            , Action<Action> cancellableRetry
                                                            , OrderedQueue queueItem
                                                            , string integrationName
                                                            , int chunkSize)
    {
        _logMessage(LogLevel.Info, $"{apiReportItem.ReportName} initialized for the MarketplaceId " +
                                    $"{apiReportItem.MarketplaceId}.");

        string reportStatusResponse = string.Empty;
        FileCollectionItem fileCollectionItem = new FileCollectionItem();
        try
        {
            cancellableRetry(() =>
            {
                Dictionary<string, string> _authenticationHeader = new Dictionary<string, string>()
                                        { { "x-amz-access-token", _tokenApiClient.GetAccessTokenAsync(TokenDataSource.AmazonSellingPartner).GetAwaiter().GetResult() } };

                var reportsUrl = _credential.CredentialSet.DownloadReportUrl + "/" + apiReportItem.ReportDocumentId;

                var httpRequestMessageSettings = new HttpRequestOptions()
                {
                    Uri = reportsUrl.ToString(),
                    Method = HttpMethod.Get,
                    Headers = _authenticationHeader,
                };
                var request = _httpClientProvider.BuildRequestMessage(httpRequestMessageSettings);

                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(MediaTypeNames.Application.Json));
                var reportResponse = _httpClientProvider.SendRequestAndDeserializeAsync<ReportResponse>(request)?.Result;

                if (reportResponse != null && !string.IsNullOrEmpty(reportResponse.Url))
                {
                    var httpRequestOptions = new HttpRequestOptions()
                    {
                        Uri = reportResponse.Url.ToString(),
                        Method = HttpMethod.Get,
                    };
                    var httpRequestMessage = _httpClientProvider.BuildRequestMessage(httpRequestOptions);

                    Stream compressedStream = _httpClientProvider.DownloadFileStreamAsync(httpRequestMessage)
                                                                 .GetAwaiter().GetResult();

                    if (apiReportItem.Status == ReportStatus.FATAL.ToString())
                    {
                        using (var zipStream = new GZipStream(compressedStream, CompressionMode.Decompress))
                        {
                            using (var unzip = new StreamReader(zipStream))
                            {
                                string jsonResponse = unzip?.ReadToEnd();
                                _logMessage(LogLevel.Error, $"{apiReportItem.Status}:error for" +
                                $": {apiReportItem.ReportName} for the MarketplaceId {queueItem.EntityID} " +
                                $"and the CreateReportId {apiReportItem.ReportId} and jsonresponse:{jsonResponse}");

                                if (jsonResponse.Contains("The report data for the requested date range is not yet available"))
                                {
                                    reportStatusResponse = InternalReportStatus.FATAL_DUE_TO_UNAVAILABLE_DATA.ToString();
                                    _logMessage(LogLevel.Info, $"The report data for the requested date range is not yet available for" +
                                    $": {apiReportItem.ReportName} for the MarketplaceId {queueItem.EntityID} " +
                                    $"and the CreateReportId {apiReportItem.ReportId}");
                                }
                                else if (apiReportItem.ReportName.Contains("BrandAnalyticsRepeatPurchaseReport"))
                                {
                                    if (jsonResponse.Contains("Please double check that your parameters are valid and fulfill the requirements of the report type")
                                            || jsonResponse.Contains("dataStartTime must be a Sunday when reportPeriod=WEEK"))
                                    {
                                        reportStatusResponse = ReportStatus.FATAL.ToString();
                                        _logMessage(LogLevel.Info, $"{apiReportItem.ReportName} for the MarketplaceId {queueItem.EntityID}, " +
                                            $"valid paramters should follow : dataStartTime must be a Sunday and reportPeriod = WEEK" +
                                            $"and dataEndTime must be <= today's date");
                                    }
                                }
                                else
                                {
                                    reportStatusResponse = ReportStatus.FATAL.ToString();
                                }
                            }
                        }
                    }
                    else
                    {
                        fileCollectionItem = UploadReportToS3(compressedStream, apiReportItem, queueItem, integrationName, chunkSize);
                        _logMessage(LogLevel.Info, $"Uploaded report data to the S3 bucket " +
                                $": {apiReportItem.ReportName} for the MarketplaceId {queueItem.EntityID} " +
                                $"and for the CreateReportId {apiReportItem.ReportId} ");

                        reportStatusResponse = InternalReportStatus.COMPLETED.ToString();
                        _logMessage(LogLevel.Info, $"Successfully downloaded report {apiReportItem.ReportName} " +
                            $"for the MarketplaceId {queueItem.EntityID}, fileDate {queueItem.FileDate} " +
                            $"and for the CreateReportId {apiReportItem.ReportId}");
                    }

                }
                else
                {
                    _logMessage(LogLevel.Info, $"Not able to connect to report url : {apiReportItem.ReportName} " +
                                                $"for the MarketplaceId {apiReportItem.MarketplaceId}.");
                }
            });
        }
        catch (Exception ex)
        {
            _logException(LogLevel.Error, $"Error while downloading the report for the MarketplaceId " +
                            $"{queueItem.EntityID}.|Exception:{ex.GetType().FullName}" +
                            $"|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
            throw;
        }
        return (reportStatusResponse, fileCollectionItem);
    }

    public FileCollectionItem UploadReportToS3(Stream compressedStream
                                            , APIReportItem apiReportItem
                                            , OrderedQueue queueItem
                                            , string integrationName
                                            , int chunkSize)
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
                    UploadReportChunksToS3(fileStream, apiReportItem, queueItem, chunkSize, integrationName);
                }
                else
                {
                    UploadToS3(fileStream, apiReportItem, queueItem, integrationName);
                }
            }
            File.Delete(tempFilePath);
        }

        FileCollectionItem fileCollectionItem = new FileCollectionItem()
        {
            FileSize = reportFileSize,
            SourceFileName = apiReportItem.ReportName.ToLower(),
            FilePath = apiReportItem.ReportName
        };
        return fileCollectionItem;
    }

    public void UploadReportChunksToS3(FileStream decompressedStream, APIReportItem apiReportItem,
                                    OrderedQueue queueItem, int chunkSize, string integrationName)
    {
        long length = decompressedStream.Length;
        using (var streamReader = new StreamReader(decompressedStream, Encoding.UTF8))
        {
            long chunkFileSize = chunkSize * 1024 * 1024;
            int chunkIndex = 0;
            long currentChunkSize = 0;

            MemoryStream memoryStream = new MemoryStream();
            StreamWriter writer = new StreamWriter(memoryStream, Encoding.UTF8);
            JsonTextWriter jsonWriter = new JsonTextWriter(writer);
            jsonWriter.WriteStartArray(); // Start writing JSON array

            JsonSerializer serializer = new JsonSerializer();
            StringBuilder jsonBuffer = new StringBuilder();
            int openBraces = 0;

            string? line;
            while ((line = streamReader.ReadLine()) != null)
            {
                line = line.Trim();
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                // Track JSON object start and end
                foreach (char c in line)
                {
                    if (c == '{') openBraces++;
                    if (c == '}') openBraces--;
                }

                jsonBuffer.Append(line);

                // If we've matched all opening and closing braces, we have a complete JSON object
                if (openBraces == 0 && jsonBuffer.Length > 0)
                {
                    try
                    {
                        JObject jsonObject = JObject.Parse(jsonBuffer.ToString());
                        serializer.Serialize(jsonWriter, jsonObject);
                        jsonWriter.Flush();

                        currentChunkSize += Encoding.UTF8.GetByteCount(jsonObject.ToString());

                        // Reset buffer for next JSON object
                        jsonBuffer.Clear();

                        // Check if chunk size exceeded
                        if (currentChunkSize >= chunkFileSize)
                        {
                            jsonWriter.WriteEndArray(); // Close JSON array before upload
                            writer.Flush();
                            UploadToS3(memoryStream, apiReportItem, queueItem, integrationName, chunkIndex++);

                            // Reset for the next chunk
                            memoryStream.Dispose();
                            memoryStream = new MemoryStream();
                            writer = new StreamWriter(memoryStream, Encoding.UTF8);
                            jsonWriter = new JsonTextWriter(writer);
                            jsonWriter.WriteStartArray();
                            currentChunkSize = 0;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error parsing JSON object: {ex.Message}");
                        jsonBuffer.Clear();
                    }
                }
            }

            // Write remaining JSON objects if any
            jsonWriter.WriteEndArray();
            writer.Flush();

            if (memoryStream.Length > 0)
            {
                UploadToS3(memoryStream, apiReportItem, queueItem, integrationName, chunkIndex);
            }

            writer.Dispose();
            memoryStream.Dispose();
        }
    }

    public void UploadToS3(Stream compressedStream, APIReportItem apiReportItem, OrderedQueue queueItem
                                , string integrationName, int? chunkIndex = null)
    {
        var incomingFile = new StreamFile(compressedStream, _greenhouseS3Credential);
        string fileName = string.Empty;
        string integration = MyRegex().Replace(integrationName, ""); // To replace special characters
        //Raw/AmazonSellingpartnerApi/{marketplaceid}/{Filedate}/{ReportName}_{clientName}.json.gz
        //folder structure should be like above
        if (chunkIndex == null)
        {
            fileName = queueItem.FileGUID + "_" + apiReportItem.ReportName.ToLower().Replace(" ", "_") + "_" + integration.ToLower() + ".json";
        }
        else
        {
            fileName = queueItem.FileGUID + "_" + apiReportItem.ReportName.ToLower().Replace(" ", "_") + "_" + integration.ToLower() + "_" + chunkIndex.ToString() + ".json";
        }

        var path = _getS3PathHelper(queueItem.EntityID, queueItem.FileDate, fileName);
        var reportFile = new S3File(new Uri(path), _greenhouseS3Credential);
        _uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
    }

    [GeneratedRegex(@"[^a-zA-Z0-9\s]")]
    private static partial Regex MyRegex();
}
