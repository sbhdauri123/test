using Greenhouse.Data.DataSource.AmazonSellingPartnerApi;
using Greenhouse.Data.DataSource.AmazonSellingPartnerApi.Responses;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.AmazonSellingPartnerApi;

public interface IAmazonSellingPartnerApiService
{
    Task<CreateReportResponse> RequestReportAsync(string jsonPayload, OrderedQueue queueItem
                                                        , APIReport<ReportSettings> apiReport
                                                        , Action<Action> cancellableRetry);

    Task<ReportProcessingStatus> GetReportStatusAndDocumentIdAsync(string reportId, OrderedQueue queueItem
                                                        , APIReportItem apiReport
                                                        , Action<Action> cancellableRetry);

    (string reportStatus, FileCollectionItem fileCollection) CheckReportStatusAndDownload(APIReportItem apiReportItem
                                                            , Action<Action> cancellableRetry
                                                            , OrderedQueue queueItem
                                                            , string integrationName
                                                            , int chunkSize);

    FileCollectionItem UploadReportToS3(Stream decompressedStream, APIReportItem apiReportItem, OrderedQueue queueItem
                                        , string integrationName, int chunkSize);

    void UploadReportChunksToS3(FileStream decompressedStream, APIReportItem apiReportItem
                                    , OrderedQueue queueItem, int chunkSize, string integrationName);

    void UploadToS3(Stream compressedStream, APIReportItem apiReportItem, OrderedQueue queueItem
                            , string integrationName, int? chunkIndex = null);

}
