using Greenhouse.Data.DataSource.AmazonAdsApi;
using Greenhouse.Data.DataSource.AmazonAdsApi.Responses;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.AmazonAdsApi;

public interface IAmazonAdsApiService
{
    Task<List<ProfileResponse>> GetProfilesDataAsync(AmazonAdsApiOAuth amazonAdsApiOAuth);

    Task<string> MakeCreateReportApiCallAsync(AmazonAdsApiOAuth amazonAdsApiOAuth
                                            , string jsonPayload, OrderedQueue queueItem
                                            , APIReport<ReportSettings> apiReport
                                            , Action<Action> cancellableRetry);

    (string reportStatus, FileCollectionItem fileCollection) CheckReportStatusAndDownload(AmazonAdsApiOAuth amazonAdsApiOAuth
                                                            , APIReportItem apiReportItem
                                                            , Action<Action> cancellableRetry
                                                            , OrderedQueue queueItem
                                                            , int chunkSize
                                                            , string reportFailureReason);

    FileCollectionItem UploadReportToS3(Stream decompressedStream, APIReportItem apiReportItem, OrderedQueue queueItem, int chunkSize);

    void UploadReportChunksToS3(FileStream decompressedStream, APIReportItem apiReportItem
                                        , OrderedQueue queueItem, int chunkSize);

    void UploadToS3(Stream compressedStream, APIReportItem apiReportItem, OrderedQueue queueItem, int? chunkIndex = null);

    Task<AdvertiserResponse> GetAdvertiserDataAsync(AmazonAdsApiOAuth amazonAdsApiOAuth, OrderedQueue queueItem
                                                                    , Action<Action> cancellableRetry, string index);

}
