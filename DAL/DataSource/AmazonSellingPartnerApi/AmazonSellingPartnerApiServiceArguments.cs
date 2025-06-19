using Greenhouse.Auth;
using Greenhouse.Data.DataSource.AmazonSellingPartnerApi;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;

namespace Greenhouse.DAL.DataSource.AmazonSellingPartnerApi;

public record AmazonSellingPartnerApiServiceArguments
(
    IHttpClientProvider HttpClientProvider,
    Credential Credential,
    Credential GreenhouseS3Credential,
    Func<string, DateTime, string, string> GetS3PathHelper,
    IEnumerable<APIReport<ReportSettings>> ApiReports,
    Action<IFile, S3File, string[], long, bool> UploadToS3,
    Action<LogLevel, string> LogMessage,
    Action<LogLevel, string, Exception> LogException,
    ITokenApiClient TokenApiClient
);
