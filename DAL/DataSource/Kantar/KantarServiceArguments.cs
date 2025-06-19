using Greenhouse.Data.DataSource.Kantar;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Greenhouse.DAL.DataSource.Kantar
{
    public record KantarServiceArguments
    (
        IHttpClientProvider HttpClientProvider,
        Credential Credential,
        Credential GreenhouseS3Credential,
        Integration Integration,
        Func<string, DateTime, string, string> GetS3PathHelper,
        IEnumerable<APIReport<ReportSettings>> ApiReports,
        string TaxonomyRootLevel,
        int MaxDegreeOfParallelism,
        Action<IFile, S3File, string[], long, bool> UploadToS3,
        Action<LogLevel, string> LogMessage,
        Action<LogLevel, string, Exception> LogException,
        CancellationToken cancellationToken
    );
}
