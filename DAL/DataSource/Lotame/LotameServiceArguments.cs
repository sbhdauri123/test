using Greenhouse.Data.Model.Setup;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Threading;

namespace Greenhouse.DAL.DataSource.Lotame
{
    public record LotameServiceArguments
    (
        IHttpClientProvider HttpClientProvider,
        Credential Credential,
        Credential GreenhouseS3Credential,
        Integration Integration,
        Func<string, DateTime, string, string> GetS3PathHelper,
        int MaxDegreeOfParallelism,
        int PageSize,
        Action<IFile, S3File, string[], long, bool> UploadToS3,
        Action<LogLevel, string> LogMessage,
        Action<LogLevel, string, Exception> LogException,
        CancellationToken cancellationToken
    );
}
