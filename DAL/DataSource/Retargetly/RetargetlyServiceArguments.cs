using Greenhouse.Data.Model.Setup;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Threading;

namespace Greenhouse.DAL.DataSource.Retargetly
{
    public record RetargetlyServiceArguments
    (
        IHttpClientProvider HttpClientProvider,
        Credential GreenhouseS3Credential,
        Credential Credential,
        RetargetlyOAuth OAuth,
        Integration Integration,
        Func<string, DateTime, string, string> GetS3PathHelper,
        Action<IFile, S3File, string[], long, bool> UploadToS3,
        Action<LogLevel, string> LogMessage,
        Action<LogLevel, string, Exception> LogException,
        CancellationToken cts
    );
}
