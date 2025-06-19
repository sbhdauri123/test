using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using System;
using System.ComponentModel.Composition;
using System.Linq;

namespace Greenhouse.Jobs.TTD;

using Greenhouse.Data.DataSource.TTD;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

[Export("TTD-MetadataAPIImportJob", typeof(IDragoJob))]
public class MetadataImportAPI : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private IHttpClientProvider _httpClientProvider;
    private Common _common;
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private string JobGUID { get { return this.JED.JobGUID.ToString(); } }

    private Queue importFile;
    private const string metdataFileName = "metadata.json";

    private Uri baseDestUri;

    public string GetJobCacheKey()
    {
        return DefaultJobCacheKey;
    }

    public void PreExecute()
    {
        _httpClientProvider ??= base.HttpClientProvider;
        _common = new Common(_httpClientProvider);
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        baseDestUri = GetDestinationFolder();
    }

    public void Execute()
    {
        //check if already processed today's files
        var processedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);
        DateTime latestProcessedFileDate;
        if (processedFiles == null || !processedFiles.Any())
        {
            latestProcessedFileDate = CurrentIntegration.FileStartDate;
        }
        else
        {
            latestProcessedFileDate = processedFiles.Max(x => x.FileDate);
        }

        DateTime fileDate = DateTime.Today.AddDays(-1).ToUniversalTime();

        if (latestProcessedFileDate.Subtract(fileDate).Days == 0)
        {
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{2} - Integration: {0} has already been processed for file date: {1}", CurrentIntegration.IntegrationName, fileDate, JobGUID)));
            return;
        }

        int hour = 0;
        var endpointParts = base.CurrentIntegration.EndpointURI.Split('/');
        var entityId = endpointParts.Last();

        importFile = new Queue()
        {
            FileGUID = Guid.NewGuid(),
            FileName = metdataFileName,
            EntityID = entityId,
            IntegrationID = CurrentIntegration.IntegrationID,
            SourceID = CurrentSource.SourceID,
            Status = Greenhouse.Common.Constants.JobStatus.Complete.ToString(),
            StatusId = (int)Constants.JobStatus.Complete,
            JobLogID = this.JobLogger.JobLog.JobLogID,
            Step = JED.Step.ToString(),
            SourceFileName = "MetadataAPI",
            FileDateHour = hour,
            FileDate = fileDate
        };

        //API GetOverview
        GetPartnerOverviewAsync(importFile).GetAwaiter().GetResult();
    }

    #region API request
    public async Task GetPartnerOverviewAsync(Queue importFile)
    {
        try
        {
            //https://api.thetradedesk.com/v3/overview/partner/ykzufg0
            Authentication authentication = await _common.GetTTDAuthAsync(CurrentIntegration.CredentialID, CurrentIntegration.IntegrationID, this.JobGUID);

            string fullURL = CurrentIntegration.EndpointURI;
            HttpRequestMessage requestMessage = _httpClientProvider.BuildRequestMessage(new Utilities.HttpRequestOptions
            {
                Uri = fullURL,
                Method = HttpMethod.Get,
                Headers = new Dictionary<string, string>
                {
                    { "TTD-Auth", authentication.token },
                    { "Accept", "application/json" }
                }
            });
            string response = await _httpClientProvider.SendRequestAsync(requestMessage);

            string[] paths = new string[] { importFile.EntityID.ToLower(), GetDatedPartition(importFile.FileDate), importFile.FileName };
            Uri destUri = RemoteUri.CombineUri(this.baseDestUri, paths);
            S3File destFile = new S3File(destUri, GreenhouseS3Creds);

            using (Stream inStream = new MemoryStream(Encoding.UTF8.GetBytes(response)))
            {
                destFile.Put(inStream);
            }
            importFile.FileSize = destFile.Length;

            Data.Services.JobService.Add(importFile);
        }
        catch (HttpClientProviderRequestException ex)
        {
            string errMsg = $"{JobGUID} - {JED.TriggerName}. Error queuing TTD metadata api file | Exception details : {ex}";
            logger.Log(Msg.Create(LogLevel.Error, logger.Name, errMsg, ex));
            throw;
        }
        catch (Exception ex)
        {
            string errMsg = string.Format("{0} - {1}. Error queuing TTD metadata api file", this.JobGUID, base.JED.TriggerName);
            logger.Log(Msg.Create(LogLevel.Error, logger.Name, errMsg, ex));
            throw;
        }
    }

    #endregion

    public void PostExecute()
    {
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {

        }
    }

    ~MetadataImportAPI()
    {
        Dispose(false);
    }
}
