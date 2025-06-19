using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;

namespace Greenhouse.Jobs.TTD;

using Greenhouse.Data.DataSource.TTD;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;

[Export("TTD-MetadataDeltaAPIImportJob", typeof(IDragoJob))]
public class MetadataDeltaAPIImport : Greenhouse.Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private IHttpClientProvider _httpClientProvider;
    private Common _common;
    private string getAdGroupUrl, getDeltaAdGroupUrl;
    private string getCampaignUrl, getDeltaCampaignUrl;

    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private string JobGUID { get { return this.JED.JobGUID.ToString(); } }
    private int delayInMilliseconds = 1000;

    private Authentication TTDAuth { get; set; }
    protected Uri baseDestUri;

    public string GetJobCacheKey()
    {
        return DefaultJobCacheKey;
    }

    public void PreExecute()
    {
        _httpClientProvider ??= base.HttpClientProvider;
        _common = new Common(_httpClientProvider);
        Lookup adgroupEndpointURL = SetupService.GetById<Lookup>(Constants.TTDADGROUPAPILOOKUPKEY);
        getAdGroupUrl = adgroupEndpointURL.Value;
        Lookup campaignEndpointURL = SetupService.GetById<Lookup>(Constants.TTDCAMPAIGNAPILOOKUPKEY);
        getCampaignUrl = campaignEndpointURL.Value;

        Lookup deltaAdgroupEndpointURL = SetupService.GetById<Lookup>(Constants.TTDDELTAADGROUPAPILOOKUPKEY);
        getDeltaAdGroupUrl = deltaAdgroupEndpointURL.Value;
        Lookup deltaCampaignEndpointURL = SetupService.GetById<Lookup>(Constants.TTDDELTACAMPAIGNAPILOOKUPKEY);
        getDeltaCampaignUrl = deltaCampaignEndpointURL.Value;

        Lookup delayInMillisecondLookup = SetupService.GetById<Lookup>(Constants.TTDDELAYBTWEENCALLS);

        delayInMilliseconds = int.Parse(delayInMillisecondLookup.Value);

        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        baseDestUri = GetDestinationFolder();
    }

    public void Execute()
    {
        this.TTDAuth = _common.GetTTDAuthAsync(CurrentIntegration.CredentialID, CurrentIntegration.IntegrationID, this.JobGUID).GetAwaiter().GetResult();
        string partnerId = GetCurrentIntegrationPartnerId();
        string serviceURL = GetCurrentIntegrationServiceUrl();

        var filesGeneratedInfo = new List<FileCollectionItem>();
        var dateCreated = DateTime.Now.ToUniversalTime();

        string[] paths = new string[] { partnerId.ToLower(), GetDatedPartition(dateCreated), GetHourPartition(dateCreated.Hour) };
        var queueItem = CreateNewQueueFile(partnerId, string.Join("/", paths), filesGeneratedInfo, dateCreated);

        List<string> advertisersIds = GetAdvertisersAsync(partnerId, serviceURL, filesGeneratedInfo, queueItem).GetAwaiter().GetResult();

        GetCampaignDeltaAsync(partnerId, advertisersIds.ToList(), filesGeneratedInfo, queueItem).GetAwaiter().GetResult();
        GetAdGroupDeltaAsync(partnerId, advertisersIds.ToList(), filesGeneratedInfo, queueItem).GetAwaiter().GetResult();

        if (filesGeneratedInfo.Count > 0)
        {
            queueItem.FileCollectionJSON = JsonConvert.SerializeObject(filesGeneratedInfo);
            JobService.Add(queueItem);
        }
        else
        {
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobGUID} - No metadata files generated during this job run."));
        }
    }

    private Queue CreateNewQueueFile(string partnerId, string path, List<FileCollectionItem> filesGenerated, DateTime timecreated)
    {
        return new Queue()
        {
            FileGUID = Guid.NewGuid(),
            FileName = path,
            EntityID = partnerId,
            IntegrationID = CurrentIntegration.IntegrationID,
            SourceID = CurrentSource.SourceID,
            Status = Greenhouse.Common.Constants.JobStatus.Complete.ToString(),
            StatusId = (int)Constants.JobStatus.Complete,
            JobLogID = this.JobLogger.JobLog.JobLogID,
            Step = JED.Step.ToString(),
            SourceFileName = path,
            FileDateHour = timecreated.Hour,
            FileDate = timecreated,
            FileSize = 0,
            FileCollectionJSON = JsonConvert.SerializeObject(filesGenerated)
        };
    }

    #region API request

    public async Task<List<string>> GetAdvertisersAsync(string partnerId, string serviceURL, List<FileCollectionItem> filesGeneratedInfo, Queue queueItem)
    {
        try
        {
            string json = $"{{'PartnerId': '{partnerId}','PageStartIndex': 0,'PageSize': null}}";
            HttpRequestMessage requestMessage = _httpClientProvider.BuildRequestMessage(new Utilities.HttpRequestOptions
            {
                Uri = serviceURL,
                Method = HttpMethod.Post,
                Content = new StringContent(json, Encoding.UTF8, "application/json"),
                ContentType = "application/json",
                Headers = new Dictionary<string, string>()
                {
                    {"TTD-Auth", this.TTDAuth.token }
                }
            });

            string responseString = await _httpClientProvider.SendRequestAsync(requestMessage);
            dynamic jsonObject = JObject.Parse(responseString);
            dynamic advertisers = jsonObject.Result;
            string fileContent = "{advertisers:[" + string.Join(",", advertisers) + "]}";

            string filename = "advertisers_" + GetEpochTime() + ".json";

            string[] paths = new string[] { partnerId.ToLower(), GetDatedPartition(queueItem.FileDate), GetHourPartition(queueItem.FileDateHour.Value), filename };
            Uri destUri = RemoteUri.CombineUri(this.baseDestUri, paths);
            S3File destFile = new S3File(destUri, GreenhouseS3Creds);

            byte[] byteArray = Encoding.UTF8.GetBytes(fileContent);

            using (MemoryStream stream = new MemoryStream(byteArray))
            {
                destFile.Put(stream);
            }

            filesGeneratedInfo.Add(new FileCollectionItem
            {
                SourceFileName = "advertiser",
                FilePath = filename,
                FileSize = 0
            });

            var advertiserIds = new List<string>();
            foreach (var advertiser in advertisers)
            {
                advertiserIds.Add(advertiser.AdvertiserId.ToString());
            }

            return advertiserIds;
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

    public async Task GetCampaignDeltaAsync(string partnerId, List<string> advertisersId, List<FileCollectionItem> filesGeneratedInfo, Queue queueItem)
    {
        Dictionary<string, string> advertiserTokens = GetAdvertisersData(Constants.CAMPAIGNTOKEN, partnerId);
        List<string> previousFailedCampaignCalls = GetFailedEntities(Constants.CAMPAIGNFAILED, partnerId);
        List<string> newFailedCampaignCalls = new List<string>();

        List<string> campaigns = new List<string>();

        try
        {
            foreach (var advertiserId in advertisersId)
            {
                string token;
                token = advertiserTokens.TryGetValue(advertiserId, out string value) ? value : "null";

                string json =
                    $"{{'AdvertiserId': '{advertiserId}','ReturnEntireCampaign': false,'LastChangeTrackingVersion': {token}}}";

                HttpRequestMessage requestMessage = _httpClientProvider.BuildRequestMessage(new Utilities.HttpRequestOptions
                {
                    Uri = getDeltaCampaignUrl,
                    Method = HttpMethod.Post,
                    Content = new StringContent(json, Encoding.UTF8, "application/json"),
                    ContentType = "application/json",
                    Headers = new Dictionary<string, string>()
                    {
                        {"TTD-Auth", this.TTDAuth.token }
                    }
                });
                await Task.Delay(delayInMilliseconds);
                string responseString = await _httpClientProvider.SendRequestAsync(requestMessage);

                dynamic jsonObject = JObject.Parse(responseString);
                dynamic campaignIds = jsonObject.ElementIds;

                token = ((long)jsonObject.LastChangeTrackingVersion.Value).ToString();
                advertiserTokens[advertiserId] = token;

                campaigns = await GetCampaignsAsync(campaignIds, newFailedCampaignCalls);

                if (campaigns.Count != 0)
                {
                    SaveCampaignsToFiles(partnerId, advertiserId, campaigns, filesGeneratedInfo, queueItem);
                    campaigns.ForEach(c => previousFailedCampaignCalls.Remove(c));
                }
            }

            if (previousFailedCampaignCalls.Count != 0)
            {
                campaigns = await GetCampaignsAsync(previousFailedCampaignCalls, newFailedCampaignCalls);
                if (campaigns.Count != 0)
                {
                    SaveCampaignsToFiles(partnerId, "retry", campaigns, filesGeneratedInfo, queueItem);
                }
            }
        }
        catch (HttpClientProviderRequestException ex)
        {
            string errMsg = $"{JobGUID} - {JED.TriggerName}. Error in extracting Campaigns in {nameof(GetCampaignDeltaAsync)} | Exception details : {ex}";
            logger.Log(Msg.Create(LogLevel.Error, logger.Name, errMsg, ex));
            throw;
        }
        catch (Exception ex)
        {
            string errMsg = string.Format("{0} - {1}. Error in extracting Campaigns in GetCampaignDelta", this.JobGUID,
                base.JED.TriggerName);
            logger.Log(Msg.Create(LogLevel.Error, logger.Name, errMsg, ex));

            throw;
        }

        AddOrUpdateAdvertisersToken(Constants.CAMPAIGNTOKEN, partnerId, advertiserTokens);
        AddOrUpdateFailedEntities(Constants.CAMPAIGNFAILED, partnerId, newFailedCampaignCalls);
    }

    private void SaveCampaignsToFiles(string partnerId, string advertiserId, List<string> campaigns, List<FileCollectionItem> filesGeneratedInfo,
        Queue queueItem)
    {
        string fileContent = "{Campaigns:[" + string.Join(",", campaigns) + "]}";
        string fileName = $"campaigns_advertiser_{advertiserId}_{GetEpochTime()}.json";

        string[] paths = new string[]
        {
            partnerId.ToLower(), GetDatedPartition(queueItem.FileDate), GetHourPartition(queueItem.FileDateHour.Value), fileName
        };

        Uri destUri = RemoteUri.CombineUri(this.baseDestUri, paths);
        S3File destFile = new S3File(destUri, GreenhouseS3Creds);

        byte[] byteArray = Encoding.UTF8.GetBytes(fileContent);

        using (MemoryStream stream = new MemoryStream(byteArray))
        {
            destFile.Put(stream);
        }

        filesGeneratedInfo.Add(new FileCollectionItem
        {
            SourceFileName = "campaign",
            FilePath = fileName,
            FileSize = 0
        });
    }

    private async Task<List<string>> GetCampaignsAsync(dynamic campaignIds, List<string> newFailedCampaignCalls)
    {
        var campaigns = new List<string>();
        foreach (dynamic campaignId in campaignIds)
        {
            try
            {
                HttpRequestMessage requestMessage = _httpClientProvider.BuildRequestMessage(new Utilities.HttpRequestOptions
                {
                    Uri = getCampaignUrl + campaignId,
                    Method = HttpMethod.Get,
                    Headers = new Dictionary<string, string>()
                    {
                        {"TTD-Auth", this.TTDAuth.token }
                    }
                });

                await Task.Delay(delayInMilliseconds);

                string campaignResponse = await _httpClientProvider.SendRequestAsync(requestMessage);
                campaigns.Add(campaignResponse);
            }
            catch (HttpClientProviderRequestException ex)
            {
                newFailedCampaignCalls.Add(campaignId.ToString());
                logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                    $"{JobGUID} - Error TTD Get campaign failed - campaignid={campaignId} | Exception details : {ex}"));

                continue;
            }
            catch (Exception e)
            {
                newFailedCampaignCalls.Add(campaignId.ToString());
                logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                    $"{this.JobGUID} - Error TTD Get campaign failed - campaignid={campaignId} message={e.Message}"));

                continue;
            }
        }

        return campaigns;
    }

    public async Task GetAdGroupDeltaAsync(string partnerId, List<string> advertisersId, List<FileCollectionItem> filesGeneratedInfo, Queue queueItem)
    {
        var advertiserTokens = GetAdvertisersData(Constants.ADGROUPTOKEN, partnerId);
        List<string> previousFailedAdGroupCalls = GetFailedEntities(Constants.ADGROUPFAILED, partnerId);
        List<string> newFailedAdGroupCalls = new List<string>();

        try
        {
            foreach (var advertiserId in advertisersId)
            {
                List<string> adgroups = new List<string>();

                string token = advertiserTokens.TryGetValue(advertiserId, out string value) ? value : "null";

                string json =
                    $"{{\"AdvertiserId\": \"{advertiserId}\",\"ReturnEntireAdGroup\": false,\"LastChangeTrackingVersion\": {token}}}";

                HttpRequestMessage requestMessage = _httpClientProvider.BuildRequestMessage(new Utilities.HttpRequestOptions
                {
                    Uri = getDeltaAdGroupUrl,
                    Method = HttpMethod.Post,
                    Content = new StringContent(json, Encoding.UTF8, "application/json"),
                    ContentType = "application/json",
                    Headers = new Dictionary<string, string>()
                    {
                        {"TTD-Auth", this.TTDAuth.token }
                    }
                });
                await Task.Delay(delayInMilliseconds);
                string responseString = await _httpClientProvider.SendRequestAsync(requestMessage);

                dynamic jsonObject = JObject.Parse(responseString);
                dynamic adGroupIds = jsonObject.ElementIds;
                token = ((long)jsonObject.LastChangeTrackingVersion.Value).ToString();
                advertiserTokens[advertiserId] = token;

                adgroups = await GetAdGroupsAsync(adGroupIds, newFailedAdGroupCalls);

                if (adgroups.Count != 0)
                {
                    SaveAdGroupsToFiles(partnerId, advertiserId, adgroups, filesGeneratedInfo, queueItem);
                    adgroups.ForEach(adg => previousFailedAdGroupCalls.Remove(adg));
                }
            }

            if (previousFailedAdGroupCalls.Count != 0)
            {
                List<string> adgroups = await GetAdGroupsAsync(previousFailedAdGroupCalls, newFailedAdGroupCalls);
                if (adgroups.Count != 0)
                {
                    SaveAdGroupsToFiles(partnerId, "retry", adgroups, filesGeneratedInfo, queueItem);
                }
            }
        }
        catch (HttpClientProviderRequestException ex)
        {
            string errMsg = $"{JobGUID} - {JED.TriggerName}. Error queuing TTD Metadata adGroup api file | Exception details : {ex}";
            logger.Log(Msg.Create(LogLevel.Error, logger.Name, errMsg, ex));
            throw;
        }
        catch (Exception ex)
        {
            string errMsg = string.Format("{0} - {1}. Error queuing TTD Metadata adGroup api file", this.JobGUID,
                base.JED.TriggerName);
            logger.Log(Msg.Create(LogLevel.Error, logger.Name, errMsg, ex));
            throw;
        }

        AddOrUpdateAdvertisersToken(Constants.ADGROUPTOKEN, partnerId, advertiserTokens);
        AddOrUpdateFailedEntities(Constants.ADGROUPFAILED, partnerId, newFailedAdGroupCalls);
    }

    private void SaveAdGroupsToFiles(string partnerId, string advertiserId, List<string> adgroups, List<FileCollectionItem> filesGeneratedInfo,
        Queue queueItem)
    {
        string fileName = $"adgroup_delta_advertiser_{advertiserId}_{GetEpochTime()}.json";
        string[] paths = new string[]
        {
            partnerId.ToLower(), GetDatedPartition(queueItem.FileDate), GetHourPartition(queueItem.FileDateHour.Value), fileName
        };

        Uri destUri = RemoteUri.CombineUri(this.baseDestUri, paths);
        S3File destFile = new S3File(destUri, GreenhouseS3Creds);
        string fileContent = "{adgroups:[" + string.Join(",", adgroups) + "]}";

        byte[] byteArray = Encoding.UTF8.GetBytes(fileContent);

        using (MemoryStream stream = new MemoryStream(byteArray))
        {
            destFile.Put(stream);
        }

        filesGeneratedInfo.Add(new FileCollectionItem
        {
            SourceFileName = "adgroup",
            FilePath = fileName,
            FileSize = 0
        });
    }

    private async Task<List<string>> GetAdGroupsAsync(dynamic adGroupIds, List<string> newFailedAdGroupCalls)
    {
        var adgroups = new List<string>();
        foreach (dynamic adGroupId in adGroupIds)
        {
            try
            {
                HttpRequestMessage requestMessage = _httpClientProvider.BuildRequestMessage(new Utilities.HttpRequestOptions
                {
                    Uri = getAdGroupUrl + adGroupId,
                    Method = HttpMethod.Get,
                    Headers = new Dictionary<string, string>()
                    {
                        {"TTD-Auth", this.TTDAuth.token }
                    }
                });

                await Task.Delay(delayInMilliseconds);
                string adGroupResponse = await _httpClientProvider.SendRequestAsync(requestMessage);

                adgroups.Add(adGroupResponse);
            }
            catch (HttpClientProviderRequestException exception)
            {
                newFailedAdGroupCalls.Add(adGroupId.ToString());
                logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                    $"{JobGUID} - Error TTD Get AdGroup failed - adGroupId={adGroupId} | Exception details : {exception}"));
                continue;
            }
            catch (Exception e)
            {
                newFailedAdGroupCalls.Add(adGroupId.ToString());
                logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                    $"{this.JobGUID} - Error TTD Get AdGroup failed - adGroupId={adGroupId} message={e.Message}"));
                continue;
            }
        }

        return adgroups;
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

    ~MetadataDeltaAPIImport()
    {
        Dispose(false);
    }

    private static string GetEpochTime()
    {
        TimeSpan t = DateTime.UtcNow - new DateTime(1970, 1, 1);
        double ms = t.TotalMilliseconds;
        return ms.ToString().Replace(".", "");
    }

    /// <summary>
    /// Retrieve the tokens of all the advertisers linked to a partner
    /// </summary>
    /// <param name="type">Type of token being cached</param>
    /// <param name="partnerId">Id of the partner to retrive the advertiser tokens from</param>
    /// <returns>Ditionnary of advertiserId-Token</returns>
    public static Dictionary<string, string> GetAdvertisersData(string type, string partnerId)
    {
        Lookup advertisersTokenJson = SetupService.GetById<Lookup>(type + partnerId);

        if (advertisersTokenJson == null)
        {
            return new Dictionary<string, string>();
        }

        var advertisersToken = JsonConvert.DeserializeObject<Dictionary<string, string>>(advertisersTokenJson.Value);

        return advertisersToken;
    }

    /// <summary>
    /// Save the token to the Configuration database
    /// </summary>
    /// <param name="type"></param>
    /// <param name="partnerId"></param>
    /// <param name="tokens"></param>
    public static void AddOrUpdateAdvertisersToken(string type, string partnerId, Dictionary<string, string> tokens)
    {
        string tokensJson = JsonConvert.SerializeObject(tokens);
        string key = type + partnerId;

        Lookup advertisersToken = SetupService.GetById<Lookup>(key);

        if (advertisersToken != null)
        {
            advertisersToken.Value = tokensJson;
            advertisersToken.LastUpdated = DateTime.Now;
            SetupService.Update(advertisersToken);
        }
        else
        {
            SetupService.InsertIntoLookup(key, tokensJson);
        }
    }

    public static List<string> GetFailedEntities(string type, string partnerId)
    {
        string key = type + partnerId;
        Lookup entityIds = SetupService.GetById<Lookup>(key);

        if (entityIds == null)
        {
            return new List<string>();
        }

        var failedGetEntities = JsonConvert.DeserializeObject<List<string>>(entityIds.Value);

        return failedGetEntities;
    }

    public static void AddOrUpdateFailedEntities(string type, string partnerId, List<string> newEntityIds)
    {
        string entityIdsJson = JsonConvert.SerializeObject(newEntityIds);
        string key = type + partnerId;
        Lookup oldEntityIds = SetupService.GetById<Lookup>(key);

        if (oldEntityIds != null)
        {
            oldEntityIds.Value = entityIdsJson;
            oldEntityIds.LastUpdated = DateTime.Now;
            SetupService.Update(oldEntityIds);
        }
        else
        {
            SetupService.InsertIntoLookup(key, entityIdsJson);
        }
    }

    public string GetCurrentIntegrationPartnerId()
    {
        return CurrentIntegration.EndpointURI.Substring(startIndex: CurrentIntegration.EndpointURI.LastIndexOf(Constants.FORWARD_SLASH) + 1);
    }

    public string GetCurrentIntegrationServiceUrl()
    {
        return CurrentIntegration.EndpointURI.Substring(startIndex: 0, length: CurrentIntegration.EndpointURI.Length - GetCurrentIntegrationPartnerId().Length - 1);
    }
}
