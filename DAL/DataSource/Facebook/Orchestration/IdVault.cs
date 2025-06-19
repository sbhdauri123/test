using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.DAL.DataSource.Facebook.Orchestration;

/// <summary>
/// ID-Vault houses all ID lists for each level of the Facebook Campaign Hierarchy and at each step of the import job
/// </summary>
[Serializable]
public class IdVault
{
    [JsonProperty("account")]
    public string AccountID { get; set; }

    [JsonProperty("cubes")]
    public List<VaultCube> VaultCubes { get; set; } = new List<VaultCube>();

    public List<string> GetDownloadedIdList(ListAsset listAsset)
    {
        var downloadedDimensionList = new List<string>();

        RemoveExpiredCubes(listAsset);

        var downloadedCube = VaultCubes.Find(x => x.AccountWide && x.ListAsset == listAsset && x.ListType == ListType.DownloadComplete && x.LastUpdated.Date >= DateTime.UtcNow.Date);

        if (downloadedCube != null)
        {
            downloadedDimensionList.AddRange(downloadedCube.IdList);
        }

        return downloadedDimensionList;
    }

    private void RemoveExpiredCubes(ListAsset listAsset)
    {
        var expiredCubes = VaultCubes.FindAll(x => x.ListAsset == listAsset && x.ListType == ListType.DownloadComplete && x.LastUpdated.Date < DateTime.UtcNow.Date);

        if (expiredCubes.Count == 0)
        {
            return;
        }

        foreach (var cube in expiredCubes)
        {
            VaultCubes.Remove(cube);
        }
    }

    public void AddOrUpdateList(ListType listType, ListAsset listAsset, List<string> idList, long queueId)
    {
        if (idList.Count == 0)
            return;

        var existingList = this.VaultCubes.Find(x => x.QueueId == queueId && x.ListType == listType && x.ListAsset == listAsset);

        if (existingList == null)
        {
            var newList = new VaultCube
            {
                ListType = listType,
                ListAsset = listAsset,
                IdList = idList,
                QueueId = queueId,
                LastUpdated = DateTime.UtcNow
            };
            this.VaultCubes.Add(newList);
        }
        else
        {
            existingList.IdList.AddRange(idList);
            existingList.LastUpdated = DateTime.UtcNow;
        }
    }
}

/// <summary>
/// Represents a list of dimension IDs (ie ad, ad set, campaign)
/// having a "type" for each way the list was retrieved (full-set or only-with-impressions)
/// </summary>
public class VaultCube
{
    [JsonProperty("type")]
    public ListType ListType { get; set; }

    [JsonProperty("asset")]
    public ListAsset ListAsset { get; set; }

    [JsonProperty("list")]
    public List<string> IdList { get; set; }

    [JsonProperty("id")]
    public long QueueId { get; set; }

    public bool AccountWide
    {
        get
        {
            if (QueueId == 0)
                return true;
            return false;
        }
    }

    [JsonProperty("lastUpdated")]
    public DateTime LastUpdated { get; set; }
}

public enum ListAsset { Ad, AdSet, Campaign }

public enum ListType
{
    DownloadComplete // all dimension IDs downloaded
}
