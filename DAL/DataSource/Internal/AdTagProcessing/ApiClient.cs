using Greenhouse.Auth;
using Greenhouse.Data.Model.AdTag.APIAdServer;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Internal.AdTagProcessing
{
    public class ApiClient : IApiClient
    {
        private readonly IHttpClientProvider _httpClientProvider;
        private readonly OAuthAuthenticator _oAuth;
        private string _baseUri;

        private string AuthToken
        {
            get
            {
                return _oAuth.GetAccessToken;
            }
        }

        public ApiClient(IHttpClientProvider httpClientProvider, OAuthAuthenticator oAuth, string uri)
        {
            _httpClientProvider = httpClientProvider;
            _oAuth = oAuth;
            _baseUri = uri;
        }

        public async Task<List<Placement>> GetAllDCMPlacementsAsync(GetAllDCMPlacementsOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            options.Validate();

            List<Placement> allPlacementsInGroup = new();

            #region Placements in PlacementGroup
            var placementIDs = await GetAllDCMPlacementIDByGroupAsync(options);
            var placementBatches = Utilities.UtilsText.GetSublistFromList<string>(placementIDs, 10);

            foreach (var batch in placementBatches)
            {
                var items = string.Join("&", batch.Select(x => $"ids={x}"));
                var placementList = await GetDCMPlacementsAsync(options.ProfileId, items);
                if (placementList?.Count > 0)
                {
                    allPlacementsInGroup.AddRange(placementList);
                }

            }
            #endregion

            //Get all other placements
            var placementsNotInGroup = await GetDCMPlacementsAsync(options);

            return allPlacementsInGroup.Union(placementsNotInGroup).ToList();
        }

        public async Task<List<string>> GetAllDCMPlacementIDByGroupAsync(GetAllDCMPlacementsOptions options)
        {
            string nextPageToken = null;
            List<string> placementIDs = new();

            do
            {
                string pageTokenQS = (!string.IsNullOrEmpty(nextPageToken)) ? $"&pageToken={HttpUtility.UrlEncode(nextPageToken)}" : "";
                var relativeUri = $"userprofiles/{options.ProfileId}/placementGroups/?minStartDate={options.StartDate.ToString("yyyy-MM-dd")}&advertiserIds={options.AdvertiserID}&sortOrder=DESCENDING&sortField=ID" + pageTokenQS;

                var request = BuildHttpRequestMessage(HttpMethod.Get, BuildUri(relativeUri), "application/json");
                var response = await _httpClientProvider.SendRequestAndDeserializeAsync<PlacementGroupResponse>(request);

                if (response?.PlacementGroups?.Count > 0)
                {
                    var placements = response.PlacementGroups.Where(x => x.PlacementIDs?.Count > 0)
                        .SelectMany(x => x.PlacementIDs);
                    placementIDs.AddRange(placements);
                    nextPageToken = response.NextPageToken;
                }
                else
                    nextPageToken = null;

            } while (nextPageToken != null);
            return placementIDs;
        }

        public async Task<List<Placement>> GetDCMPlacementsAsync(GetAllDCMPlacementsOptions options)
        {
            string nextPageToken = null;
            List<Placement> placementList = new List<Placement>();

            do
            {
                string pageTokenQS = (!string.IsNullOrEmpty(nextPageToken)) ? $"&pageToken={HttpUtility.UrlEncode(nextPageToken)}" : "";
                var relativeUri = $"userprofiles/{options.ProfileId}/placements/?minStartDate={options.StartDate.ToString("yyyy-MM-dd")}&advertiserIds={options.AdvertiserID}&sortOrder=DESCENDING&sortField=ID" + pageTokenQS;

                var request = BuildHttpRequestMessage(HttpMethod.Get, BuildUri(relativeUri), "application/json");
                var placementResponse = await _httpClientProvider.SendRequestAndDeserializeAsync<PlacementResponse>(request);

                if (placementResponse?.Placements?.Count > 0)
                {
                    placementList.AddRange(placementResponse.Placements);
                    nextPageToken = placementResponse.NextPageToken;
                }
                else
                    nextPageToken = null;

            } while (nextPageToken != null);

            return placementList;
        }

        public async Task<List<Placement>> GetDCMPlacementsAsync(string profileId, string placementIDs)
        {
            List<Placement> placements = new();

            PlacementResponse placementsResponse = null;
            do
            {
                var relativeUri = $"userprofiles/{profileId}/placements?{placementIDs}";

                if (!string.IsNullOrEmpty(placementsResponse?.NextPageToken))
                    relativeUri += $"&pageToken={HttpUtility.UrlEncode(placementsResponse.NextPageToken)}";


                var request = BuildHttpRequestMessage(HttpMethod.Get, BuildUri(relativeUri), "application/json");
                placementsResponse = await _httpClientProvider.SendRequestAndDeserializeAsync<PlacementResponse>(request);

                if (placementsResponse?.Placements.Count > 0)
                {
                    placements?.AddRange(placementsResponse.Placements);
                }

            } while (string.IsNullOrEmpty(placementIDs) && !string.IsNullOrEmpty(placementsResponse?.NextPageToken));

            return placements;
        }

        public async Task<string> UpdateDCMPlacementAsync(UpdateDCMPlacementOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            options.Validate();

            if (string.IsNullOrEmpty(options.Placement.TagSetting.AdditionalKeyValues))
            {
                return null;
            }

            var postData = JsonConvert.SerializeObject(options.Placement);

            string relativeUri = string.Format("userprofiles/{0}/placements?id={1}", options.ProfileId, options.Placement.Id);

            var request = BuildHttpRequestMessage(HttpMethod.Patch, BuildUri(relativeUri), "application/json", postData);
            var response = await _httpClientProvider.SendRequestAsync(request);

            return response;
        }

        private HttpRequestOptions BuildHttpRequestMessage(HttpMethod httpMethod, string uri, string contentType, string content)
        {
            return new HttpRequestOptions
            {
                Uri = uri,
                ContentType = contentType,
                Method = httpMethod,
                AuthToken = AuthToken,
                Content = new StringContent(content, Encoding.UTF8, contentType)
            };
        }

        private HttpRequestOptions BuildHttpRequestMessage(HttpMethod httpMethod, string uri, string contentType)
        {
            return new HttpRequestOptions
            {
                Uri = uri,
                ContentType = contentType,
                Method = httpMethod,
                AuthToken = AuthToken,
            };
        }

        private string BuildUri(string path) => $"{_baseUri}/{path}".TrimEnd('/');
    }
}
