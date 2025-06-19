using Greenhouse.Auth;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.AmazonAdsApi;

public interface IAmazonAdsApiOAuth
{
    Task<ResponseToken> GetOauthToken();

    Dictionary<string, string> PrepareRequestContent(bool isRefreshToken);

    string AccessToken { get; }
}
