using Greenhouse.Data.DataSource.TTD;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Net.Http;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.Jobs.TTD;

public class Common
{
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private readonly IHttpClientProvider httpClientProvider;

    public Common(IHttpClientProvider httpClientProvider) => this.httpClientProvider = httpClientProvider;

    public async Task<Authentication> GetTTDAuthAsync(int credentialId, int integrationId, string jobGuid)
    {
        var credential = Data.Services.SetupService.GetById<Data.Model.Setup.Credential>(credentialId);
        string fullURL = credential.CredentialSet.EndpointURI;

        var authHeader = new
        {
            Login = credential.CredentialSet.UserName,
            Password = credential.CredentialSet.Password,
            TokenExpirationInMinutes = 1440
        };

        try
        {
            return await httpClientProvider.SendRequestAndDeserializeAsync<Authentication>(new HttpRequestOptions
            {
                Uri = fullURL,
                Method = HttpMethod.Post,
                ContentType = MediaTypeNames.Application.Json,
                Content = new StringContent(JsonConvert.SerializeObject(authHeader), Encoding.UTF8,
                    MediaTypeNames.Application.Json),
                Headers = null
            });
        }
        catch (HttpClientProviderRequestException ex)
        {
            string errMsg = $"{jobGuid} - Failed to get TTD Auth token for integration: {integrationId}. Exception details: {ex}";
            logger.Log(Msg.Create(LogLevel.Error, logger.Name, errMsg));
            throw;
        }
        catch (Exception ex)
        {
            string errMsg = string.Format("{0} - Failed to get TTD Auth token for integration: {1}. Error: {2}", jobGuid, integrationId, ex.Message);
            logger.Log(Msg.Create(LogLevel.Error, logger.Name, errMsg));
            throw;
        }
    }
}
