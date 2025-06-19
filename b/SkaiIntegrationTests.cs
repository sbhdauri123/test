using Greenhouse.DAL.DataSource.Skai;
using Greenhouse.Data.DataSource.Skai;
using Greenhouse.Data.DataSource.Skai.CustomMetrics;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using Greenhouse.Utilities.IO;
using Microsoft.Extensions.Logging.Testing;

namespace Greenhouse.Jobs.IntegrationTests
{
    public class SkaiIntegrationTests
    {
        [Fact]
        public async Task GetCustomReportData()
        {
            //Arrange
            string apiHost = "https://services.kenshoo.com/api/v1";
            Credential skaiCredential = new("INSERT_ENCRYPTED_PASSWORD");
            string endpoint = "reports";
            HttpClient client = new();
            FakeLogger<TempFileStreamProcessor> logger = new();
            IStreamProcessor streamProcessor = new TempFileStreamProcessor(logger);
            HttpClientProvider httpClientProvider = new(client, streamProcessor);
            SkaiOAuth skaiOAuth = new(skaiCredential, apiHost, httpClientProvider);
            string serverID = "7246";

            ApiRequest reportRequest = new(apiHost, skaiOAuth, endpoint, httpClientProvider)
            {
                ServerID = serverID,
                MethodType = HttpMethod.Post,
                BodyRequest = new SyncReportRequest
                {
                    ProfileId = 410,
                    Entity = ReportEntity.CAMPAIGN.ToString(),
                    DateRange = new Date_Range { StartDate = "2024-05-09", EndDate = "2024-05-09" },
                    Limit = 10,
                    Fields = new[] { new CustomMetricField { Group = "ATTRIBUTES", Name = "profileID" } }
                }
            };

            reportRequest.SetParameters();

            //Act
            var reportResponse = await reportRequest.FetchDataAsync<SyncReportResponse>();

            //Assert
            Assert.NotNull(reportResponse);
        }
    }
}