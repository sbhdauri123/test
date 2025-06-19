using Greenhouse.Data.DataSource.Snapchat;
using Greenhouse.Data.Model.Aggregate;

namespace Greenhouse.Utilities.Tests.Unit;

public class UtilsIOTests
{
    [Fact]
    public void DeepCloneJson_ShouldReturnDeepClonedObject()
    {
        // Arrange
        List<APIReport<ReportSettings>> reports =
        [
            new()
            {
                CreatedDate = DateTime.Now,
                LastUpdated = DateTime.Now,
                APIReportID = 6123,
                APIReportName = "ConversionAd",
                SourceID = 222,
                CredentialID = 118,
                IsActive = true,
                ReportSettingsJSON =
                    "{\"reportType\":\"conversions\", \"entity\":\"Campaigns\", \"breakdown\":\"ad\", \"granularity\":\"DAY\", \"omitEmpty\":\"true\", \"conversionSourceTypes\":\"web,app,total\", \"viewAttributionWindow\":\"1_DAY\", \"swipeUpAttributionWindow\":\"28_DAY\", \"limit\":\"25\"}",
                ReportFields = new List<APIReportField>
                {
                    new()
                    {
                        CreatedDate = DateTime.Now,
                        LastUpdated = DateTime.Now,
                        APIReportFieldID = 40224,
                        APIReportFieldName = "conversion_purchases",
                        APIReportID = 6123,
                        SortOrder = 0,
                        IsActive = true,
                        IsDimensionField = false
                    }
                }
            }
        ];

        // Act
        List<APIReport<ReportSettings>>? act = UtilsIO.DeepCloneJson(reports);

        // Assert
        act.Should().BeEquivalentTo(reports);
    }
}