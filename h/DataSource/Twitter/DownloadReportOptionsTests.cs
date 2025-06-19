using Greenhouse.DAL.DataSource.Twitter;

namespace Greenhouse.DAL.Tests.Unit.DataSource.Twitter;

public class DownloadReportOptionsValidationTests
{
    [Fact]
    public void Validate_WithValidOptions_ShouldNotThrow()
    {
        // Arrange
        DownloadReportOptions options = new()
        {
            AccountId = "123",
            ReportUrl = "https://api.twitter.com/2/reports/123.gz"
        };

        // Act & Assert
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Validate_WithInvalidAccountId_ShouldThrowArgumentException(string? accountId)
    {
        // Arrange
        DownloadReportOptions options = new()
        {
            AccountId = accountId,
            ReportUrl = "https://api.twitter.com/2/reports/123.gz"
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("AccountId is required. (Parameter 'AccountId')");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Validate_WithInvalidReportUrl_ShouldThrowArgumentException(string? reportUrl)
    {
        // Arrange
        DownloadReportOptions options = new() { AccountId = "123", ReportUrl = reportUrl };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("ReportUrl is required. (Parameter 'ReportUrl')");
    }

    [Theory]
    [InlineData("not a url")]
    [InlineData("https://")]
    public void Validate_WithInvalidUrlFormat_ShouldThrowArgumentException(string reportUrl)
    {
        // Arrange
        DownloadReportOptions options = new() { AccountId = "123", ReportUrl = reportUrl };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("ReportUrl must be a valid absolute URL. (Parameter 'ReportUrl')");
    }

    [Theory]
    [InlineData("http://api.twitter.com/2/reports/123.gz")]
    [InlineData("https://api.twitter.com/2/reports/123.gz")]
    [InlineData("https://example.com/path/to/report.json")]
    public void Validate_WithValidUrlFormats_ShouldNotThrow(string reportUrl)
    {
        // Arrange
        DownloadReportOptions options = new() { AccountId = "123", ReportUrl = reportUrl };

        // Act & Assert
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }
}