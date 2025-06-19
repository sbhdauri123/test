using Greenhouse.DAL.DataSource.Twitter;
using Greenhouse.Data.DataSource.Twitter;
using Greenhouse.Data.Model.Aggregate;

namespace Greenhouse.DAL.Tests.Unit.DataSource.Twitter;

public class DownloadDimensionFileOptionsValidationTests
{
    [Fact]
    public void Validate_WithValidOptions_ShouldNotThrow()
    {
        // Arrange
        DownloadDimensionFileOptions options = new()
        {
            AccountId = "123",
            EntityIds = new List<string> { "entity1" },
            Report = new APIReport<ReportSettings>()
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
        DownloadDimensionFileOptions options = new()
        {
            AccountId = accountId,
            EntityIds = new List<string> { "entity1" },
            Report = new APIReport<ReportSettings>()
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("AccountId is required. (Parameter 'AccountId')");
    }

    [Fact]
    public void Validate_WithNullEntityIds_ShouldThrowArgumentNullException()
    {
        // Arrange
        DownloadDimensionFileOptions options = new()
        {
            AccountId = "123",
            EntityIds = null,
            Report = new APIReport<ReportSettings>()
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentNullException>()
            .WithMessage("EntityIds cannot be null. (Parameter 'EntityIds')");
    }

    [Fact]
    public void Validate_WithNullReport_ShouldThrowArgumentNullException()
    {
        // Arrange
        DownloadDimensionFileOptions options = new()
        {
            AccountId = "123",
            EntityIds = new List<string> { "entity1" },
            Report = null
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentNullException>()
            .WithMessage("Report is required. (Parameter 'Report')");
    }

    [Fact]
    public void Validate_WithValidPageNumber_ShouldNotThrow()
    {
        // Arrange
        DownloadDimensionFileOptions options = new()
        {
            AccountId = "123",
            EntityIds = new List<string> { "entity1" },
            Report = new APIReport<ReportSettings>(),
        };

        // Act & Assert
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Fact]
    public void Validate_WithNullPageNumber_ShouldNotThrow()
    {
        // Arrange
        DownloadDimensionFileOptions options = new()
        {
            AccountId = "123",
            EntityIds = new List<string> { "entity1" },
            Report = new APIReport<ReportSettings>(),
        };

        // Act & Assert
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("someCursor")]
    public void Validate_WithDifferentCursorValues_ShouldNotThrow(string? cursor)
    {
        // Arrange
        DownloadDimensionFileOptions options = new()
        {
            AccountId = "123",
            EntityIds = new List<string> { "entity1" },
            Report = new APIReport<ReportSettings>(),
            Cursor = cursor
        };

        // Act & Assert
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }
}