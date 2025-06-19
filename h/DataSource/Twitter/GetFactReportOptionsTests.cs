using Greenhouse.DAL.DataSource.Twitter;

namespace Greenhouse.DAL.Tests.Unit.DataSource.Twitter;

public class GetFactReportOptionsTests
{
    [Fact]
    public void Validate_WithValidOptions_ShouldNotThrow()
    {
        // Arrange
        GetFactReportOptions options = new()
        {
            AccountId = "123",
            Entity = "entity1",
            Granularity = "DAY",
            Placement = "ALL_ON_TWITTER",
            MetricGroups = "ENGAGEMENT",
            ReportType = "ENTITY",
            FileDate = DateTime.Now,
            EntityIds = new List<string> { "456" }
        };

        // Act & Assert
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Fact]
    public void Validate_WithMissingAccountId_ShouldThrowArgumentException()
    {
        // Arrange
        GetFactReportOptions options = new()
        {
            Entity = "entity1",
            Granularity = "DAY",
            Placement = "ALL_ON_TWITTER",
            MetricGroups = "ENGAGEMENT",
            ReportType = "ENTITY",
            FileDate = DateTime.Now,
            EntityIds = new List<string> { "456" }
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("AccountId is required. (Parameter 'AccountId')");
    }

    [Fact]
    public void Validate_WithMissingEntity_ShouldThrowArgumentException()
    {
        // Arrange
        GetFactReportOptions options = new()
        {
            AccountId = "123",
            Granularity = "DAY",
            Placement = "ALL_ON_TWITTER",
            MetricGroups = "ENGAGEMENT",
            ReportType = "ENTITY",
            FileDate = DateTime.Now,
            EntityIds = new List<string> { "456" }
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("Entity is required. (Parameter 'Entity')");
    }

    [Fact]
    public void Validate_WithMissingGranularity_ShouldThrowArgumentException()
    {
        // Arrange
        GetFactReportOptions options = new()
        {
            AccountId = "123",
            Entity = "entity1",
            Placement = "ALL_ON_TWITTER",
            MetricGroups = "ENGAGEMENT",
            ReportType = "ENTITY",
            FileDate = DateTime.Now,
            EntityIds = new List<string> { "456" }
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("Granularity is required. (Parameter 'Granularity')");
    }

    [Fact]
    public void Validate_WithMissingPlacement_ShouldThrowArgumentException()
    {
        // Arrange
        GetFactReportOptions options = new()
        {
            AccountId = "123",
            Entity = "entity1",
            Granularity = "DAY",
            MetricGroups = "ENGAGEMENT",
            ReportType = "ENTITY",
            FileDate = DateTime.Now,
            EntityIds = new List<string> { "456" }
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("Placement is required. (Parameter 'Placement')");
    }

    [Fact]
    public void Validate_WithMissingMetricGroups_ShouldThrowArgumentException()
    {
        // Arrange
        GetFactReportOptions options = new()
        {
            AccountId = "123",
            Entity = "entity1",
            Granularity = "DAY",
            Placement = "ALL_ON_TWITTER",
            ReportType = "ENTITY",
            FileDate = DateTime.Now,
            EntityIds = new List<string> { "456" }
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("MetricGroups is required. (Parameter 'MetricGroups')");
    }

    [Fact]
    public void Validate_WithMissingReportType_ShouldThrowArgumentException()
    {
        // Arrange
        GetFactReportOptions options = new()
        {
            AccountId = "123",
            Entity = "entity1",
            Granularity = "DAY",
            Placement = "ALL_ON_TWITTER",
            MetricGroups = "ENGAGEMENT",
            FileDate = DateTime.Now,
            EntityIds = new List<string> { "456" }
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("ReportType is required. (Parameter 'ReportType')");
    }

    [Fact]
    public void Validate_WithDefaultFileDate_ShouldThrowArgumentException()
    {
        // Arrange
        GetFactReportOptions options = new()
        {
            AccountId = "123",
            Entity = "entity1",
            Granularity = "DAY",
            Placement = "ALL_ON_TWITTER",
            MetricGroups = "ENGAGEMENT",
            ReportType = "ENTITY",
            EntityIds = new List<string> { "456" }
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("FileDate is required and must be a valid date. (Parameter 'FileDate')");
    }

    [Fact]
    public void Validate_WithEmptyEntityIds_ShouldThrowArgumentException()
    {
        // Arrange
        GetFactReportOptions options = new()
        {
            AccountId = "123",
            Entity = "entity1",
            Granularity = "DAY",
            Placement = "ALL_ON_TWITTER",
            MetricGroups = "ENGAGEMENT",
            ReportType = "ENTITY",
            FileDate = DateTime.Now,
            EntityIds = new List<string>()
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("At least one EntityId is required. (Parameter 'EntityIds')");
    }

    [Fact]
    public void Validate_WithSegmentationTypeButNoValue_ShouldThrowArgumentException()
    {
        // Arrange
        GetFactReportOptions options = new()
        {
            AccountId = "123",
            Entity = "entity1",
            Granularity = "DAY",
            Placement = "ALL_ON_TWITTER",
            MetricGroups = "ENGAGEMENT",
            ReportType = "ENTITY",
            FileDate = DateTime.Now,
            EntityIds = new List<string> { "456" },
            SegmentationType = "AGE"
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage(
                "SegmentationValue is required when SegmentationType is provided. (Parameter 'SegmentationValue')");
    }

    [Fact]
    public void Validate_WithSegmentationValueButNoType_ShouldThrowArgumentException()
    {
        // Arrange
        GetFactReportOptions options = new()
        {
            AccountId = "123",
            Entity = "entity1",
            Granularity = "DAY",
            Placement = "ALL_ON_TWITTER",
            MetricGroups = "ENGAGEMENT",
            ReportType = "ENTITY",
            FileDate = DateTime.Now,
            EntityIds = new List<string> { "456" },
            SegmentationValue = "18-24"
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage(
                "SegmentationType is required when SegmentationValue is provided. (Parameter 'SegmentationType')");
    }
}