using Greenhouse.DAL.DataSource.Twitter;

namespace Greenhouse.DAL.Tests.Unit.DataSource.Twitter;

public class GetReportRequestStatusOptionsTests
{
    [Fact]
    public void Validate_WithValidOptions_ShouldNotThrow()
    {
        // Arrange
        GetReportRequestStatusOptions options = new()
        {
            AccountId = "123",
            JobIds = new List<string> { "job1", "job2" }
        };

        // Act & Assert
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Fact]
    public void Validate_WithMissingAccountId_ShouldThrowArgumentException()
    {
        // Arrange
        GetReportRequestStatusOptions options = new() { JobIds = new List<string> { "job1", "job2" } };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("AccountId is required. (Parameter 'AccountId')");
    }

    [Fact]
    public void Validate_WithEmptyAccountId_ShouldThrowArgumentException()
    {
        // Arrange
        GetReportRequestStatusOptions options = new()
        {
            AccountId = "",
            JobIds = new List<string> { "job1", "job2" }
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("AccountId is required. (Parameter 'AccountId')");
    }

    [Fact]
    public void Validate_WithWhitespaceAccountId_ShouldThrowArgumentException()
    {
        // Arrange
        GetReportRequestStatusOptions options = new()
        {
            AccountId = "   ",
            JobIds = new List<string> { "job1", "job2" }
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("AccountId is required. (Parameter 'AccountId')");
    }

    [Fact]
    public void Validate_WithNullJobIds_ShouldThrowArgumentNullException()
    {
        // Arrange
        GetReportRequestStatusOptions options = new() { AccountId = "123", JobIds = null };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentNullException>()
            .WithMessage("JobIds is required. (Parameter 'JobIds')");
    }

    [Fact]
    public void Validate_WithEmptyJobIds_ShouldNotThrow()
    {
        // Arrange
        GetReportRequestStatusOptions options = new() { AccountId = "123", JobIds = new List<string>() };

        // Act & Assert
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }
}