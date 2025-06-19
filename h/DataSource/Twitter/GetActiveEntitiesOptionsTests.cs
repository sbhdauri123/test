using Greenhouse.DAL.DataSource.Twitter;

namespace Greenhouse.DAL.Tests.Unit.DataSource.Twitter;

public class GetActiveEntitiesOptionsTests
{
    [Fact]
    public void Validate_WithValidOptions_ShouldNotThrow()
    {
        // Arrange
        GetActiveEntitiesOptions options = new()
        {
            AccountId = "123",
            Entity = "entity1",
            FileDate = DateTime.Now,
            Granularity = "DAY"
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
        GetActiveEntitiesOptions options = new()
        {
            AccountId = accountId,
            Entity = "entity1",
            FileDate = DateTime.Now,
            Granularity = "DAY"
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
    public void Validate_WithInvalidEntity_ShouldThrowArgumentException(string? entity)
    {
        // Arrange
        GetActiveEntitiesOptions options = new()
        {
            AccountId = "123",
            Entity = entity,
            FileDate = DateTime.Now,
            Granularity = "DAY"
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("Entity is required. (Parameter 'Entity')");
    }

    [Fact]
    public void Validate_WithDefaultFileDate_ShouldThrowArgumentException()
    {
        // Arrange
        GetActiveEntitiesOptions options = new()
        {
            AccountId = "123",
            Entity = "entity1",
            FileDate = default,
            Granularity = "DAY"
        };

        // Act & Assert
        options.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage("FileDate is required and must be a valid date. (Parameter 'FileDate')");
    }

    [Fact]
    public void Validate_WithValidFileDate_ShouldNotThrow()
    {
        // Arrange
        GetActiveEntitiesOptions options = new()
        {
            AccountId = "123",
            Entity = "entity1",
            FileDate = new DateTime(2024, 8, 30),
            Granularity = "DAY"
        };

        // Act & Assert
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }
}