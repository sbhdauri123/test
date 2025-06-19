using Greenhouse.JobService.Interfaces;
using Greenhouse.JobService.Strategies.Factory;

namespace Greenhouse.JobService.Tests.Unit.Strategies.Factory;

public class JobStrategyFactoryTests
{
    #region Constructor Tests

    [Fact]
    public void Constructor_WhenStrategiesIsNull_ShouldThrowArgumentNullException()
    {
        Func<JobStrategyFactory> act = () => new JobStrategyFactory(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WhenStrategiesHaveDuplicateKeys_ShouldThrowArgumentException()
    {
        // Arrange
        IJobStrategy? strategy1 = Substitute.For<IJobStrategy>();
        IJobStrategy? strategy2 = Substitute.For<IJobStrategy>();
        strategy1.ContractKey.Returns("duplicate-key");
        strategy2.ContractKey.Returns("duplicate-key");

        // Act
        Func<JobStrategyFactory> act = () => new JobStrategyFactory([strategy1, strategy2]);

        // Assert
        act.Should().Throw<ArgumentException>().Which.Message.Should()
            .Contain("An item with the same key has already been added");
    }

    [Fact]
    public void Constructor_WhenGivenValidStrategies_ShouldInitializeCorrectly()
    {
        // Arrange
        IJobStrategy? strategy1 = Substitute.For<IJobStrategy>();
        IJobStrategy? strategy2 = Substitute.For<IJobStrategy>();
        strategy1.ContractKey.Returns("key1");
        strategy2.ContractKey.Returns("key2");

        // Act
        JobStrategyFactory factory = new([strategy1, strategy2]);

        // Assert
        factory.GetStrategy("key1").Should().Be(strategy1);
        factory.GetStrategy("key2").Should().Be(strategy2);
    }

    [Fact]
    public void Constructor_WhenGivenEmptyStrategies_ShouldCreateEmptyDictionary()
    {
        // Arrange
        JobStrategyFactory factory = new([]);

        // Act
        Func<IJobStrategy> act = () => factory.GetStrategy("any-key");

        // Assert
        act.Should().Throw<KeyNotFoundException>();
    }

    [Fact]

    #endregion

    public void GetStrategy_WhenValidContractKey_ShouldReturnCorrectStrategy()
    {
        // Arrange
        IJobStrategy? strategy1 = Substitute.For<IJobStrategy>();
        IJobStrategy? strategy2 = Substitute.For<IJobStrategy>();
        strategy1.ContractKey.Returns("key1");
        strategy2.ContractKey.Returns("key2");

        JobStrategyFactory factory = new([strategy1, strategy2]);

        // Act
        IJobStrategy result = factory.GetStrategy("key1");

        // Assert
        result.Should().Be(strategy1);
    }

    [Fact]
    public void GetStrategy_WhenContractKeyDoesNotExist_ShouldThrowKeyNotFoundException()
    {
        // Arrange
        IJobStrategy? strategy = Substitute.For<IJobStrategy>();
        strategy.ContractKey.Returns("existing-key");

        JobStrategyFactory factory = new([strategy]);

        // Act
        Func<IJobStrategy> act = () => factory.GetStrategy("non-existing-key");

        // Assert
        act.Should().Throw<KeyNotFoundException>()
            .WithMessage("*No strategy found for contract key: non-existing-key*");
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void GetStrategy_WhenContractKeyIsEmpty_ShouldThrowKeyNotFoundException(string contractKey)
    {
        // Arrange
        IJobStrategy? strategy = Substitute.For<IJobStrategy>();
        strategy.ContractKey.Returns("existing-key");

        JobStrategyFactory factory = new([strategy]);

        // Act
        Func<IJobStrategy> act = () => factory.GetStrategy(contractKey);

        // Assert
        act.Should().Throw<KeyNotFoundException>();
    }

    [Fact]
    public void GetStrategy_WhenContractKeyIsNull_ShouldThrowKeyNotFoundException()
    {
        // Arrange
        IJobStrategy? strategy = Substitute.For<IJobStrategy>();
        strategy.ContractKey.Returns("existing-key");

        JobStrategyFactory factory = new([strategy]);

        // Act
        Func<IJobStrategy> act = () => factory.GetStrategy(null!);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("contractKey");
    }
}