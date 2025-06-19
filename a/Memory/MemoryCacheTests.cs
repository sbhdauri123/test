namespace Greenhouse.Caching.Tests.Unit.Memory;

public class MemoryCacheTests
{
    [Fact]
    public void MemoryCache_ShouldSetAndGet()
    {
        // Arrange
        System.Runtime.Caching.MemoryCache memoryCache = new("testCache");
        System.Runtime.Caching.CacheItemPolicy cacheItemPolicy =
            new() { AbsoluteExpiration = DateTime.Now.Add(TimeSpan.FromHours(1)) };

        const string cacheKey = "testKey";
        const string cacheValue = "testValue";

        // Act
        memoryCache.Set(cacheKey, cacheValue, cacheItemPolicy);

        // Assert
        string? result = memoryCache.Get(cacheKey) as string;
        result.Should().Be(cacheValue);
    }
}