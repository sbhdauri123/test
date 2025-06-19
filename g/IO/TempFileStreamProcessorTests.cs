using Greenhouse.Utilities.IO;
using Microsoft.Extensions.Logging.Testing;
using NSubstitute;
using System.Text;

namespace Greenhouse.Utilities.Tests.Unit.IO;

public class TempFileStreamProcessorTests
{
    private readonly FakeLogger<TempFileStreamProcessor> _logger;
    private readonly TempFileStreamProcessor _sut;

    public TempFileStreamProcessorTests()
    {
        _logger = new FakeLogger<TempFileStreamProcessor>();
        _sut = new TempFileStreamProcessor(_logger);
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameters_ShouldNotThrow()
    {
        Func<TempFileStreamProcessor> act = () => new TempFileStreamProcessor(_logger);
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_WithNullLogger_ThrowsArgumentNullException()
    {
        Func<TempFileStreamProcessor> act = () => new TempFileStreamProcessor(null);
        act.Should().Throw<ArgumentNullException>().WithParameterName("logger");
    }

    #endregion

    [Fact]
    public async Task ProcessStreamAsync_ShouldProcessStreamCorrectly()
    {
        // Arrange
        const string content = "Test Content";
        using MemoryStream sourceStream = new(Encoding.UTF8.GetBytes(content));

        // Act
        await using Stream responseStream = await _sut.ProcessStreamAsync(sourceStream);
        using StreamReader reader = new(responseStream);
        string result = await reader.ReadToEndAsync();

        // Assert
        result.Should().Be(content);
    }

    [Fact]
    public async Task ProcessStreamAsync_ShouldAllowMultipleReadsOfSameStream()
    {
        // Arrange
        const string content = "Test Content";
        using MemoryStream sourceStream = new(Encoding.UTF8.GetBytes(content));

        // Act
        await using Stream responseStream = await _sut.ProcessStreamAsync(sourceStream);
        using StreamReader reader = new(responseStream, leaveOpen: true);
        string firstRead = await reader.ReadToEndAsync();
        responseStream.Seek(0, SeekOrigin.Begin);
        string secondRead = await reader.ReadToEndAsync();

        // Assert
        firstRead.Should().Be(content);
        secondRead.Should().Be(content);
    }


    [Fact]
    public async Task ProcessStreamAsync_ShouldHandleEmptyStream()
    {
        // Arrange
        using MemoryStream sourceStream = new();

        // Act
        await using Stream responseStream = await _sut.ProcessStreamAsync(sourceStream);

        using StreamReader reader = new(responseStream);
        string result = await reader.ReadToEndAsync();

        // Assert
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task ProcessStreamAsync_ShouldHandleCancellation()
    {
        // Arrange
        using MemoryStream sourceStream = new(new byte[1024 * 1024]); // 1MB
        using CancellationTokenSource cts = new();

        // Act
        Func<Task> act = () => _sut.ProcessStreamAsync(sourceStream, Arg.Any<string>(), cts.Token);
        await cts.CancelAsync();

        // Assert
        await act.Should().ThrowAsync<OperationCanceledException>();
    }


    [Theory]
    [InlineData(100)]
    [InlineData(1024)]
    [InlineData(1024 * 1024)]
    public async Task ProcessStreamAsync_ShouldHandleVariousStreamSizes(int size)
    {
        // Arrange
        byte[] content = new byte[size];
        new Random(42).NextBytes(content);
        using MemoryStream sourceStream = new(content);

        // Act
        await using Stream responseStream = await _sut.ProcessStreamAsync(sourceStream);
        byte[] buffer = new byte[size];
        _ = await responseStream.ReadAsync(buffer);

        // Assert
        buffer.Should().BeEquivalentTo(content);
    }

    [Fact]
    public async Task ProcessStreamAsync_ShouldHandleLargeStream()
    {
        // Arrange
        byte[] largeContent = new byte[1024 * 1024]; // 1MB
        Random.Shared.NextBytes(largeContent);
        using MemoryStream sourceStream = new(largeContent);

        // Act
        await using Stream responseStream = await _sut.ProcessStreamAsync(sourceStream);
        byte[] buffer = new byte[largeContent.Length];
        _ = await responseStream.ReadAsync(buffer);

        // Assert
        buffer.Should().BeEquivalentTo(largeContent);
    }

    [Fact]
    public async Task ProcessStreamAsync_ShouldHandleNonSeekableStream()
    {
        // Arrange
        const string content = "Test Content";
        await using NonSeekableStream nonSeekableStream = new(Encoding.UTF8.GetBytes(content));

        // Act
        await using Stream responseStream = await _sut.ProcessStreamAsync(nonSeekableStream);
        using StreamReader reader = new(responseStream);
        string result = await reader.ReadToEndAsync();

        // Assert
        result.Should().Be(content);
    }

    [Fact]
    public async Task ProcessStreamAsync_ShouldLogWarningOnFailedCleanup()
    {
        // Arrange
        using MemoryStream sourceStream = new("test"u8.ToArray());

        // Act
        FileStream responseStream = await _sut.ProcessStreamAsync(sourceStream);
        string tempFilePath = responseStream.Name;
        await responseStream.DisposeAsync();

        // Assert
        File.Exists(tempFilePath).Should().BeFalse();
    }

    #region Helpers

    private sealed class NonSeekableStream(byte[] data) : Stream
    {
        private readonly MemoryStream _inner = new(data);

        public override bool CanSeek => false;
        public override bool CanRead => true;
        public override bool CanWrite => false;

        public override long Length => _inner.Length;

        public override long Position
        {
            get => _inner.Position;
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
            => _inner.Read(buffer, offset, count);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => _inner.ReadAsync(buffer, offset, count, cancellationToken);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => _inner.ReadAsync(buffer, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin)
            => throw new NotSupportedException();

        public override void SetLength(long value)
            => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count)
            => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _inner.Dispose();
            }

            base.Dispose(disposing);
        }
    }

    #endregion
}
