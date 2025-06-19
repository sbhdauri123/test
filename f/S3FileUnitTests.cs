using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Greenhouse.Services.RemoteAccess;
using Moq;
using NSubstitute;
using System.Text;

namespace Greenhouse.Services.UnitTests
{
    public class S3FileUnitTests
    {
        private readonly Mock<IAmazonS3> _mockS3Client = new();
        private readonly IFileTransferUtility _subTransferUtility = Substitute.For<IFileTransferUtility>();
        private const long MAX_PUT_SIZE = 2147483648L; //2GB

        [Theory]
        [InlineData("testData", true)]
        [InlineData("testData", false)]
        public void PutFile_passHugeStream_uploadSuccessful(string content, bool isSeekable)
        {
            //Arrange
            var testStream = new FakeStream(Encoding.UTF8.GetBytes(content));
            testStream.SetSeek(isSeekable);
            testStream.SetLength(MAX_PUT_SIZE + 1);

            string bucketName = "dev-datalake-americas";
            string keyName = "greenhouseUnitTests/testFile.txt";

            _mockS3Client.Setup(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()));

            _subTransferUtility.UploadAsync(Arg.Any<TransferUtilityUploadRequest>()).Returns(Task.CompletedTask);

            var s3File = new S3File(_mockS3Client.Object, _subTransferUtility, bucketName, keyName);

            //Act
            s3File.Put(testStream);

            //Assert
            _mockS3Client.Verify(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()), Times.Never);
            _subTransferUtility.Received().UploadAsync(Arg.Is<TransferUtilityUploadRequest>(x => x.Key == keyName && x.BucketName == bucketName));
        }

        [Fact]
        public void PutFile_passSmallSeekableStream_uploadSuccessful()
        {
            //Arrange
            var testStream = new FakeStream(Encoding.UTF8.GetBytes("testData"));
            testStream.SetSeek(true);

            string bucketName = "dev-datalake-americas";
            string keyName = "greenhouseUnitTests/testFile.txt";

            _mockS3Client.Setup(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()));

            var s3File = new S3File(_mockS3Client.Object, _subTransferUtility, bucketName, keyName);

            //Act
            s3File.Put(testStream);

            //Assert
            _mockS3Client.Verify(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        }

        public class FakeStream : MemoryStream
        {
            public FakeStream(byte[] buffer) : base(buffer) { }

            private bool _isSeekable;

            public override bool CanSeek => _isSeekable;

            private long _contentLength;
            public override long Length
            {
                get
                {
                    return _contentLength;
                }
            }

            public override long Position
            {
                get => base.Position;
                set => throw new NotSupportedException();
            }

            public override long Seek(long offset, SeekOrigin origin) => throw new NotImplementedException();

            public override void SetLength(long value)
            {
                _contentLength = value;
            }

            public void SetSeek(bool isSeekable)
            {
                _isSeekable = isSeekable;
            }
        }
    }
}