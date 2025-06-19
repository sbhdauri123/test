using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;

namespace Greenhouse.Services.IntegrationTests
{
    public class S3FileIntegrationTests
    {
        [Fact]
        public void MultipartUpload_HugeCopyToMemoryStream_StreamTooLongError()
        {
            //Arrange
            string bucketName = "dev-datalake-americas";

            // source file
            string[] sourcePaths = new string[] { "integration-tests-assets", "S3FileIntegrationTests", "8b9e555f-2729-47e5-89b5-0d567296d13e_tv_detail.txt.gz" };
            S3File sourceFile = GetS3File(bucketName, sourcePaths);

            // destination file
            string[] destinationPaths = new string[] { "integration-tests-assets", "S3FileIntegrationTests", "destinationOutput", "8b9e555f-2729-47e5-89b5-0d567296d13e_tv_detail.txt.gz" };
            S3File destinationFile = GetS3File(bucketName, destinationPaths);

            //Act
            Action copyFileAction = () => CopyFileStreamToMemory(sourceFile, destinationFile, true);

            //Assert
            IOException exception = Assert.Throws<IOException>(copyFileAction);
            Assert.Equal("Stream was too long.", exception.Message);
        }
        [Fact]
        public void MultipartUpload_HugeStream_UploadSuccess()
        {
            //Arrange
            string bucketName = "dev-datalake-americas";
            Guid guid = Guid.NewGuid();

            // source file
            string[] sourcePaths = new string[] { "integration-tests-assets", "S3FileIntegrationTests", "8b9e555f-2729-47e5-89b5-0d567296d13e_tv_detail.txt.gz" };
            S3File sourceFile = GetS3File(bucketName, sourcePaths);

            // destination file
            string[] destinationPaths = new string[] { "integration-tests-assets", "S3FileIntegrationTests", "destinationOutput", $"{guid}_tv_detail.txt.gz" };
            S3File destinationFile = GetS3File(bucketName, destinationPaths);

            //Act
            Action copyFileAction = () => CopyFileStreamToMemory(sourceFile, destinationFile, false);

            //Assert
            Exception exception = Record.Exception(copyFileAction);
            Assert.Null(exception);
            destinationFile.Delete();
        }

        [Fact]
        public void MultipartUploadAsync_HugeStream_UploadSuccess()
        {
            //Arrange
            string bucketName = "dev-datalake-americas";
            Guid guid = Guid.NewGuid();

            // source file
            string[] sourcePaths = new string[] { "integration-tests-assets", "S3FileIntegrationTests", "8b9e555f-2729-47e5-89b5-0d567296d13e_tv_detail.txt.gz" };
            S3File sourceFile = GetS3File(bucketName, sourcePaths);

            // destination file
            string[] destinationPaths = new string[] { "integration-tests-assets", "S3FileIntegrationTests", "destinationOutput", $"{guid}_tv_detail.txt.gz" };
            S3File destinationFile = GetS3File(bucketName, destinationPaths);

            //Act
            Action copyFileAction = () => CopyFileStreamToMemoryAsync(sourceFile, destinationFile, false).GetAwaiter().GetResult();

            //Assert
            Exception exception = Record.Exception(copyFileAction);
            Assert.Null(exception);
            destinationFile.Delete();
        }

        private static S3File GetS3File(string bucketName, string[] s3Paths)
        {
            Uri baseUri = RemoteUri.GetServiceUri(Constants.URI_SCHEME_S3, Greenhouse.Configuration.Settings.Current.AWS.Region, bucketName);

            var destinationUri = RemoteUri.CombineUri(baseUri, s3Paths);

            var s3Creds = Credential.GetGreenhouseAWSCredential();

            S3File rawFile = new(destinationUri, s3Creds);
            return rawFile;
        }

        private static void CopyFileStreamToMemory(S3File sourceFile, S3File destinationFile, bool useMemoryStream = false)
        {
            var localPath = "C:\\Temp\\S3FileIntegrationTests";
            var localFileUri = RemoteUri.CombineUri(localPath, sourceFile.Name);
            var localFile = new FileSystemFile(localFileUri);

            if (!localFile.Directory.Exists)
            {
                localFile.Directory.Create();
            }

            sourceFile.CopyTo(localFile, true);
            var inputStream = localFile.Get();

            if (useMemoryStream)
            {
                using (MemoryStream memoryStream = new MemoryStream())
                {
                    inputStream.CopyTo(memoryStream);
                    destinationFile.S3MultiPartUpload(memoryStream);
                }
            }
            else
            {
                destinationFile.S3MultiPartUpload(inputStream);
            }

            localFile.Directory.Delete(true);
        }

        private static async Task CopyFileStreamToMemoryAsync(S3File sourceFile, S3File destinationFile, bool useMemoryStream = false)
        {
            var localPath = "C:\\Temp\\S3FileIntegrationTests";
            var localFileUri = RemoteUri.CombineUri(localPath, sourceFile.Name);
            var localFile = new FileSystemFile(localFileUri);

            if (!localFile.Directory.Exists)
            {
                localFile.Directory.Create();
            }

            sourceFile.CopyTo(localFile, true);
            var inputStream = localFile.Get();

            await destinationFile.S3MultiPartUploadAsync(inputStream);

            localFile.Directory.Delete(true);
        }
    }
}