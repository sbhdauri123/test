using Amazon.S3;
using Amazon.S3.Transfer;
using System;
using System.Threading.Tasks;

namespace Greenhouse.Services.RemoteAccess
{
    public class S3TransferUtility : IFileTransferUtility
    {
        private readonly IAmazonS3 _s3Client;

        public S3TransferUtility(IAmazonS3 client)
        {
            this._s3Client = client;
        }

        public async Task UploadAsync(TransferUtilityUploadRequest uploadRequest)
        {
            try
            {
                TransferUtility fileTransferUtility = new(_s3Client);
                await fileTransferUtility.UploadAsync(uploadRequest);
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
