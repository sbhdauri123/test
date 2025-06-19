using Amazon.S3.Transfer;
using System.Threading.Tasks;

namespace Greenhouse.Services.RemoteAccess
{
    public interface IFileTransferUtility
    {
        Task UploadAsync(TransferUtilityUploadRequest utilityUploadRequest);
    }
}
