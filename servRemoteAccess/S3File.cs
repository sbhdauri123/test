using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon.S3.Util;
using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Greenhouse.Services.RemoteAccess
{
    public class S3File : IFile
    {
        public const long MAX_PUT_SIZE = 2147483648L; //2GB
        public const long PART_SIZE = 262144000L; //250MB

        public Uri Uri { get; private set; }
        public RemoteUri UriHelper { get; private set; }
        private readonly Credential _credentials;
        private GetObjectMetadataResponse _fileInfo;
        private readonly IAmazonS3 _s3Client;
        private readonly IFileTransferUtility _transferUtility;

        public S3File(Uri uri, Credential creds)
        {
            _credentials = creds;
            this.Uri = uri;
            this.UriHelper = new RemoteUri(uri);
            S3Uri = new AmazonS3Uri(this.Uri);

            // get region from application setting b/c can no longer get region from URI in sdk-v3
            var awsRegion = RegionEndpoint.GetBySystemName(Configuration.Settings.Current.AWS.Region);
            S3Uri.Region = awsRegion;

            _s3Client = RemoteClientCache.GetItem(Common.Constants.DeliveryType.S3, S3Uri.Region?.SystemName, _credentials) as IAmazonS3;

            _exists = FileExists();

            var filename = S3Uri.Key.Split(Constants.FORWARD_SLASH_ARRAY).Last();
            Name = filename;
            FullName = this.Uri.AbsolutePath;
            Extension = UriHelper.Extension;
        }

        public S3File(S3Object s3Object, IAmazonS3 s3Client, S3Directory s3Directory)
        {
            _s3Client = s3Client;
            _exists = true;
            _length = s3Object.Size;
            _lastWriteTimeUtc = s3Object.LastModified.ToUniversalTime();
            Directory = s3Directory;
            Name = s3Object.Key.Split(Constants.FORWARD_SLASH_ARRAY).Last();

            FullName = $"/{s3Object.BucketName}/{s3Object.Key}";

            Uri s3ObjectUri = new Uri($"s3:/{FullName}");
            S3Uri = new AmazonS3Uri(s3ObjectUri);

            Extension = Path.GetExtension(Name);
        }

        /// <summary>
        /// Constructor created for purpose of making unit testing easier
        /// </summary>
        /// <param name="s3Client"></param>
        /// <param name="transferUtility"></param>
        /// <param name="bucketName"></param>
        /// <param name="keyName"></param>
        public S3File(IAmazonS3 s3Client, IFileTransferUtility transferUtility, string bucketName, string keyName)
        {
            _s3Client = s3Client;
            _transferUtility = transferUtility;
            FullName = $"/{bucketName}/{keyName}";
            Uri s3ObjectUri = new Uri($"s3:/{FullName}");
            S3Uri = new AmazonS3Uri(s3ObjectUri);
        }

        public IDirectory Directory { get; internal set; }

        public AmazonS3Uri S3Uri { get; internal set; }

        internal bool? _exists;
        public bool Exists
        {
            get
            {
                if (!_exists.HasValue)
                {
                    _exists = _fileInfo?.HttpStatusCode == System.Net.HttpStatusCode.OK;
                }
                return _exists.Value;
            }
        }

        private bool FileExists()
        {
            bool hasFile = false;
            try
            {
                var metaRequest = new GetObjectMetadataRequest() { BucketName = S3Uri.Bucket, Key = S3Uri.Key };
                _fileInfo = _s3Client.GetObjectMetadataAsync(metaRequest)?.Result;

                hasFile = true;
            }
            catch (Exception)
            {
                hasFile = false;
            }

            return hasFile;
        }

        internal long _length = -1;
        public long Length
        {
            get
            {
                if (_length == -1)
                {
                    _exists = FileExists();
                    _length = _fileInfo?.ContentLength ?? -1;
                }
                return _length;
            }
        }

        internal DateTime? _lastWriteTimeUtc;
        public DateTime LastWriteTimeUtc
        {
            get
            {
                if (!_lastWriteTimeUtc.HasValue)
                {
                    _exists = FileExists();
                    _lastWriteTimeUtc = _fileInfo.LastModified;
                }
                return _lastWriteTimeUtc.Value;
            }
        }

        public string Name { get; private set; }
        public string FullName { get; private set; }
        public string Extension { get; private set; }
        public string ContentType
        {
            get
            {
                return UtilsIO.GetMimeTypeForFileExtension(this.Extension);
            }
        }

        public Stream Create()
        {
            bool hasObject = false;
            try
            {
                hasObject = System.Threading.Tasks.Task.Run(async () => { return await CreateAsync(); }).Result;

                if (!hasObject)
                    return null;

                return this.Get();
            }
            catch
            {
                throw;
            }
            finally
            {
                _exists = hasObject;
            }
        }

        public async Task<bool> CreateAsync()
        {
            if (this.Exists)
                return true;

            PutObjectRequest request = new PutObjectRequest
            {
                BucketName = S3Uri.Bucket,
                Key = S3Uri.Key
            };

            var response = await _s3Client.PutObjectAsync(request);
            var status = response.HttpStatusCode;
            return (status == System.Net.HttpStatusCode.OK);
        }

        public void Delete()
        {
            bool hasObject = true;
            try
            {
                var isSuccess = System.Threading.Tasks.Task.Run(async () => { return await DeleteAsync(); }).Result;
                if (isSuccess)
                    hasObject = false;
            }
            catch
            {
                throw;
            }
            finally
            {
                _exists = hasObject;
            }
        }

        public async Task<bool> DeleteAsync()
        {
            if (!this.Exists)
                return true;
            Amazon.S3.Model.DeleteObjectRequest delRequest = new Amazon.S3.Model.DeleteObjectRequest()
            {
                BucketName = S3Uri.Bucket,
                Key = S3Uri.Key
            };
            var response = await _s3Client.DeleteObjectAsync(delRequest);
            var status = response.HttpStatusCode;
            return (status == System.Net.HttpStatusCode.OK || status == System.Net.HttpStatusCode.NoContent);
        }

        public void CopyTo(IFile destFile, bool overwrite = false)
        {
            //S3 to S3 is internally optimized
            if (destFile is S3File)
            {
                AmazonS3Uri destUri = new AmazonS3Uri(destFile.Uri);

                Amazon.S3.Model.CopyObjectRequest request = new Amazon.S3.Model.CopyObjectRequest
                {
                    SourceBucket = S3Uri.Bucket,
                    SourceKey = S3Uri.Key,
                    DestinationBucket = destUri.Bucket,
                    DestinationKey = destUri.Key
                };
                CopyObjectResponse response = _s3Client.CopyObjectAsync(request).Result;
            }
            else
            {
                if (overwrite || (!destFile.Exists))
                {
                    using (Stream inputStream = this.Get())
                    {
                        destFile.Put(inputStream);
                    }
                }
            }
        }

        public async Task CopyToAsync(S3File destFile)
        {
            AmazonS3Uri destUri = new(destFile.Uri);

            if (this.Length < MAX_PUT_SIZE)
            {
                CopyObjectRequest request = new()
                {
                    SourceBucket = S3Uri.Bucket,
                    SourceKey = S3Uri.Key,
                    DestinationBucket = destUri.Bucket,
                    DestinationKey = destUri.Key
                };
                await _s3Client.CopyObjectAsync(request);
            }
            else
            {
                await using Stream inputStream = await this.GetAsync();
                await destFile.S3MultiPartUploadAsync(inputStream);
            }
        }

        public void MoveTo(IFile destFile)
        {
            throw new NotImplementedException();
        }

        public override string ToString()
        {
            if (this.Uri != null)
            {
                return this.Uri.ToString();
            }
            return null;
        }

        public Stream Get()
        {
            if (this.Length < MAX_PUT_SIZE)
            {
                GetObjectRequest req = new GetObjectRequest() { BucketName = S3Uri.Bucket, Key = S3Uri.Key };
                GetObjectResponse res = _s3Client.GetObjectAsync(req).Result;
                return res.ResponseStream;
            }
            //file is larger than the max allowable GET size for AWS S3, must use the transfer utility and multi-part the file.
            //Note: this requires a fully seekable stream so that it can be chunked up, you cannot use a stream that you are 
            //reading in on the fly, it will not work
            else
            {
                TransferUtility tu = new TransferUtility(_s3Client);
                TransferUtilityOpenStreamRequest req = new TransferUtilityOpenStreamRequest() { BucketName = S3Uri.Bucket, Key = S3Uri.Key };

                return tu.OpenStream(req);
            }
        }

        public async Task<Stream> GetAsync()
        {
            if (this.Length < MAX_PUT_SIZE)
            {
                GetObjectRequest req = new GetObjectRequest() { BucketName = S3Uri.Bucket, Key = S3Uri.Key };
                GetObjectResponse res = await _s3Client.GetObjectAsync(req);
                return res.ResponseStream;

            }
            //file is larger than the max allowable GET size for AWS S3, must use the transfer utility and multi-part the file.
            //Note: this requires a fully seekable stream so that it can be chunked up, you cannot use a stream that you are 
            //reading in on the fly, it will not work
            else
            {
                TransferUtility tu = new TransferUtility(_s3Client);
                TransferUtilityOpenStreamRequest req = new TransferUtilityOpenStreamRequest() { BucketName = S3Uri.Bucket, Key = S3Uri.Key };

                return await tu.OpenStreamAsync(req);
            }
        }

        public void Put(Stream inputStream)
        {
            Put(inputStream, autoCloseStream: true);
        }

        public void Put(Stream inputStream, bool autoCloseStream = true)
        {
            //files smaller than 5GB can use the simple PUT, otherwise must use multipart
            if (inputStream.CanSeek && inputStream.Length < MAX_PUT_SIZE)
            {
                PutObjectRequest req = new() { BucketName = S3Uri.Bucket, Key = S3Uri.Key, InputStream = inputStream, AutoCloseStream = autoCloseStream };
                req.Metadata.Add("x-amz-meta-content-length", inputStream.Length.ToString());
                PutObjectResponse res = _s3Client.PutObjectAsync(req).GetAwaiter().GetResult(); //if you get a response the write succeded, AWS is all or nothing
            }
            //file is larger than the max allowable PUT size for AWS S3, must use the transfer utility and multi-part the file.
            //Note: this requires a fully seekable stream so that it can be chunked up, you cannot use a stream that you are 
            //reading in on the fly, it will not work
            else
            {
                if (inputStream.CanSeek)
                {
                    // avoid using memory stream if already seekable
                    // this prevents error Stream was too long"
                    S3MultiPartUpload(inputStream);
                }
                else
                {
                    using (MemoryStream memoryStream = new MemoryStream())
                    {
                        inputStream.CopyTo(memoryStream);
                        S3MultiPartUpload(memoryStream);
                    }
                }
            }
        }

        public void S3MultiPartUpload(Stream inputStream)
        {
            var tu = _transferUtility ?? new S3TransferUtility(_s3Client);
            TransferUtilityUploadRequest req = new() { BucketName = S3Uri.Bucket, Key = S3Uri.Key, InputStream = inputStream, AutoCloseStream = true };
            tu.UploadAsync(req).GetAwaiter().GetResult();
        }

        public async Task S3MultiPartUploadAsync(Stream inputStream)
        {
            var tu = _transferUtility ?? new S3TransferUtility(_s3Client);
            TransferUtilityUploadRequest req = new() { BucketName = S3Uri.Bucket, Key = S3Uri.Key, InputStream = inputStream, AutoCloseStream = true };
            await tu.UploadAsync(req);
        }
    }
}
