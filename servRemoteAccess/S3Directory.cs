using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Greenhouse.Services.RemoteAccess
{
    public class S3Directory : IDirectory
    {
        public Uri Uri { get; private set; }
        public RemoteUri UriHelper { get; private set; }
        private readonly DirectoryInfo _di;
        private readonly AmazonS3Uri _s3Uri;
        private readonly Credential _credentials;
        private readonly IAmazonS3 _s3Client;

        public S3Directory(Uri uri, Credential creds)
        {
            _credentials = creds;
            this.Uri = (uri.OriginalString.EndsWith(Constants.FORWARD_SLASH) ? uri : new Uri(string.Format("{0}/", uri.OriginalString)));
            this.UriHelper = new RemoteUri(uri);
            _di = new DirectoryInfo(this.Uri.AbsolutePath);
            _s3Uri = new AmazonS3Uri(this.Uri);
            _s3Uri.Region = RegionEndpoint.GetBySystemName(creds.CredentialSet.Region);
            _s3Client = RemoteClientCache.GetItem(Common.Constants.DeliveryType.S3, _s3Uri.Region.SystemName, _credentials) as IAmazonS3;

            _exists = this.DirectoryExists();
            var prefixes = _s3Uri.Key.Split(Constants.FORWARD_SLASH_ARRAY, StringSplitOptions.RemoveEmptyEntries);
            Name = prefixes.Last();
            FullName = Uri.AbsolutePath;
            Parent = prefixes.Length > 1 ? prefixes[^2] : null;
        }

        internal bool? _exists;
        public bool Exists
        {
            get
            {
                if (!_exists.HasValue)
                {
                    _exists = DirectoryExists();
                }
                return _exists.Value;
            }
        }

        private bool DirectoryExists()
        {
            bool hasDirectory = false;
            try
            {
                var request = new ListObjectsV2Request
                {
                    BucketName = _s3Uri.Bucket,
                    Prefix = _s3Uri.Key == null ? string.Empty : _s3Uri.Key,
                    MaxKeys = 1 // Set to 1 to minimize response data
                };

                var response = _s3Client.ListObjectsV2Async(request)?.Result;

                hasDirectory = response?.S3Objects.Count > 0;
            }
            catch (Exception)
            {
            }

            return hasDirectory;
        }

        public string Name { get; internal set; }
        public string FullName { get; internal set; }
        public string Parent { get; internal set; }

        public IEnumerable<IDirectory> GetDirectories()
        {
            // get the first tier only (based on usage in baseframeworkjob.DeleteExistingFileGuid):
            // DO GET /tier1
            // DONT GET /tier1/tier2

            return GetDirectoriesRecursive(false);
        }
        public IEnumerable<S3Directory> GetDirectoriesRecursive(bool recursive = false)
        {
            List<string> allDirectoryNames = new List<string>();

            ListObjectsV2Request request = new ListObjectsV2Request() { BucketName = _s3Uri.Bucket, Prefix = _s3Uri.Key };

            // remove the delimiter when we want to "recursively" get all keys matching the prefix
            if (!recursive)
                request.Delimiter = "/";

            ListObjectsV2Response response;
            do
            {
                response = _s3Client.ListObjectsV2Async(request)?.Result;

                IEnumerable<S3Object> folders = response.S3Objects.Where(x => x.Key.EndsWith(Constants.FORWARD_SLASH) && x.Size == 0);

                IEnumerable<string> folderPrefixes = folders.ToList().Select(x => x.Key).Distinct();

                if (response.CommonPrefixes.Count != 0)
                {
                    // for TTD-delivery, directories are not registering as s3 objects
                    // so workaround here is to get the directory folder names from the common prefixes
                    IEnumerable<string> nonObjectPrefixes = response.CommonPrefixes.Where(x => x.EndsWith(Constants.FORWARD_SLASH)).ToList().Distinct();
                    // remove the common endpoint prefix and leave only the new directory key
                    IEnumerable<string> keyPrefixes = nonObjectPrefixes.Except(folderPrefixes).Select(x => x.Remove(0, _s3Uri.Key.Length)).Distinct();
                    allDirectoryNames.AddRange(keyPrefixes);
                }

                if (recursive)
                {
                    // Get all common prefixes manually for nested "folders" (eg /folder/folder1/folder2)
                    var allKeyNames = response.S3Objects.ConvertAll(s3Object => s3Object.Key);

                    foreach (var keyName in allKeyNames)
                    {
                        string folderName = keyName;

                        // remove the stage file URI if present
                        if (keyName.Contains(_s3Uri.Key))
                            folderName = keyName.Substring(_s3Uri.Key.Length);

                        int lastDelimiterPosition = folderName.LastIndexOf('/');
                        if (lastDelimiterPosition == -1)
                            continue;

                        string newCommonPrefix = folderName.Substring(0, lastDelimiterPosition);

                        if (!allDirectoryNames.Contains(newCommonPrefix))
                            allDirectoryNames.Add(newCommonPrefix);
                    }

                    allDirectoryNames.AddRange(folderPrefixes.Except(allDirectoryNames));
                }
                else
                {
                    var primaryFolders = folderPrefixes.Select(name => name.Split(Constants.FORWARD_SLASH_ARRAY).First()).Distinct().ToList();
                    allDirectoryNames.AddRange(primaryFolders.Except(allDirectoryNames));
                }

                request.ContinuationToken = response.NextContinuationToken;
            } while (response.IsTruncated);

            List<S3Directory> dirs = new List<S3Directory>();

            if (allDirectoryNames.Count != 0)
                allDirectoryNames.ForEach(keyPrefix => dirs.Add(new S3Directory(RemoteUri.CombineUri(this.Uri, keyPrefix), _credentials)));

            return dirs.Where(x => x.Exists);
        }

        public IEnumerable<IFile> GetFiles(bool isRecursive = false)
        {
            if (isRecursive)
            {
                throw new NotImplementedException();
            }

            List<IFile> files = new List<IFile>();
            ListObjectsV2Request listObjsReq = new ListObjectsV2Request();
            listObjsReq.BucketName = _s3Uri.Bucket;
            listObjsReq.Prefix = UriHelper.FolderPath;
            listObjsReq.Delimiter = "/";

            var s3Objects = GetS3ObjectsAsync(listObjsReq).Result;

            files.AddRange(s3Objects.Select(s3File => new S3File(s3File, _s3Client, this)));

            return files.Where(x => x.Exists);
        }

        private async Task<List<S3Object>> GetS3ObjectsAsync(ListObjectsV2Request listObjsReq)
        {
            var s3ObjectList = new List<S3Object>();
            ListObjectsV2Response listObjsRes;
            do
            {
                //always fetch at least one batch
                listObjsRes = await _s3Client.ListObjectsV2Async(listObjsReq);
                //add all the manifest files
                s3ObjectList.AddRange(listObjsRes.S3Objects);

                listObjsReq.ContinuationToken = listObjsRes.NextContinuationToken;
            } while (listObjsRes.IsTruncated);

            return s3ObjectList;
        }

        public void Create()
        {
            bool hasObject = false;
            try
            {
                hasObject = System.Threading.Tasks.Task.Run(async () => { return await CreateAsync(_s3Uri.Key); }).Result;
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

        public async Task<bool> CreateAsync(string key)
        {
            if (this.Exists)
                return true;

            PutObjectRequest request = new PutObjectRequest
            {
                BucketName = _s3Uri.Bucket,
                Key = $"{key.TrimEnd('/')}/",
            };

            var response = await _s3Client.PutObjectAsync(request);
            var status = response.HttpStatusCode;
            return (status == System.Net.HttpStatusCode.OK);
        }

        public IDirectory CreateSubDirectory(string subDir)
        {
            S3Directory newSubDirectory = new S3Directory(RemoteUri.CombineUri(this.Uri, subDir), _credentials);
            AmazonS3Uri subDirUri = new AmazonS3Uri(newSubDirectory.Uri);

            ListObjectsV2Request request = new ListObjectsV2Request()
            {
                BucketName = _s3Uri.Bucket,
                Prefix = subDirUri.Key,
                MaxKeys = 1
            };

            ListObjectsV2Response response = _s3Client.ListObjectsV2Async(request)?.Result;

            if (response.S3Objects.Count == 0)
                newSubDirectory.Create();

            return newSubDirectory;
        }

        public void Delete(bool recursive)
        {
            bool hasObjects = true;
            try
            {
                bool isSuccess = false;

                // delete all files in sub-folders if recursive is true
                if (recursive)
                {
                    var subDirectories = this.GetDirectoriesRecursive(recursive);
                    if (subDirectories.Any())
                    {
                        foreach (S3Directory directory in subDirectories)
                        {
                            isSuccess = System.Threading.Tasks.Task.Run(async () => { return await directory.DeleteAsync(); }).Result;

                            if (!isSuccess)
                                break;
                        }
                    }
                }

                // delete all files in this folder
                isSuccess = System.Threading.Tasks.Task.Run(async () => { return await this.DeleteAsync(); }).Result;

                if (isSuccess)
                    hasObjects = false;
            }
            catch
            {
                throw;
            }
            finally
            {
                _exists = hasObjects;
            }
        }

        public async Task<bool> DeleteAsync()
        {
            if (!this.DirectoryExists())
                return true;

            var allFiles = this.GetFiles();

            var fileBatches = UtilsText.GetSublistFromList(allFiles, 1000);

            bool isSuccess = false;

            foreach (var batch in fileBatches)
            {
                DeleteObjectsRequest delRequest = new DeleteObjectsRequest()
                {
                    BucketName = _s3Uri.Bucket
                };

                List<S3File> s3Files = new List<S3File>();

                batch.ToList().ForEach(file => s3Files.Add((S3File)file));

                s3Files.ForEach(file => delRequest.AddKey(file.S3Uri.Key));

                var response = await _s3Client.DeleteObjectsAsync(delRequest);
                var status = response.HttpStatusCode;

                isSuccess = (status == System.Net.HttpStatusCode.OK || status == System.Net.HttpStatusCode.NoContent);

                if (!isSuccess)
                    break;
            }

            return isSuccess;
        }

        public override string ToString()
        {
            if (this.Uri != null)
            {
                return this.Uri.ToString();
            }
            return null;
        }

        /// <summary>
        /// Copy files from an s3 directory
        /// </summary>
        public List<S3File> CopyFiles(Func<List<S3File>, List<S3File>> fileFilterFunc, Func<S3File, Uri> getDestUriFunc)
        {
            List<S3File> files = new();
            ListObjectsV2Request listObjsReq = new();
            listObjsReq.BucketName = _s3Uri.Bucket;
            listObjsReq.Prefix = UriHelper.FolderPath;
            listObjsReq.Delimiter = "/";

            var s3Objects = GetS3ObjectsAsync(listObjsReq).Result;

            var directoryFiles = s3Objects.ConvertAll(s3File => new S3File(s3File, _s3Client, this));

            var sourceFiles = fileFilterFunc(directoryFiles);

            if (sourceFiles.Count == 0)
                return files;

            foreach (var sourceFile in sourceFiles)
            {
                Uri destUri = getDestUriFunc(sourceFile);
                S3File destFile = new(destUri, _credentials);
                sourceFile.CopyTo(destFile);
                files.Add(destFile);
            }

            return files;
        }
    }
}