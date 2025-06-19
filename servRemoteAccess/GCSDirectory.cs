using Google.Apis.Storage.v1;
using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Greenhouse.Services.RemoteAccess
{
    public class GCSDirectory : IDirectory
    {
        private readonly Credential _credentials;
        public Uri Uri { get; private set; }
        public RemoteUri UriHelper { get; private set; }
        private readonly StorageService _gcsClient;
        private readonly DirectoryInfo _di;
        private readonly IHttpClientProvider _httpClientProvider;
        private readonly Auth.GoogleOAuthAuthenticator _oauthAuthenticator;

        public GCSDirectory(Uri uri, Credential creds, IHttpClientProvider httpClientProvider, Auth.GoogleOAuthAuthenticator oauthAuthenticator)
        {
            this.Uri = (uri.OriginalString.EndsWith(Constants.FORWARD_SLASH) ? uri : new Uri(string.Format("{0}/", uri.OriginalString)));
            this.UriHelper = new RemoteUri(uri);
            this._credentials = creds;
            _di = new DirectoryInfo(this.Uri.AbsolutePath);
            _gcsClient = RemoteClientCache.GetItem(Common.Constants.DeliveryType.GCS, string.Empty, _credentials) as StorageService;
            Name = _di.Name;
            FullName = Uri.AbsolutePath;
            Parent = _di.Parent == null ? null : _di.Parent.Name;
            _httpClientProvider = httpClientProvider;
            _oauthAuthenticator = oauthAuthenticator;
        }

        public bool Exists
        {
            get
            {
                ObjectsResource.ListRequest req = new ObjectsResource.ListRequest(_gcsClient, this.UriHelper.Bucket);
                req.Prefix = this.UriHelper.FolderPath;
                req.MaxResults = 1;
                var result = req.Execute();
                return result.Items != null && result.Items.Count > 0;
            }
        }

        public string Name { get; internal set; }
        public string FullName { get; internal set; }
        public string Parent { get; internal set; }

        public IEnumerable<IDirectory> GetDirectories()
        {
            ObjectsResource.ListRequest req = new ObjectsResource.ListRequest(_gcsClient, this.UriHelper.Bucket);
            req.Prefix = this.UriHelper.FolderPath;
            req.Delimiter = "/";
            List<IDirectory> dirs = new List<IDirectory>();
            do
            {
                var res = req.Execute();
                if (res?.Prefixes == null)
                {
                    break;
                }

                var items = res.Prefixes.Select(i => i.ReplaceFirst(this.UriHelper.FolderPath, string.Empty)).Where(s => s.EndsWith('/') && s.Count(c => c == '/') == 1);
                dirs.AddRange(items.Select((x, i) => new GCSDirectory(RemoteUri.CombineUri(Uri, x), _credentials, _httpClientProvider, _oauthAuthenticator)));
                //check to see if there are more batches
                if (res.NextPageToken != null)
                {
                    req.PageToken = res.NextPageToken;
                }
                else
                {
                    req = null;
                }
            } while (req != null);

            return dirs;
        }

        public IEnumerable<IFile> GetFiles(bool isRecursive = false)
        {
            ObjectsResource.ListRequest req = new ObjectsResource.ListRequest(_gcsClient, this.UriHelper.Bucket);
            req.Prefix = this.UriHelper.FolderPath;

            List<IFile> files = new List<IFile>();

            // if not recursive, filter out subfolders and the files they contain
            bool pathContainsDirectory(string path) => path.ReplaceFirst(UriHelper.FolderPath, string.Empty).Any(c => c == '/');
            bool pathIsOrContainsDirectory(string path) => path.EndsWith('/') || pathContainsDirectory(path);
            bool allowAllFiles = isRecursive;
            bool isPathValid(string path) => allowAllFiles || !pathIsOrContainsDirectory(path);

            do
            {
                var res = req.Execute();

                if (res?.Items == null)
                {
                    break;
                }

                var items = res.Items.Where(i => isPathValid(i.Name));

                files.AddRange(items.Select((x, i) => new GCSFile(RemoteUri.CombineUri(Uri, x.Name.ReplaceFirst(this.UriHelper.FolderPath, string.Empty)),
                                                                    this._credentials,
                                                                    _httpClientProvider,
                                                                    _oauthAuthenticator
                                                                    )
                {
                    _lastWriteTimeUtc = x.Updated.HasValue ? x.Updated.Value.ToUniversalTime() : DateTime.MinValue.ToUniversalTime(),
                    _length = (long)x.Size.Value,
                    _exists = true,
                    _generation = x.Generation
                }));

                req.PageToken = res.NextPageToken;
            } while (req.PageToken != null);

            return files;
        }

        public void Create()
        {
            Google.Apis.Storage.v1.Data.Object newObj = new Google.Apis.Storage.v1.Data.Object() { Name = this.UriHelper.FolderPath, ContentType = "text/plain", Size = 0 };
            _gcsClient.Objects.Insert(newObj, this.UriHelper.Bucket, new System.IO.MemoryStream(), "text/plain").Upload();
        }

        public IDirectory CreateSubDirectory(string subDir)
        {
            GCSDirectory di = new GCSDirectory(new Uri(string.Format("{0}/{1}", this.Uri.OriginalString, subDir)), _credentials, _httpClientProvider, _oauthAuthenticator);
            Google.Apis.Storage.v1.Data.Object newObj = new Google.Apis.Storage.v1.Data.Object() { Name = string.Format("{0}{1}/", this.UriHelper.FolderPath, subDir), ContentType = "text/plain", Size = 0 };
            _gcsClient.Objects.Insert(newObj, this.UriHelper.Bucket, new System.IO.MemoryStream(), "text/plain").Upload();
            return di;
        }

        public void Delete(bool recursive)
        {
            if (!recursive)
            {
                var v = _gcsClient.Objects.Delete(this.UriHelper.Bucket, this.UriHelper.FolderPath).Execute();
            }
            else
            {
                ObjectsResource.ListRequest req = new ObjectsResource.ListRequest(_gcsClient, this.UriHelper.Bucket);
                req.Prefix = this.UriHelper.FolderPath;

                do
                {
                    var res = req.Execute();
                    foreach (Google.Apis.Storage.v1.Data.Object obj in res.Items)
                    {
                        ObjectsResource.DeleteRequest del = new ObjectsResource.DeleteRequest(_gcsClient, this.UriHelper.Bucket, obj.Name);
                        var v = del.Execute();
                    }
                    //check to see if there are more batches
                    if (res.NextPageToken != null)
                    {
                        req.PageToken = res.NextPageToken;
                    }
                    else
                    {
                        req = null;
                    }
                } while (req != null);
            }
        }

        public override string ToString()
        {
            if (this.Uri != null)
            {
                return this.Uri.ToString();
            }
            return null;
        }
    }
}
