using Google.Apis.Auth.OAuth2.Responses;
using Google.Apis.Storage.v1;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.Services.RemoteAccess
{
    public class GCSFile : IFile
    {
        private readonly CompositeFormat GCS_BASE_URL = CompositeFormat.Parse(@"https://storage.googleapis.com/{0}/{1}");

        public Uri Uri { get; private set; }
        public RemoteUri UriHelper { get; private set; }
        internal Credential _credentials;
        private readonly StorageService _gcsClient;
        //private readonly Int32 _maxTimeOut = 3600000;
        private readonly Auth.GoogleOAuthAuthenticator _oauthAuthenticator;
        private readonly IHttpClientProvider _httpClientProvider;

        public GCSFile(Uri uri, Credential creds, IHttpClientProvider httpClientProvider,
            Auth.GoogleOAuthAuthenticator oauthAuthenticator)
        {
            Uri = uri;
            UriHelper = new RemoteUri(uri);
            _credentials = creds;
            _oauthAuthenticator = oauthAuthenticator;            
            _gcsClient = RemoteClientCache.GetItem(Common.Constants.DeliveryType.GCS, string.Empty, _credentials) as StorageService;
            Name = UriHelper.ResourceName;
            FullName = Uri.AbsolutePath;
            Extension = UriHelper.Extension;
            Directory = new GCSDirectory(new Uri(this.Uri.OriginalString.Replace(Name, string.Empty)), _credentials, httpClientProvider, _oauthAuthenticator);
            _httpClientProvider = httpClientProvider;
        }

        public IDirectory Directory { get; internal set; }
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

        internal DateTime? _lastWriteTimeUtc;
        public DateTime LastWriteTimeUtc
        {
            get
            {
                if (!_lastWriteTimeUtc.HasValue)
                {
                    ObjectsResource.ListRequest req = new ObjectsResource.ListRequest(_gcsClient, UriHelper.Bucket);
                    req.Prefix = UriHelper.Key;
                    var response = req.Execute();
                    if (response.Items != null && response.Items.Count == 1)
                    {
                        _lastWriteTimeUtc = (response.Items[0].Updated.HasValue ? response.Items[0].Updated.Value.ToUniversalTime() : DateTime.MinValue.ToUniversalTime());
                    }
                }
                return _lastWriteTimeUtc.Value;
            }
        }

        internal bool? _exists;
        public bool Exists
        {
            get
            {
                if (!_exists.HasValue)
                {
                    ObjectsResource.ListRequest req = new ObjectsResource.ListRequest(_gcsClient, UriHelper.Bucket);
                    req.Prefix = UriHelper.Key;
                    var response = req.Execute();
                    _exists = (response.Items != null && response.Items.Count > 0);
                }
                return _exists.Value;
            }
        }

        internal long _length = -1;
        public long Length
        {
            get
            {
                if (_length == -1)
                {
                    ObjectsResource.ListRequest req = new ObjectsResource.ListRequest(_gcsClient, UriHelper.Bucket);
                    req.Prefix = UriHelper.Key;
                    var response = req.Execute();
                    if (response.Items != null && response.Items.Count == 1)
                    {
                        _length = (long)response.Items[0].Size;
                    }
                }
                return _length;
            }
        }

        internal long? _generation;
        public long? Generation
        {
            get
            {
                if (!_generation.HasValue)
                {
                    var response = _gcsClient.Objects.Get(UriHelper.Bucket, UriHelper.Key).Execute();
                    _generation = response.Generation;
                }

                return _generation;
            }
        }

        public System.IO.Stream Create()
        {
            throw new NotImplementedException("Google GCS does not support writable streams at this time. If they ever decide to produce a non-alpha version of their SDK this can be easily implemented");
        }

        public void Delete()
        {
            try
            {
                var v = _gcsClient.Objects.Delete(UriHelper.Bucket, UriHelper.Key).Execute();
            }
            catch
            {
                throw;
            }
            finally
            {
                _exists = false;
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

        /// <summary>
        /// Uses a memory stream by default. Assign HttpClientProvider to avoid using default memory stream.
        /// Uses If-Generation-Match as a precondition to validate that the file has not been modified since.
        /// </summary>
        /// <exception cref="Google.GoogleApiException">System.Net.HttpStatusCode.PreconditionFailed</exception>
        public Stream Get()
        {
            var req = _gcsClient.Objects.Get(UriHelper.Bucket, UriHelper.Key);
            req.IfGenerationMatch = Generation;

            // exception is not thrown when returning a stream
            // so we make the same call here to throw an exception if the Generation does not match
            req.Execute();

            string remoteUrl = string.Format(null, GCS_BASE_URL, UriHelper.Bucket, UriHelper.Key);
            TokenResponse tokenResponse = _oauthAuthenticator.GetTokenResponse().GetAwaiter().GetResult();

            return _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
            {
                Uri = remoteUrl,
                Method = HttpMethod.Get,
                AuthScheme = "Bearer",
                AuthToken = tokenResponse.AccessToken,
                Headers = new Dictionary<string, string>
                {
                    { "IfGenerationMatch", Generation.ToString() }
                }
            }).GetAwaiter().GetResult();
        }

        public void Put(Stream inputStream)
        {
            Google.Apis.Storage.v1.Data.Object newObj = new Google.Apis.Storage.v1.Data.Object() { Name = UriHelper.Key, Size = (ulong)Length };
            _gcsClient.Objects.Insert(newObj, UriHelper.Bucket, inputStream, this.ContentType).Upload();
        }

        public void CopyTo(IFile destFile, bool overwrite = false)
        {
            if (!overwrite && destFile.Exists)
            {
                return;
            }

            if (destFile is GCSFile)
            {
                Google.Apis.Storage.v1.Data.Object newObj = new Google.Apis.Storage.v1.Data.Object() { Size = (ulong)Length, ContentType = destFile.ContentType };
                ObjectsResource.CopyRequest req = new ObjectsResource.CopyRequest(_gcsClient,
                    newObj,
                    this.UriHelper.Bucket,
                    this.UriHelper.Key,
                    destFile.UriHelper.Bucket,
                    destFile.UriHelper.Key);

                req.Execute();
            }
            else
            {
                using (Stream inputStream = this.Get())
                {
                    destFile.Put(inputStream);
                }
            }
        }

        public void MoveTo(IFile destFile)
        {
            if (destFile is GCSFile)
            {
                Google.Apis.Storage.v1.Data.Object newObj = new Google.Apis.Storage.v1.Data.Object() { Size = (ulong)Length, ContentType = destFile.ContentType };
                Google.Apis.Storage.v1.Data.RewriteResponse res = null;

                do
                {
                    ObjectsResource.RewriteRequest req = new ObjectsResource.RewriteRequest(_gcsClient,
                        newObj,
                        this.UriHelper.Bucket,
                        this.UriHelper.Key,
                        destFile.UriHelper.Bucket,
                        destFile.UriHelper.Key);

                    res = req.Execute();
                    if (res == null || !res.Done.HasValue) break;
                } while (!res.Done.Value);
                //GCS's shitty API doesn't actually know what 'move' means so it doesn't remove the object from it's initial location, we have to delete it.
                this.Delete();
            }
            else
            {
                using (Stream inputStream = this.Get())
                {
                    destFile.Put(inputStream);
                }
                this.Delete();
            }
        }
    }
}
