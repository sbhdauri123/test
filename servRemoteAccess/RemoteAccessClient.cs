using Greenhouse.Auth;
using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using System;

namespace Greenhouse.Services.RemoteAccess
{
    public sealed class RemoteAccessClient : IDisposable
    {
        private readonly Credential _credentials;
        private readonly Uri _uri;
        private GoogleOAuthAuthenticator _oauthAuthenticator;

        public RemoteAccessClient(Uri rootUri, Credential creds)
        {
            _uri = rootUri;
            _credentials = creds;
        }

        public Constants.DeliveryType ClientType
        {
            get
            {
                return RemoteUri.GetDeliveryType(_uri);
            }
        }

        public IDirectory WithDirectory(Uri dirUri = null, IHttpClientProvider httpClientProvider = null)
        {
            dirUri = dirUri == null ? _uri : dirUri;
            IDirectory dir = null;
            switch (dirUri.Scheme)
            {
                case Constants.URI_SCHEME_FILE:
                    dir = new FileSystemDirectory(dirUri);
                    break;
                case Constants.URI_SCHEME_SFTP:
                    dir = new SftpDirectory(dirUri, _credentials);
                    break;
                case Constants.URI_SCHEME_S3:
                    dir = new S3Directory(dirUri, _credentials);
                    break;
                case Constants.URI_SCHEME_GCS:
                    
                    _oauthAuthenticator ??= new GoogleOAuthAuthenticator(
                        _credentials.CredentialSet.Username, _credentials.CredentialSet.ClientId,
                        _credentials.CredentialSet.ClientSecret, _credentials.CredentialSet.RefreshToken);
                    
                    dir = new GCSDirectory(dirUri, _credentials, httpClientProvider, _oauthAuthenticator);
                    break;
            }
            return dir;
        }

        public IFile WithFile(Uri fileUri = null, IHttpClientProvider httpClientProvider = null)
        {
            fileUri = fileUri == null ? _uri : fileUri;
            IFile file = null;
            switch (fileUri.Scheme)
            {
                case Constants.URI_SCHEME_FILE:
                    file = new FileSystemFile(fileUri);
                    break;
                case Constants.URI_SCHEME_SFTP:
                    file = new SftpFile(fileUri, _credentials);
                    break;
                case Constants.URI_SCHEME_S3:
                    file = new S3File(fileUri, _credentials);
                    break;
                case Constants.URI_SCHEME_GCS:
                    
                    _oauthAuthenticator ??= new GoogleOAuthAuthenticator(
                        _credentials.CredentialSet.Username, _credentials.CredentialSet.ClientId,
                        _credentials.CredentialSet.ClientSecret, _credentials.CredentialSet.RefreshToken);
                    
                    file = new GCSFile(fileUri, _credentials, httpClientProvider, _oauthAuthenticator);
                    break;
            }
            return file;
        }

        public void Dispose()
        {
            RemoteClientCache.ResetAll();
        }
    }
}
