using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using Rebex.Net;
using System;
using System.IO;

namespace Greenhouse.Services.RemoteAccess
{
    public class SftpFile : IFile
    {
        public Uri Uri { get; private set; }
        public RemoteUri UriHelper { get; private set; }
        private readonly Sftp _client;
        internal Credential _credentials;

        private void ConnectClient()
        {
            if (!_client.IsConnected)
            {
                _client.Connect(_credentials.CredentialSet.ServerName, Convert.ToInt32(_credentials.CredentialSet.Port));
                _client.Login(_credentials.CredentialSet.Username, _credentials.CredentialSet.Password);
                _client.Settings = new SftpSettings() { DisableTransferQueue = true };
            }
        }

        public SftpFile(Uri uri, Credential creds)
        {
            this.Uri = uri;
            this.UriHelper = new RemoteUri(uri);
            this._credentials = creds;
            _client = RemoteClientCache.GetItem(Common.Constants.DeliveryType.SFTP, string.Empty, _credentials) as Sftp;

            Name = UriHelper.ResourceName;
            FullName = Uri.AbsolutePath;
            Extension = UriHelper.Extension;
            Directory = new SftpDirectory(new Uri(this.Uri.OriginalString.Replace(Name, string.Empty)), creds);
        }

        public IDirectory Directory { get; internal set; }
        public string Name { get; private set; }
        public string FullName { get; private set; }
        public string Extension { get; private set; }

        internal bool? _exists;
        public bool Exists
        {
            get
            {
                if (!_exists.HasValue)
                {
                    try
                    {
                        ConnectClient();
                        _exists = _client.FileExists(this.Uri.AbsolutePath);
                    }
                    catch
                    {
                        throw;
                    }
                    finally
                    {
                        _client.Disconnect();
                    }
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
                    try
                    {
                        ConnectClient();
                        _length = _client.GetFileLength(this.Uri.AbsolutePath);
                    }
                    catch
                    {
                        throw;
                    }
                    finally
                    {
                        _client.Disconnect();
                    }
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
                    try
                    {
                        ConnectClient();
                        _lastWriteTimeUtc = _client.GetFileDateTime(this.Uri.AbsolutePath).ToUniversalTime();
                    }
                    catch
                    {
                        throw;
                    }
                    finally
                    {
                        _client.Disconnect();
                    }
                }
                return _lastWriteTimeUtc.Value;
            }
        }

        public string ContentType
        {
            get
            {
                return UtilsIO.GetMimeTypeForFileExtension(this.Extension);
            }
        }

        public System.IO.Stream Create()
        {
            ConnectClient();
            try
            {
                return _client.GetUploadStream(this.Uri.AbsolutePath);
            }
            catch
            {
                throw;
            }
            finally
            {
                _exists = true;
            }
        }

        public void Delete()
        {
            try
            {
                ConnectClient();
                _client.DeleteFile(this.Uri.AbsolutePath);
            }
            catch
            {
                throw;
            }
            finally
            {
                _client.Disconnect();
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

        public Stream Get()
        {
            try
            {
                ConnectClient();
                _client.ChangeDirectory(this.Directory.FullName);
                return _client.GetDownloadStream(this.Name);
            }
            catch
            {
                throw;
            }
        }

        public void Put(Stream inputStream)
        {
            try
            {
                ConnectClient();
                _client.ChangeDirectory(this.Directory.FullName);
                _client.PutFile(inputStream, this.Name);
            }
            catch
            {
                throw;
            }
            finally
            {
                _client.Disconnect();
            }
        }

        public void CopyTo(IFile destFile, bool overwrite = false)
        {
            if (!overwrite && destFile.Exists)
            {
                return;
            }
            using (Stream inputStream = this.Get())
            {
                destFile.Put(inputStream);
            }
        }

        public void MoveTo(IFile destFile)
        {
            if (destFile is SftpFile)
            {
                try
                {
                    ConnectClient();
                    _client.Rename(this.FullName, destFile.FullName);
                }
                catch
                {
                    throw;
                }
                finally
                {
                    _client.Disconnect();
                }
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
