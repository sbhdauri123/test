using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using Rebex.Net;
using System;
using System.Collections.Generic;
using System.IO;

namespace Greenhouse.Services.RemoteAccess
{
    public class SftpDirectory : IDirectory
    {
        public Uri Uri { get; private set; }
        public RemoteUri UriHelper { get; private set; }
        private readonly DirectoryInfo _di;
        private readonly Sftp _client;
        private readonly Credential _credentials;

        public SftpDirectory(Uri uri, Credential creds)
        {
            _credentials = creds;
            this.Uri = (uri.OriginalString.EndsWith(Constants.FORWARD_SLASH) ? uri : new Uri(string.Format("{0}/", uri.OriginalString)));
            this.UriHelper = new RemoteUri(uri);
            this._credentials = creds;
            _di = new DirectoryInfo(this.Uri.AbsolutePath);
            _client = RemoteClientCache.GetItem(Common.Constants.DeliveryType.SFTP, string.Empty, _credentials) as Sftp;
            Name = _di.Name;
            FullName = Uri.AbsolutePath;
            Parent = _di.Parent == null ? null : _di.Parent.Name;
        }

        private void ConnectClient()
        {
            if (!_client.IsConnected)
            {
                _client.Connect(_credentials.CredentialSet.ServerName, Convert.ToInt32(_credentials.CredentialSet.Port));
                _client.Login(_credentials.CredentialSet.Username, _credentials.CredentialSet.Password);
            }
        }

        public bool Exists
        {
            get
            {
                ConnectClient();
                try
                {
                    return _client.DirectoryExists(this.Uri.AbsolutePath);
                }
                catch
                {
                    return false;
                }
                finally
                {
                    _client.Disconnect();
                }
            }
        }

        public string Name { get; private set; }
        public string FullName { get; private set; }
        public string Parent { get; private set; }

        public IEnumerable<IDirectory> GetDirectories()
        {
            List<IDirectory> dirs = new List<IDirectory>();
            try
            {
                ConnectClient();
                SftpItemCollection items = _client.GetList(this.FullName);
                foreach (SftpItem item in items)
                {
                    if (item.IsDirectory)
                    {
                        dirs.Add(new SftpDirectory(RemoteUri.CombineUri(Uri, item.Name), _credentials));
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                _client.Disconnect();
            }
            return dirs;
        }

        public IEnumerable<IFile> GetFiles(bool isRecursive = false)
        {
            if (isRecursive)
            {
                throw new NotImplementedException();
            }

            List<IFile> files = new List<IFile>();
            try
            {
                ConnectClient();
                SftpItemCollection items = _client.GetList(this.FullName);
                foreach (SftpItem item in items)
                {
                    if (item.IsFile)
                    {
                        files.Add(new SftpFile(RemoteUri.CombineUri(Uri, item.Name), this._credentials)
                        {
                            Directory = this,
                            _lastWriteTimeUtc = (item.LastWriteTime.HasValue ? item.LastWriteTime.Value.ToUniversalTime() : DateTime.MinValue.ToUniversalTime()),
                            _length = item.Length,
                            _exists = true
                        });
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                _client.Disconnect();
            }

            return files;
        }

        public void Create()
        {
            ConnectClient();
            try
            {
                //make sure parent directory structure exists first, otherwise create
                if (this.UriHelper.FolderDepth > 1)
                {
                    foreach (string dir in this.UriHelper.FolderPath.Split(Constants.FORWARD_SLASH_ARRAY, StringSplitOptions.RemoveEmptyEntries))
                    {
                        if (!_client.DirectoryExists(dir))
                        {
                            _client.CreateDirectory(dir);
                        }

                        _client.ChangeDirectory(dir);
                    }

                    _client.ChangeDirectory(string.Format("/{0}", this.UriHelper.Bucket));
                }
                else
                {
                    _client.CreateDirectory(this.FullName);
                }
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

        public IDirectory CreateSubDirectory(string subDir)
        {
            ConnectClient();
            try
            {
                SftpDirectory di = new SftpDirectory(new Uri(string.Format("{0}/{1}", this.Uri.OriginalString, subDir)), _credentials);
                _client.CreateDirectory(di.FullName);
                return di;
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

        public void Delete(bool recursive)
        {
            ConnectClient();
            try
            {
                Rebex.IO.TraversalMode mode = (recursive ? Rebex.IO.TraversalMode.Recursive : Rebex.IO.TraversalMode.NonRecursive);
                if (_client.DirectoryExists(this.Uri.AbsolutePath))
                {
                    _client.Delete(this.FullName, mode);
                }
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
