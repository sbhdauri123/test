using Greenhouse.Utilities;
using System;
using System.IO;

namespace Greenhouse.Services.RemoteAccess
{
    public class FileSystemFile : IFile
    {
        public Uri Uri { get; private set; }
        public RemoteUri UriHelper { get; private set; }
        private readonly FileInfo _fileInfo;

        public FileSystemFile(Uri uri)
        {
            this.Uri = uri;
            this.UriHelper = new RemoteUri(uri);
            _fileInfo = new FileInfo(this.Uri.LocalPath);
        }

        public IDirectory Directory
        {
            get
            {
                return new FileSystemDirectory(new Uri(this.Uri.OriginalString.Replace(Name, string.Empty)));
            }
        }

        public bool Exists
        {
            get
            {
                return _fileInfo.Exists;
            }
        }

        public DateTime LastWriteTimeUtc
        {
            get
            {
                return _fileInfo.LastWriteTimeUtc;
            }
        }

        public string Name
        {
            get
            {
                return _fileInfo.Name;
            }
        }
        public string FullName
        {
            get
            {
                return _fileInfo.FullName;
            }
        }
        public string Extension
        {
            get
            {
                return _fileInfo.Extension;
            }
        }
        public long Length
        {
            get
            {
                return _fileInfo.Length;
            }
        }

        public string ContentType
        {
            get
            {
                return UtilsIO.GetMimeTypeForFileExtension(this.Extension);
            }
        }

        public Stream Create()
        {
            return File.Create(_fileInfo.FullName, 40960, FileOptions.WriteThrough);
        }

        public void Delete()
        {
            _fileInfo.Delete();
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
            return _fileInfo.OpenRead();
        }

        public void Put(Stream inputStream)
        {
            using (Stream outputStream = _fileInfo.OpenWrite())
            {
                inputStream.CopyTo(outputStream);
            }
        }

        public void CopyTo(IFile destFile, bool overwrite = false)
        {
            if (destFile is FileSystemFile)
            {
                _fileInfo.CopyTo(destFile.FullName, overwrite);
            }
            else
            {
                using (Stream stream = this.Get())
                {
                    destFile.Put(stream);
                }
            }
        }

        public void MoveTo(IFile destFile)
        {
            if (destFile is FileSystemFile)
            {
                _fileInfo.MoveTo(destFile.FullName);
            }
            else
            {
                using (Stream stream = this.Get())
                {
                    destFile.Put(stream);
                }

                this.Delete();
            }
        }
    }
}
