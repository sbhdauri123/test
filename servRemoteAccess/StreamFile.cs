using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using System;
using System.IO;

namespace Greenhouse.Services.RemoteAccess
{
    public class StreamFile : IFile
    {
        public Uri Uri { get; private set; }
        public RemoteUri UriHelper { get; private set; }

        public IDirectory Directory => throw new NotImplementedException();

        public bool Exists => throw new NotImplementedException();

        public DateTime LastWriteTimeUtc => throw new NotImplementedException();

        public string Name => throw new NotImplementedException();

        public string FullName => throw new NotImplementedException();

        public string Extension => throw new NotImplementedException();

        public string ContentType => throw new NotImplementedException();

        internal long _length = -1;
        public long Length
        {
            get
            {
                if (_length == -1)
                {
                    try
                    {
                        _length = _fileInfo.Length;
                    }
                    catch (Exception)  //for times when we can't get length.
                    {
                        _length = S3File.MAX_PUT_SIZE + 1;
                    }
                }
                return _length;
            }
        }

        private readonly System.IO.Stream _fileInfo;
        private readonly bool _autoCloseStream;

        public StreamFile(System.IO.Stream strm, Credential creds, bool autoCloseStream = true)
        {
            _fileInfo = strm;
            _autoCloseStream = autoCloseStream;
        }

        /// <summary>
        /// not implemented
        /// </summary>
        public Stream Create()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// not implemented
        /// </summary>
        public void Delete()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// not implemented
        /// </summary>
        public Stream Get()
        {
            return this._fileInfo;
        }

        /// <summary>
        /// not implemented
        /// </summary>
        public void Put(Stream inputStream)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// If destFile is a FileSystem file, then will copy stream to local file.
        /// If destFile is an S3 file, then will copy directly to S3.
        /// Will always overwrite existing file.
        /// </summary>
        /// <param name="destFile"></param>
        /// <param name="overwrite"></param>
        public void CopyTo(IFile destFile, bool overwrite = false)
        {
            System.IO.Stream fileStream = null;
            if (destFile is FileSystemFile)
            {
                if (destFile.Exists)
                    destFile.Delete();

                if (!destFile.Directory.Exists)
                    destFile.Directory.Create();

                fileStream = destFile.Create();
            }
            else if (destFile is S3File s3File)
            {
                if (s3File.Exists)
                    s3File.Delete();
                // aws s3 does not return a seekable stream to write to
                // so we upload the source stream as a put request
                s3File.Put(this._fileInfo, _autoCloseStream);
                return;
            }

            using (fileStream)
            {
                byte[] buffer = new byte[4096];
                int read;
                while ((read = _fileInfo.Read(buffer, 0, buffer.Length)) > 0)
                {
                    fileStream.Write(buffer, 0, read);
                }
                fileStream.Dispose();
            }
        }

        /// <summary>
        /// not implemented
        /// </summary>
        /// <param name="destFile"></param>
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
    }
}
