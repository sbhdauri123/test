using Greenhouse.Utilities;
using System;
using System.Collections.Generic;
using System.IO;

namespace Greenhouse.Services.RemoteAccess
{
    public class FileSystemDirectory : IDirectory
    {
        public Uri Uri { get; private set; }
        public RemoteUri UriHelper { get; private set; }
        private readonly DirectoryInfo _di;

        public FileSystemDirectory(Uri uri)
        {
            this.Uri = uri;
            this.UriHelper = new RemoteUri(uri);
            _di = new DirectoryInfo(this.Uri.LocalPath);
            Name = _di.Name;
            FullName = _di.FullName;
            Parent = _di.Parent == null ? null : _di.Parent.Name;
        }

        public bool Exists
        {
            get
            {
                return _di.Exists;
            }
        }

        public string Name { get; private set; }
        public string FullName { get; private set; }
        public string Parent { get; private set; }

        public IEnumerable<IDirectory> GetDirectories()
        {
            List<IDirectory> dirs = new List<IDirectory>();
            foreach (DirectoryInfo di in _di.GetDirectories())
            {
                dirs.Add(new FileSystemDirectory(new Uri(string.Format("{0}/{1}", this.Uri, di.Name))));
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
            foreach (FileInfo fi in _di.GetFiles())
            {
                files.Add(new FileSystemFile(new Uri(string.Format("{0}/{1}", this.Uri, fi.Name))));
            }
            return files;
        }

        public void Create()
        {
            _di.Create();
        }

        public IDirectory CreateSubDirectory(string subDir)
        {
            DirectoryInfo di = _di.CreateSubdirectory(subDir);
            return new FileSystemDirectory(new Uri(string.Format("{0}/{1}", this.Uri, di.Name)));
        }

        public void Delete(bool recursive)
        {
            _di.Delete(recursive);
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
