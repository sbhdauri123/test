using Greenhouse.Utilities;
using System;
using System.IO;

namespace Greenhouse.Services.RemoteAccess
{
    public interface IFile
    {
        Uri Uri { get; }
        RemoteUri UriHelper { get; }
        IDirectory Directory { get; }
        bool Exists { get; }
        DateTime LastWriteTimeUtc { get; }
        string Name { get; }
        string FullName { get; }
        string Extension { get; }
        string ContentType { get; }
        long Length { get; }
        Stream Create();
        void Delete();
        Stream Get();
        void Put(Stream inputStream);
        void CopyTo(IFile destFile, bool overwrite = false);
        void MoveTo(IFile destFile);
    }
}
