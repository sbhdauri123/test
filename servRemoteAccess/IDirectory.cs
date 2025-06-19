using Greenhouse.Utilities;
using System;
using System.Collections.Generic;

namespace Greenhouse.Services.RemoteAccess
{
    public interface IDirectory
    {
        Uri Uri { get; }
        RemoteUri UriHelper { get; }
        bool Exists { get; }
        string Name { get; }
        string FullName { get; }
        string Parent { get; }
        IEnumerable<IDirectory> GetDirectories();
        IEnumerable<IFile> GetFiles(bool isRecursive = false);
        void Create();
        IDirectory CreateSubDirectory(string subDir);
        void Delete(bool recursive);
    }
}
