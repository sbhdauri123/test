using System;

namespace Greenhouse.Data.Model.Core
{
    [Serializable]
    public class FileCollectionItem
    {
        public string SourceFileName { get; set; }
        public string FilePath { get; set; }
        public long FileSize { get; set; }
    }
}
