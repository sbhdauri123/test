using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.Model.Core
{
    [Serializable]
    [Dapper.Table("Queue")]
    public class Queue : BasePOCO, IFileItem
    {
        [Key]
        public Int64 ID { get; set; }
        public Int64 JobLogID { get; set; }
        public Guid FileGUID { get; set; }
        public string Step { get; set; }
        public int IntegrationID { get; set; }
        public int SourceID { get; set; }
        public string SourceFileName { get; set; }
        public string FileName { get; set; }
        public string EntityID { get; set; }
        public DateTime FileDate { get; set; }
        public int? FileDateHour { get; set; }
        public int StatusId { get; set; }
        public string Status { get; set; }

        private long _fileSize;
        public long FileSize
        {
            get
            {
                if (FileCollection != null && FileCollection.Any())
                {
                    _fileSize = FileCollection.Sum(s => s.FileSize);
                }
                return _fileSize;
            }
            set
            {
                _fileSize = value;
            }
        }
        public string FileCollectionJSON { get; set; }
        [Computed]
        public IEnumerable<FileCollectionItem> FileCollection
        {
            get
            {
                if (string.IsNullOrEmpty(FileCollectionJSON))
                {
                    return null;
                }
                return Newtonsoft.Json.JsonConvert.DeserializeObject<IEnumerable<FileCollectionItem>>(FileCollectionJSON);
            }
        }

        public bool IsBackfill { get; set; }
        public bool IsDimOnly { get; set; }
        public int? CredentialID { get; set; }

        public DateTime? DeliveryFileDate { get; set; }
    }
}
