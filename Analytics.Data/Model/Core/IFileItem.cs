using System;
using System.Collections.Generic;

namespace Greenhouse.Data.Model.Core
{
    public interface IFileItem
    {
        Int64 ID { get; set; }
        Int64 JobLogID { get; set; }
        Guid FileGUID { get; set; }
        string Step { get; set; }
        int IntegrationID { get; set; }
        int SourceID { get; set; }
        string SourceFileName { get; set; }
        string FileName { get; set; }
        string EntityID { get; set; }
        DateTime FileDate { get; set; }
        int? FileDateHour { get; set; }
        int StatusId { get; set; }
        string Status { get; set; }
        long FileSize { get; set; }
        string FileCollectionJSON { get; set; }
        bool IsBackfill { get; set; }
        DateTime CreatedDate { get; set; }
        DateTime LastUpdated { get; set; }
        bool IsDimOnly { get; set; }
        int? CredentialID { get; set; }

        IEnumerable<FileCollectionItem> FileCollection { get; }

        DateTime? DeliveryFileDate { get; set; }
    }
}
