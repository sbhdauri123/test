using Dapper.Contrib.Extensions;
using Greenhouse.Utilities;
using System;
//using Dapper;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class SourceFile : BasePOCO
    {
        [Dapper.Key]
        public int SourceFileID { get; set; }
        public string SourceFileName { get; set; }
        public string RegexMask { get; set; }
        public int SourceID { get; set; }
        public bool IsActive { get; set; }
        public int DeliveryOffsetOverride { get; set; }

        public string PartitionColumn { get; set; }
        public string FileDelimiter { get; set; }

        public int? PartitionCount { get; set; } = 12;
        public bool HasHeader { get; set; }
        public int FileFormatID { get; set; }
        public bool IsDoneFile { get; set; }
        public string QuotedIdentifier { get; set; }

        private RegexCodec _fileRegexCodec;
        [Computed]
        public RegexCodec FileRegexCodec
        {
            get
            {
                if (_fileRegexCodec == null)
                {
                    _fileRegexCodec = new RegexCodec(this.RegexMask);
                }
                return _fileRegexCodec;
            }
        }

        public string VendorSLA { get; set; }
        public string Cadence { get; set; }
        public int? RowsToSkip { get; set; } = 0;
        public bool HasDeliveryData { get; set; }
    }
}
