using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class MetadataStageConfiguration : BasePOCO
    {
        public int SourceID { get; set; }
        public string TableName { get; set; }
        public string FieldName { get; set; }
        public int FieldOrder { get; set; }
        public int SourceFileID { get; set; }
        public int IsActive { get; set; }
    }
}
