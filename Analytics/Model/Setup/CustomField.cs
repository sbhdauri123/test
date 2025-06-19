using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class CustomField : BasePOCO
    {
        [Key]

        public string TableSource { get; set; }
        public string RetrievedColumnName { get; set; }
        public string FunctionalColumnName { get; set; }
        public string ParentID { get; set; }
        public string ParentName { get; set; }
        public string ChildID { get; set; }
        public string ChildName { get; set; }
        public bool IsActive { get; set; }
        public string Entity { get; set; }
        new public DateTime CreatedDate { get; set; }
        new public DateTime LastUpdated { get; set; }

        //extract the SourceName from TableSource (example: sa360 from dti_sa360.saved_column)
        public string SourceName => this.TableSource.Split('.')[0].Replace("dti_", "");
        public string CustomFieldID => this.SourceName + "__" + this.ParentID + "__" + this.ChildID + "__" + this.RetrievedColumnName;
    }
}
