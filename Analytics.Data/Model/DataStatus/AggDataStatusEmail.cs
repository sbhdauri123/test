using System;

namespace Greenhouse.Data.Model.DataStatus
{
    public class AggDataStatusEmail
    {
        public int SourceID { get; set; }
        public string SourceName { get; set; }
        public DateTime? MaxDataDate { get; set; }
        public int NumMeasuredFileGuid { get; set; }
        public int NumComplete { get; set; }
        public int NumIncomplete { get; set; }
        public string IncompleteExample { get; set; }
    }
}
