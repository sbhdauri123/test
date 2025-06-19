using Greenhouse.Common;
using System;

namespace Greenhouse.Jobs.Infrastructure
{
    [System.ComponentModel.Composition.MetadataAttribute]
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class DragoJobAttribute : System.ComponentModel.Composition.ExportAttribute
    {
        public DragoJobAttribute() : base(typeof(IDragoJob)) { }
        public Constants.JobStep Step { get; set; }
        public Constants.ExecutionType ExecutionType { get; set; }
        public string DataSource { get; set; }
        public string StepDescription { get; set; }
    }
}
