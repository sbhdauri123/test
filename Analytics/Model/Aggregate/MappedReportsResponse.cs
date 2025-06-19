using Dapper.Contrib.Extensions;
using System.Collections.Generic;

namespace Greenhouse.Data.Model.Aggregate
{
    public class MappedReportsResponse<T> : BasePOCO
    {
        private T reportSettings;

        public int APIReportID { get; set; }
        public string APIReportName { get; set; }
        public int SourceID { get; set; }
        public int CredentialID { get; set; }
        public bool IsActive { get; set; }
        public string ReportSettingsJSON { get; set; }
        public string EntityID { get; set; }
        public bool IsDefault { get; set; }

        [Computed]
        public T ReportSettings
        {
            get
            {
                if (string.IsNullOrEmpty(ReportSettingsJSON))
                {
                    return default(T);
                }

                if (EqualityComparer<T>.Default.Equals(reportSettings, default(T))) reportSettings = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(ReportSettingsJSON);

                return reportSettings;
            }
        }

        [Computed]
        public IEnumerable<APIReportField> ReportFields { get; set; }
    }
}
