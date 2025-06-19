using System.Globalization;

namespace Greenhouse.UI.Models
{
    public class QuartzTriggerLiteWrapper
    {
        private string _description;

        public string JobName { get; set; }

        private string _advertiserName;
        public string AdvertiserName
        {
            get
            {
                return _advertiserName;
            }
            set
            {
                _advertiserName = value.Equals("All", StringComparison.InvariantCultureIgnoreCase) ? "N/A" : value;
            }
        }
        public string JobType { get; set; }
        public string JobSubType { get; set; }
        public string JobStep { get; set; }
        public string Interval { get; set; }
        public string ModuleName { get; set; }
        public string TriggerName { get; set; }
        public string TriggerGroup { get; set; }
        public string JobGroup { get; set; }
        public string SourceName { get; set; }

        private string schedulerName;
        public string SchedulerName
        {
            get
            {
                return schedulerName;
            }
            set
            {
                var quartzSchedulerName = value.Split("-".ToCharArray()).Last();
                schedulerName = new CultureInfo("en-US").TextInfo.ToTitleCase(quartzSchedulerName.ToLower());
                //This could be removed once the Framework is fixed with job steps.
                if (schedulerName.Equals("Dataload", StringComparison.InvariantCultureIgnoreCase)) schedulerName = "Processing";
            }
        }

        public string Description
        {
            get { return _description; }
            set
            {
                // parse value and set appropriate propeties
                _description = value;
            }
        }
        public string TriggerState { get; set; }

        public DateTime NextFireTime { get; set; }
        private string _nextFireTimeStr;
        public string NextFireTimeStr
        {
            get
            {
                string dateFormat = String.Format("{0} {1}", this.ShortDatePattern, this.ShortTimePattern);
                _nextFireTimeStr = (this.NextFireTime == DateTime.MinValue) ? String.Empty : this.NextFireTime.ToString(dateFormat);
                return _nextFireTimeStr;
            }
            set
            {
                _nextFireTimeStr = value;
            }
        }

        private string _nextFireTimeString;
        public string NextFireTimeString
        {
            get
            {
                _nextFireTimeString = (this.NextFireTime == DateTime.MinValue) ? String.Empty : this.NextFireTime.ToString("yyyy-MM-dd HH:mm:ss.sss");
                return _nextFireTimeString;
            }
            set
            {
                _nextFireTimeString = value;
            }
        }

        public DateTime PrevFireTime { get; set; }
        private string _prevFireTimeStr;
        public string PrevFireTimeStr
        {
            get
            {
                string dateFormat = String.Format("{0} {1}", this.ShortDatePattern, this.ShortTimePattern);
                _prevFireTimeStr = (this.PrevFireTime == DateTime.MinValue) ? String.Empty : this.PrevFireTime.ToString(dateFormat);
                return _prevFireTimeStr;
            }
            set { _prevFireTimeStr = value; }
        }

        public bool Selected { get; set; }
        private string shortDatePattern;
        public string ShortDatePattern
        {
            get
            {
                if (String.IsNullOrEmpty(shortDatePattern))
                {
                    shortDatePattern = "yyyy-MM-dd";
                }
                return shortDatePattern;
            }
        }

        private string shortTimePattern;
        public string ShortTimePattern
        {
            get
            {
                if (String.IsNullOrEmpty(shortTimePattern))
                {
                    var ci = System.Globalization.CultureInfo.GetCultureInfo("en-US");
                    shortTimePattern = ci.DateTimeFormat.ShortTimePattern;
                }
                return shortTimePattern;
            }
        }
    }
}