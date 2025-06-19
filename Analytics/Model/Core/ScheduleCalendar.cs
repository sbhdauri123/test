using System;
using System.Text;

namespace Greenhouse.Data.Model.Core
{
    [Serializable]
    public class ScheduleCalendar
    {
        public ScheduleCalendar(string timeZoneString, IntervalType it, string intervalExp = null, DateTime? startTime = null)
        {
            if (it == IntervalType.Weekly && string.IsNullOrEmpty(intervalExp))
            {
                throw new ArgumentNullException(nameof(intervalExp), "When specifying an IntervalType other than 'Daily' or 'Backfill' you must specify a value for the CronExpression property.");
            }
            this.TimeZoneString = timeZoneString;
            this.Interval = it;
            this.IntervalExpression = intervalExp;
            this._startTime = startTime;
        }

        public string TimeZoneString { get; set; }

        public enum IntervalType
        {
            Minutely,
            Hourly,
            Daily,
            Weekly,
            Backfill,
            Monthly
        }

        public IntervalType Interval { get; set; }
        public string IntervalExpression { get; set; }
        private DateTime? _startTime;
        public DateTime StartTime
        {
            get
            {
                return (_startTime.HasValue ? _startTime.Value : DateTime.Now);
            }
            set
            {
                _startTime = value;
            }
        }
        private string _cronExpression;
        public string CronExpression
        {
            get
            {
                if (!string.IsNullOrEmpty(_cronExpression))
                    return _cronExpression;
                else
                {
                    SimpleCronExpression sce = null;
                    if (Interval != IntervalType.Monthly)
                    {
                        string interval = Interval == IntervalType.Daily ? "MON-SUN" : IntervalExpression;
                        sce = new SimpleCronExpression(
                           0, StartTime.Minute, StartTime.Hour, null, -1, interval);
                    }
                    else
                    {
                        sce = new SimpleCronExpression(
                            0, StartTime.Minute, StartTime.Hour, IntervalExpression, -1, null);
                    }
                    return sce.ToString();
                }
            }
            set
            {
                _cronExpression = value;
            }
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            try
            {
                sb.Append("[StartTime=").Append(StartTime.ToShortDateString()).Append(' ').Append(StartTime.ToShortTimeString()).Append("] ");
                sb.Append("[TimeZoneString=").Append(TimeZoneString).Append("] ");
                sb.Append("[IntervalType=").Append(Interval.ToString()).Append("] ");
                sb.Append("[IntervalExpression=").Append(IntervalExpression).Append("] ");
                sb.Append("[CronExpression=").Append(CronExpression).Append("] ");
            }
            catch (Exception exc)
            {
                sb.Append(exc.StackTrace);
            }
            return sb.ToString();
        }
    }
}
