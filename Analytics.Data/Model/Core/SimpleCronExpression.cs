using Greenhouse.Common;
using System;
using System.Text;

namespace Greenhouse.Data.Model.Core
{
    [Serializable]
    public class SimpleCronExpression
    {
        public SimpleCronExpression()
        {
        }

        public static SimpleCronExpression Parse(string cronString)
        {
            SimpleCronExpression sce = new SimpleCronExpression();
            string[] parts = cronString.Split(Constants.SPACE_ARRAY);
            sce.Seconds = Convert.ToInt32(parts[0]);
            sce.Minutes = Convert.ToInt32(parts[1]);
            sce.Hours = Convert.ToInt32(parts[2]);
            sce.DayOfMonth = parts[3];
            int mon = 0;
            if (int.TryParse(parts[4], out mon))
            {
                sce.Month = mon;
            }
            else
            {
                sce.Month = -1;
            }
            sce.DayOfWeek = parts[5];
            if (parts.Length > 6)
            {
                sce.Year = Convert.ToInt32(parts[6]);
            }
            return sce;
        }

        public SimpleCronExpression(int seconds, int minutes, int hours, string dayOfMonth, int month, string dayOfWeek)
            : this(seconds, minutes, hours, dayOfMonth, month, dayOfWeek, 0)
        {
        }

        public SimpleCronExpression(int seconds, int minutes, int hours, string dayOfMonth, int month, string dayOfWeek, int year)
        {
            this.Seconds = seconds;
            this.Minutes = minutes;
            this.Hours = hours;
            this.DayOfMonth = dayOfMonth;
            this.Month = month;
            this.DayOfWeek = dayOfWeek;
            this.Year = year;
        }

        private int _seconds;

        /// <summary>
        /// Property Seconds (int).
        /// <summary>
        public int Seconds
        {
            get
            {
                return this._seconds;
            }
            set
            {
                if (value < 0 || value > 59)
                {
                    this._seconds = 0;
                }
                else
                {
                    this._seconds = value;
                }
            }
        }

        private int _minutes;

        /// <summary>
        /// Property Minutes (int).
        /// <summary>
        public int Minutes
        {
            get
            {
                return this._minutes;
            }
            set
            {
                if (value < 0 || value > 59)
                {
                    this._minutes = 0;
                }
                else
                {
                    this._minutes = value;
                }
            }
        }

        private int _hours;

        /// <summary>
        /// Property Hours (int).
        /// <summary>
        public int Hours
        {
            get
            {
                return this._hours;
            }
            set
            {
                if (value < 0 || value > 23)
                {
                    this._hours = 0;
                }
                else
                {
                    this._hours = value;
                }
            }
        }

        private string _dayOfMonth;

        /// <summary>
        /// Property DayOfMonth (string).
        /// <summary>
        public string DayOfMonth
        {
            get
            {
                return this._dayOfMonth;
            }
            set
            {
                if (value == null || value == string.Empty)
                {
                    this._dayOfMonth = "?";
                }
                else
                {
                    this._dayOfMonth = value;
                }
            }
        }

        private int _month;

        /// <summary>
        /// Property Month (int).
        /// <summary>
        public int Month
        {
            get
            {
                return this._month;
            }
            set
            {
                this._month = value;
            }
        }

        private string _dayOfWeek;

        /// <summary>
        /// Property DayOfWeek (string).
        /// <summary>
        public string DayOfWeek
        {
            get
            {
                return this._dayOfWeek;
            }
            set
            {
                if (value == null || value == string.Empty)
                {
                    this._dayOfWeek = "?";
                }
                else
                {
                    this._dayOfWeek = value;
                }
            }
        }

        private int _year;

        /// <summary>
        /// Property Year (int).
        /// <summary>
        public int Year
        {
            get
            {
                return this._year;
            }
            set
            {
                this._year = value;
            }
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(this.Seconds).Append(' ').Append(this.Minutes).Append(' ').Append(this.Hours).Append(' ').Append(this.DayOfMonth).Append(' ');
            sb.Append((this.Month < 0 || this.Month > 11) ? "*" : this.Month.ToString()).Append(' ').Append(this.DayOfWeek).Append(' ');
            sb.Append((this.Year <= 0) ? "" : this.Year.ToString());

            return sb.ToString().Trim();
        }
    }
}
