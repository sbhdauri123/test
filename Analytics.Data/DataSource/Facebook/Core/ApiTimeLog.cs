using System;
using System.Diagnostics;

namespace Greenhouse.Data.DataSource.Facebook.Core
{
    public class ApiTimeLog
    {
        public long DownloadSize { get; set; }
        public TimeSpan RecordedTime { get; set; }
        public int Counter { get; set; }
        private Stopwatch _timer;

        public Stopwatch GetTimer
        {
            get
            {
                if (_timer == null)
                    _timer = new Stopwatch();

                return _timer;
            }
        }
    }
}
