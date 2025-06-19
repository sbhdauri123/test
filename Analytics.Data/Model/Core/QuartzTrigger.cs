using Dapper;
using System;

namespace Greenhouse.Data.Model.Core
{
    [Table("QRTZ_TRIGGERS")]
    public class QuartzTrigger
    {
        [System.ComponentModel.DataAnnotations.Key]
        public string TRIGGER_NAME { get; set; }
        public string TRIGGER_GROUP { get; set; }

        public string JOB_NAME { get; set; }

        public string JOB_GROUP { get; set; }
        public string DESCRIPTION { get; set; }

        public Int64? NEXT_FIRE_TIME { get; set; }

        public Int64? PREV_FIRE_TIME { get; set; }

        public int PRIORITY { get; set; }

        public string TRIGGER_STATE { get; set; }

        public string TRIGGER_TYPE { get; set; }

        public Int64 START_TIME { get; set; }

        public Int64? END_TIME { get; set; }

        public string CALENDAR_NAME { get; set; }

        public int MISFIRE_INSTR { get; set; }

        public object JOB_DATA { get; set; }

        public string SCHED_NAME { get; set; }

        public string CRON_EXPRESSION { get; set; }
        public Int32 REPEAT_INTERVAL { get; set; }

        public string JOB_CLASS_NAME { get; set; }
    }
}
