using System;

namespace Greenhouse.Data.DataSource.DCM
{
    [Serializable]
    public enum ReportTypes
    {
        STANDARD,
        FLOODLIGHT,
        PATH_TO_CONVERSION,
        REACH
    }

    public enum ReportStatus
    {
        NEW_REPORT,
        CANCELLED,
        DONE,
        FAILED,
        PENDING,
        PROCESSING,
        REPORT_AVAILABLE,
        RUNNING,
        STATE_UNSPECIFIED,
        QUEUED
    }
}