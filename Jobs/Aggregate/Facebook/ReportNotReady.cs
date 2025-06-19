using System;

namespace Greenhouse.Jobs.Aggregate.Facebook
{
    public class ReportNotReadyException : Exception
    {
        public ReportNotReadyException() : base("The report is not yet ready")
        {
        }
        public ReportNotReadyException(string message) : base(message)
        {
        }
    }
}
