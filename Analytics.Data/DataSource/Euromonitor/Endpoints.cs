using System.Text;

namespace Greenhouse.Data.DataSource.Euromonitor
{
    public static class Endpoints
    {
        public static readonly string JobHistory = "/statistics/jobHistory?NumberOfDays={0}";
        public static readonly string JobDownload = "/statistics/getjobdownloadurl/{0}";
        public static readonly string Authentication = "/authentication/connect/token";
    }
}
