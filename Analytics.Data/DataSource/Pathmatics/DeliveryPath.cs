using System;
using System.Linq;

namespace Greenhouse.Data.DataSource.Pathmatics
{
    public class DeliveryPath
    {
        public string Bucket { get; }
        public string Entity { get; }
        public string JobId { get; }
        public string RandomGuid { get; }
        public string FileName { get { return $"{JobId}/{RandomGuid}"; } }
        public string AbsolutePath { get { return $"{Entity}/{JobId}/{RandomGuid}"; } }

        public DeliveryPath(string latestPath)
        {
            var charSeparators = new char[] { '/' };
            var pathComponents = latestPath.Split(charSeparators, StringSplitOptions.RemoveEmptyEntries).Reverse().ToArray();
            RandomGuid = pathComponents[0];
            JobId = pathComponents[1];
            Entity = pathComponents[2];
            Bucket = pathComponents[3];
        }
        public override string ToString()
        {
            return $"Latest filepath components-bucket:{Bucket}|entity:{Entity}|jobid:{JobId}" +
                $"|randomid:{RandomGuid}|filename used to check fileLog:{FileName}";
        }
    }
}
