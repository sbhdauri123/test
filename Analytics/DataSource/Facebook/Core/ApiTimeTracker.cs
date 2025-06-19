using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.DataSource.Facebook.Core
{
    public class ApiTimeTracker
    {
        public ApiTimeTracker()
        {
            SessionLog = new Dictionary<SessionTypeEnum, ApiTimeLog>();
        }

        public Dictionary<SessionTypeEnum, ApiTimeLog> SessionLog { get; private set; }

        public void StartSession(SessionTypeEnum sessionType)
        {
            if (!SessionLog.Any(x => x.Key == sessionType))
                SessionLog.Add(sessionType, new ApiTimeLog());

            SessionLog[sessionType].GetTimer.Restart();
        }

        public void SaveSession(SessionTypeEnum sessionType, long downloadSize)
        {
            if (!SessionLog.Any(x => x.Key == sessionType))
                SessionLog.Add(sessionType, new ApiTimeLog());

            SessionLog[sessionType].GetTimer.Stop();
            SessionLog[sessionType].DownloadSize += downloadSize;
            SessionLog[sessionType].RecordedTime += SessionLog[sessionType].GetTimer.Elapsed;
            SessionLog[sessionType].Counter++;
        }
    }
}
