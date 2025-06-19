using System;

namespace Greenhouse.Data.DataSource.Skai
{
    [Serializable]
    public class SkaiProfile
    {
        public string ProfileID { get; set; }
        public string ServerID { get; set; }
        public string ProfileName { get; set; }
        public string ProfileStatus { get; set; }
    }
}
