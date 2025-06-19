using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public class AdAccountCreatives
    {
        public string AdAccountID { get; set; }
        public string Timezone { get; set; }
        public List<Creative> Creatives { get; set; }
    }

    public class CreativeTZ
    {
        public string AdAccountID { get; set; }
        public string Timezone { get; set; }
        public string CreativeID { get; set; }
    }
}
