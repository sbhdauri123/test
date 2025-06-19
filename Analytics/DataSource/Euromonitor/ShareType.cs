using System.Text.Json.Serialization;

namespace Greenhouse.Data.DataSource.Euromonitor
{
    public class ShareType
    {
        public ShareType(int id, string name)
        {
            Id = id;
            Name = name;

            int start = name.IndexOf('(') + 1;
            int end = name.IndexOf(')', start);
            Code = name.Substring(start, end - start);
        }

        [JsonPropertyName("id")]
        public int Id { get; private set; }
       
        [JsonPropertyName("name")]
        public string Name { get; private set; }
        
        public string Code { get; set; }

        public string Type { get; set; }
    }
}