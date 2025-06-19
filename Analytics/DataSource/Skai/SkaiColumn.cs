using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Skai
{
    public class SkaiColumn
    {
        [JsonProperty("server_id")]
        public string ServerID { get; set; }
        [JsonProperty("profile_id")]
        public string ProfileID { get; set; }
        [JsonProperty("field_type")]
        public string FieldType { get; set; }
        [JsonProperty("type")]
        public string Type { get; set; }
        [JsonProperty("id")]
        public string ColumnID { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("group")]
        public string Group { get; set; }

        public SkaiColumn(ColumnAttribute columnAttribute, string columnType, SkaiProfile profile)
        {
            ServerID = profile.ServerID;
            ProfileID = profile.ProfileID;
            FieldType = columnType;
            Type = columnAttribute.ValueType;
            ColumnID = columnAttribute.Id;
            Name = columnAttribute.Name;
            Group = columnAttribute.Group;
        }
    }
}
