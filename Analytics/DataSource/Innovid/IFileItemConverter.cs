using Greenhouse.Data.Model.Core;
using Newtonsoft.Json;
using System;
namespace Greenhouse.Data.DataSource.Innovid
{
    public class IFileItemConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(IFileItem);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return serializer.Deserialize(reader, typeof(Queue));
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            serializer.Serialize(writer, value);
        }
    }
}
