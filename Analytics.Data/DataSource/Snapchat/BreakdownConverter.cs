using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public static class BreakdownConverter
    {
        public static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            MetadataPropertyHandling = MetadataPropertyHandling.Ignore,
            DateParseHandling = DateParseHandling.None,
            Converters =
            {
                ConversionBreakDownStatsConverter.Singleton
            },
        };
    }

    internal sealed class ConversionBreakDownStatsConverter : JsonConverter
    {
        public override bool CanConvert(Type t) => t == typeof(Timeseries);

        public override object ReadJson(JsonReader reader, Type t, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null) return null;
            var value = serializer.Deserialize<dynamic>(reader);
            if (value != null)
            {
                var data = JsonConvert.DeserializeObject<Timeseries>(value.ToString());
                var conversion = JsonConvert.DeserializeObject<Conversion>(value.SelectToken("stats").ToString());

                if (conversion == null)
                    conversion = new Conversion();

                data.Conversions = conversion;
                return data;
            }
            throw new JsonSerializationException("Cannot unmarshal type Timeseries");
        }

        public override void WriteJson(JsonWriter writer, object untypedValue, JsonSerializer serializer)
        {
            throw new NotImplementedException("Not Implemented");
        }

        public static readonly ConversionBreakDownStatsConverter Singleton = new ConversionBreakDownStatsConverter();
    }
}