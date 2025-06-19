using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Brief
{
    public class SearchArtifactResponse
    {
        [JsonProperty("total")]
        public int Total { get; set; }
        [JsonProperty("artifacts")]
        public List<Artifact> Artifacts { get; set; }
        [JsonProperty("aggregations")]
        public object Aggregations { get; set; }
    }
    public class Artifact
    {
        [JsonProperty("artifactID")]
        public int ArtifactID { get; set; }
        [JsonProperty("artifactName")]
        public string ArtifactName { get; set; }
        [JsonProperty("appRegistryID")]
        public int AppRegistryID { get; set; }
        [JsonProperty("artifactRegistryID")]
        public int ArtifactRegistryID { get; set; }
        [JsonProperty("isValidated")]
        public bool IsValidated { get; set; }
        [JsonProperty("referenceMethodID")]
        public int ReferenceMethodID { get; set; }
        [JsonProperty("masterAgencyDivisionID")]
        public int MasterAgencyDivisionID { get; set; }
        [JsonProperty("masterBusinessUnitID")]
        public int MasterBusinessUnitID { get; set; }
        [JsonProperty("masterRegionID")]
        public int MasterRegionID { get; set; }
        [JsonProperty("masterCountryID")]
        public int MasterCountryID { get; set; }
        [JsonProperty("ancestors")]
        public List<object> Ancestors { get; set; }
        [JsonProperty("isDeleted")]
        public bool IsDeleted { get; set; }
        [JsonProperty("createdBy")]
        public string CreatedBy { get; set; }
        [JsonProperty("createdDate", NullValueHandling = NullValueHandling.Include), JsonConverter(typeof(MinDateTimeConverter))]
        public DateTime CreatedDate { get; set; }
        [JsonProperty("updatedBy")]
        public string UpdatedBy { get; set; }
        [JsonProperty("updatedDate", NullValueHandling = NullValueHandling.Include), JsonConverter(typeof(MinDateTimeConverter))]
        public DateTime UpdatedDate { get; set; }
        [JsonProperty("requestType")]
        public string RequestType { get; set; }
        [JsonProperty("requestName")]
        public string RequestName { get; set; }
        [JsonProperty("budget")]
        public double Budget { get; set; }
        [JsonProperty("flightStartDate", NullValueHandling = NullValueHandling.Include), JsonConverter(typeof(MinDateTimeConverter))]
        public DateTime FlightStartDate { get; set; }
        [JsonProperty("flightEndDate", NullValueHandling = NullValueHandling.Include), JsonConverter(typeof(MinDateTimeConverter))]
        public DateTime FlightEndDate { get; set; }
        [JsonProperty("brands")]
        public List<Brand> Brands { get; set; }
        [JsonProperty("products")]
        public List<Product> Products { get; set; }
        [JsonProperty("objectives")]
        public List<Objective> Objectives { get; set; }
        [JsonProperty("kpis")]
        public List<Kpi> Kpis { get; set; }
        [JsonProperty("targetAudiences")]
        public List<TargetAudience> TargetAudiences { get; set; }
        [JsonProperty("fundingSources")]
        public List<FundingSource> FundingSources { get; set; }
        [JsonProperty("geographies")]
        public List<Geography> Geographies { get; set; }
        [JsonProperty("regions")]
        public List<Region> Regions { get; set; }
        [JsonProperty("dmas")]
        public List<Dma> Dmas { get; set; }
        [JsonProperty("mediaGroups")]
        public List<MediaGroup> MediaGroups { get; set; }
        [JsonProperty("mediaTypes")]
        public List<MediaType> MediaTypes { get; set; }
        [JsonProperty("contacts")]
        public List<Contact> Contacts { get; set; }
        [JsonProperty("briefIDs")]
        public List<int> BriefIDs { get; set; }
        [JsonProperty("briefVersions")]
        public List<BriefVersion> BriefVersions { get; set; }
        [JsonProperty("taskIDs")]
        public List<object> TaskIDs { get; set; }
        [JsonProperty("dueDate", NullValueHandling = NullValueHandling.Include), JsonConverter(typeof(MinDateTimeConverter))]
        public DateTime DueDate { get; set; }
    }
    public class Brand
    {
        [JsonProperty("code")]
        public string Code { get; set; }
    }
    public class Product
    {
        [JsonProperty("code")]
        public string Code { get; set; }
    }
    public class Objective
    {
        [JsonProperty("code")]
        public string Code { get; set; }
    }
    public class Kpi
    {
        [JsonProperty("code")]
        public string Code { get; set; }
        [JsonProperty("goal")]
        public double Goal { get; set; }
    }
    public class TargetAudience
    {
        [JsonProperty("code")]
        public string Code { get; set; }
        [JsonProperty("isPrimary")]
        public bool IsPrimary { get; set; }
    }
    public class FundingSource
    {
        [JsonProperty("code")]
        public string Code { get; set; }
    }
    public class Geography
    {
        [JsonProperty("code")]
        public string Code { get; set; }
    }
    public class Region
    {
        [JsonProperty("code")]
        public string Code { get; set; }
    }
    public class Dma
    {
        [JsonProperty("code")]
        public string Code { get; set; }
    }
    public class MediaGroup
    {
        [JsonProperty("code")]
        public string Code { get; set; }
    }
    public class MediaType
    {
        [JsonProperty("code")]
        public string Code { get; set; }
    }
    public class Contact
    {
        [JsonProperty("type")]
        public string Type { get; set; }
        [JsonProperty("code")]
        public string Code { get; set; }
    }
    public class BriefVersion
    {
        [JsonProperty("briefId")]
        public int BriefId { get; set; }
        [JsonProperty("briefVersion")]
        public int Version { get; set; }
    }
    public class MinDateTimeConverter : DateTimeConverterBase
    {
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.Value == null)
                return DateTime.MinValue;

            return (DateTime)reader.Value;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            DateTime dateTimeValue = (DateTime)value;
            if (dateTimeValue == DateTime.MinValue)
            {
                writer.WriteNull();
                return;
            }

            writer.WriteValue(value);
        }
    }
}
