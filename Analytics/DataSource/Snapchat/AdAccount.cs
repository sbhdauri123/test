using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public partial class AdAccountsRoot
    {
        [JsonProperty("request_status")]
        public string RequestStatus { get; set; }

        [JsonProperty("request_id")]
        public string RequestId { get; set; }

        [JsonProperty("paging")]
        public Paging Paging { get; set; }

        [JsonProperty("adaccounts")]
        public AdAccounts[] Adaccounts { get; set; }
    }

    public partial class AdAccounts
    {
        [JsonProperty("sub_request_status")]
        public string SubRequestStatus { get; set; }

        [JsonProperty("adaccount")]
        public AdAccount Adaccount { get; set; }
    }

    public partial class AdAccount
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("updated_at")]
        public string UpdatedAt { get; set; }

        [JsonProperty("created_at")]
        public string CreatedAt { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("status")]
        public string Status { get; set; }

        [JsonProperty("organization_id")]
        public string OrganizationId { get; set; }

        [JsonProperty("funding_source_ids")]
        public string[] FundingSourceIds { get; set; }

        [JsonProperty("currency")]
        public string Currency { get; set; }

        [JsonProperty("timezone")]
        public string Timezone { get; set; }

        [JsonProperty("advertiser_organization_id")]
        public string AdvertiserOrganizationId { get; set; }

        [JsonProperty("billing_center_id")]
        public string BillingCenterId { get; set; }

        [JsonProperty("billing_type")]
        public string BillingType { get; set; }

        [JsonProperty("lifetime_spend_cap_micro", NullValueHandling = NullValueHandling.Ignore)]
        public string LifetimeSpendCapMicro { get; set; }

        [JsonProperty("agency_representing_client")]
        public string AgencyRepresentingClient { get; set; }

        [JsonProperty("client_paying_invoices")]
        public string ClientPayingInvoices { get; set; }

        [JsonProperty("po_number", NullValueHandling = NullValueHandling.Ignore)]
        public string PoNumber { get; set; }

        [JsonProperty("cell_ids", NullValueHandling = NullValueHandling.Ignore)]
        public string[] CellIds { get; set; }

        [JsonProperty("agency_client_metadata", NullValueHandling = NullValueHandling.Ignore)]
        public AgencyClientMetadata AgencyClientMetadata { get; set; }

        [JsonProperty("regulations", NullValueHandling = NullValueHandling.Ignore)]
        public Regulations Regulations { get; set; }
    }

    public partial class Regulations
    {
        [JsonProperty("restricted_delivery_signals")]
        public string RestrictedDeliverySignals { get; set; }
    }

    public class AgencyClientMetadata
    {
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("email")]
        public string Email { get; set; }
        [JsonProperty("address_line_1")]
        public string AddressLine { get; set; }
        [JsonProperty("city")]
        public string City { get; set; }
        [JsonProperty("administrative_district_level_1")]
        public string AdministrativeDistrictLevel { get; set; }
        [JsonProperty("country")]
        public string Country { get; set; }
        [JsonProperty("zipcode")]
        public string Zipcode { get; set; }
        [JsonProperty("tax_id")]
        public string TaxId { get; set; }
    }
}
