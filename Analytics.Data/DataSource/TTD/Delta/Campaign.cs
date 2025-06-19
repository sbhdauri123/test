using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.TTD.Delta
{
    public class CampaignFlight
    {
        public string CampaignFlightId { get; set; }
        public string CampaignId { get; set; }
        public string StartDateInclusiveUTC { get; set; }
        public string EndDateExclusiveUTC { get; set; }
        public string BudgetInAdvertiserCurrency { get; set; }
        public string BudgetInImpressions { get; set; }
        public string DailyTargetInAdvertiserCurrency { get; set; }
        public string DailyTargetInImpressions { get; set; }
    }

    public class CampaignConversionReportingColumn
    {
        public string TrackingTagId { get; set; }
        public string TrackingTagName { get; set; }
        public string ReportingColumnId { get; set; }
        public string CrossDeviceAttributionModelId { get; set; }
    }

    public class Campaign
    {
        public Money Budget { get; set; }
        public string BudgetInImpressions { get; set; }
        public Money DailyBudget { get; set; }
        public string DailyBudgetInImpressions { get; set; }
        public string StartDate { get; set; }
        public string EndDate { get; set; }
        public bool AutoAllocatorEnabled { get; set; }
        public string PacingMode { get; set; }
        public List<CampaignFlight> CampaignFlights { get; set; }
        public string AdvertiserId { get; set; }
        public string CampaignId { get; set; }
        public string CampaignName { get; set; }
        public string Description { get; set; }
        public string PartnerCostPercentageFee { get; set; }
        public Money PartnerCPMFee { get; set; }
        public Money PartnerCPCFee { get; set; }
        public List<CampaignConversionReportingColumn> CampaignConversionReportingColumns { get; set; }
        public string Availability { get; set; }
        public bool CtvTargetingAndAttribution { get; set; }
        public string CreatedAtUTC { get; set; }
        public string LastUpdatedAtUTC { get; set; }
        public string PurchaseOrderNumber { get; set; }
        public string TimeZone { get; set; }
    }

    public class RootCampaign
    {
        public List<Campaign> Campaigns { get; set; }
    }
}
