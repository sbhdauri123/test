using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.TTD.Delta
{
    public class Money
    {
        public string Amount { get; set; }
        public string CurrencyCode { get; set; }
    }

    public class AdGroupBudgetSettings
    {
        public Money Budget { get; set; }
        public Int64? BudgetInImpressions { get; set; }
        public Money DailyBudget { get; set; }
        public Int64? DailyBudgetInImpressions { get; set; }
        public List<AdGroupFlight> AdGroupFlights { get; set; }
        public bool PacingEnabled { get; set; }
        public string PacingMode { get; set; }
        public int AutoAllocatorPriority { get; set; }
    }

    public class AdGroupFlight
    {
        public string AdGroupId { get; set; }
        public Int64 CampaignFlightId { get; set; }
        public double BudgetInAdvertiserCurrency { get; set; }
        public Int64? BudgetInImpressions { get; set; }
        public double? DailyTargetInAdvertiserCurrency { get; set; }
        public Int64? DailyTargetInImpressions { get; set; }
    }

    public class CPMRate
    {
        public double Amount { get; set; }
        public string CurrencyCode { get; set; }
    }

    public class CPMRateInAdvertiserCurrency
    {
        public double Amount { get; set; }
        public string CurrencyCode { get; set; }
    }

    public class AudienceExcluderFee
    {
        public object PercentOfMediaCostRate { get; set; }
        public object PercentOfDataCostRate { get; set; }
        public CPMRate CPMRate { get; set; }
        public CPMRateInAdvertiserCurrency CPMRateInAdvertiserCurrency { get; set; }
        public object CPCRate { get; set; }
        public object CPCRateInAdvertiserCurrency { get; set; }
    }

    public class RecencyAdjustment
    {
        public int RecencyWindowStartInMinutes { get; set; }
        public double Adjustment { get; set; }
    }

    public class AdGroupCrossDeviceVendor
    {
        public bool AdjustBidsOnConfidence { get; set; }
        public int CrossDeviceVendorId { get; set; }
        public string CrossDeviceVendorName { get; set; }
        public Fee CrossDeviceVendorFee { get; set; }
    }

    public class TargetDemographicSettings
    {
        public string DataRateType { get; set; }
        public string CountryCode { get; set; }
        public string Gender { get; set; }
        public string StartAge { get; set; }
        public string EndAge { get; set; }
    }

    public class AdGroupAudienceTargeting
    {
        public bool AudienceExcluderEnabled { get; set; }
        public Fee AudienceExcluderFee { get; set; }
        public bool TargetTrackableUsersEnabled { get; set; }

        public bool TargetDemographicSettingsEnabled { get; set; }

        public TargetDemographicSettings TargetDemographicSettings { get; set; }
        public Fee TargetTrackableUsersFee { get; set; }
        public List<AdGroupCrossDeviceVendor> CrossDeviceVendorListForAudience { get; set; }
        public string AudienceId { get; set; }
        public List<RecencyAdjustment> RecencyAdjustments { get; set; }
        public int RecencyExclusionWindowInMinutes { get; set; }
        public bool AudiencePredictorEnabled { get; set; }
    }

    public class CPAInAdvertiserCurrency
    {
        public double Amount { get; set; }
        public string CurrencyCode { get; set; }
    }

    public class AdGroupROIGoal
    {
        public bool MaximizeReach { get; set; }
        public Money CPAInAdvertiserCurrency { get; set; }
        public double CTRInPercent { get; set; }
        public double NielsenOTPInPercent { get; set; }
        public Money CPCInAdvertiserCurrency { get; set; }
        public bool MaximizeConversionRevenue { get; set; }

        public double ReturnOnAdSpendPercent { get; set; }
        public double VCRInPercent { get; set; }
        public double ViewabilityInPercent { get; set; }
        public Money VCPMInAdvertiserCurrency { get; set; }
        public double GRPInPercent { get; set; }
        public Money CPCVInAdvertiserCurrency { get; set; }
    }

    public class FrequencyPricingSlopeCPM
    {
        public double Amount { get; set; }
        public string CurrencyCode { get; set; }
    }

    public class AdGroupFrequencySettings
    {
        public int? FrequencyCap { get; set; }
        public int? FrequencyPeriodInMinutes { get; set; }
        public Money FrequencyPricingSlopeCPM { get; set; }
    }

    public class AdGroupUserTimeTargeting
    {
        public List<BidAdjustment<Int32>> UserHourOfWeekAdjustments { get; set; }
        public double UserHourOfWeekUnknownAdjustment { get; set; }
        public bool UserHourOfWeekAdjustmentsEnabled { get; set; }
    }

    public class AdGroupSiteTargeting
    {
        public List<string> SiteListIds { get; set; }
        public double SiteListFallThroughAdjustment { get; set; }
    }

    public class AdGroupFoldTargeting
    {
        public double AboveFoldAdjustment { get; set; }
        public double BelowFoldAdjustment { get; set; }
        public double UnknownFoldAdjustment { get; set; }
    }

    public class Adjust
    {
        public int Id { get; set; }
        public double Adjustment { get; set; }
    }

    public class SupplyVendorAdjustments
    {
        public double DefaultAdjustment { get; set; }
        public List<Adjust> Adjustments { get; set; }
    }

    public class MobileCarrierAdjustments
    {
        public double DefaultAdjustment { get; set; }
        public List<object> Adjustments { get; set; }
    }

    public class BrowserAdjustments
    {
        public double DefaultAdjustment { get; set; }
        public List<object> Adjustments { get; set; }
    }

    public class OSAdjustments
    {
        public List<BidAdjustment<string>> OSVersionAdjustments { get; set; }
        public List<BidAdjustment<string>> OSFamilyAdjustments { get; set; }
        public double DefaultAdjustment { get; set; }
    }

    public class DeviceTypeAdjustments
    {
        public double DefaultAdjustment { get; set; }
        public List<object> Adjustments { get; set; }
    }

    public class AdGroupAutoOptimizationSettings
    {
        public bool IsBaseBidAutoOptimizationEnabled { get; set; }
        public bool IsAudienceAutoOptimizationEnabled { get; set; }
        public bool IsSiteAutoOptimizationEnabled { get; set; }
        public bool IsCreativeAutoOptimizationEnabled { get; set; }
        public bool IsSupplyVendorAutoOptimizationEnabled { get; set; }
        public bool IsUseClicksAsConversionsEnabled { get; set; }
        public bool IsUseSecondaryConversionsEnabled { get; set; }
    }

    public class AdGroupContractTargeting
    {
        public bool AllowOpenMarketBiddingWhenTargetingContracts { get; set; }
        public List<string> ContractIds { get; set; }
        public List<string> ContractGroupIds { get; set; }
        public List<BidAdjustment<string>> ContractAdjustments { get; set; }
        public List<BidAdjustment<string>> DeliveryProfileAdjustments { get; set; }
    }

    public class QualityAlliancePlayerSizeTargeting
    {
        public List<BidAdjustment<string>> QualityAlliancePlayerSizeAdjustments { get; set; }
    }

    public class AdGroupVideoTargeting
    {
        public QualityAlliancePlayerSizeTargeting QualityAlliancePlayerSizeTargeting { get; set; }
        public Fee QualityAlliancePlayerSizeTargetingFee { get; set; }
        public BidAdjustments<string> QualityAdjustments { get; set; }
        public BidAdjustments<string> SkippabilityAdjustments { get; set; }
        public BidAdjustments<string> MutedStateAdjustments { get; set; }
        public BidAdjustments<string> PlaybackTypeAdjustments { get; set; }
        public BidAdjustments<string> PlayerSizeAdjustments { get; set; }
    }

    public class IntegralContextualCategorySettings
    {
        public Fee FeeForContextualCategories { get; set; }
        public bool ContextualCategoriesEnabled { get; set; }
    }

    public class IntegralBrandSafetySettings
    {
        public Fee FeeForBrandSafety { get; set; }
        public Fee FeeForVideoBrandSafety { get; set; }
        public bool BrandSafetyEnabled { get; set; }
        public string AdultContent { get; set; }
        public string AlcoholContent { get; set; }
        public string DrugContent { get; set; }
        public string HateSpeechContent { get; set; }
        public string GamblingContent { get; set; }
        public string IllegalDownloadContent { get; set; }
        public string OffensiveLanguageContent { get; set; }
        public string ViolentContent { get; set; }
    }

    public class IntegralViewabilitySettings
    {
        public Fee FeeForViewabilityRating { get; set; }
        public string ViewabilityRating { get; set; }
        public Fee FeeForVideoViewabilityRating { get; set; }
        public string VideoViewabilityRating { get; set; }
    }

    public class IntegralSuspiciousActivitySettings
    {
        public Fee FeeForSuspiciousActivityRating { get; set; }
        public Fee FeeForVideoSuspiciousActivityRating { get; set; }
        public string SuspiciousActivityRating { get; set; }
    }

    public class IntegralSettings
    {
        public IntegralContextualCategorySettings IntegralContextualCategorySettings { get; set; }
        public IntegralBrandSafetySettings IntegralBrandSafetySettings { get; set; }
        public IntegralViewabilitySettings IntegralViewabilitySettings { get; set; }
        public IntegralSuspiciousActivitySettings IntegralSuspiciousActivitySettings { get; set; }
    }

    public class GrapeshotContextualCategorySettings
    {
        public Fee FeeForContextualCategories { get; set; }
        public bool ContextualCategoriesEnabled { get; set; }
    }

    public class GrapeshotBrandSafetySettings
    {
        public Fee FeeForBrandSafety { get; set; }
        public bool BrandSafetyEnabled { get; set; }
        public bool BlockProfanityAndHateSpeech { get; set; }
        public bool BlockIllegalDownload { get; set; }
        public bool BlockMatureContent { get; set; }
        public bool BlockDrugs { get; set; }
        public bool BlockTobacco { get; set; }
        public bool BlockFirearms { get; set; }
        public bool BlockCrime { get; set; }
        public bool BlockDeathMurder { get; set; }
        public bool BlockRacist { get; set; }
        public bool BlockMilitaryContent { get; set; }
        public bool BlockTerrorism { get; set; }
    }

    public class GrapeshotSettings
    {
        public GrapeshotContextualCategorySettings GrapeshotContextualCategorySettings { get; set; }
        public GrapeshotBrandSafetySettings GrapeshotBrandSafetySettings { get; set; }
    }

    public class DoubleVerifyContextualCategorySettings
    {
        public Fee FeeForContextualCategories { get; set; }
        public bool ContextualCategoriesEnabled { get; set; }
    }

    public class DoubleVerifyViewabilitySettings
    {
        public Fee FeeForViewability { get; set; }
        public Fee FeeForVideoViewability { get; set; }
        public string ViewabilityRating { get; set; }
        public string ViewabilityType { get; set; }
    }

    public class DoubleVerifyBrandSafetySettings
    {
        public Fee FeeForBrandSafety { get; set; }
        public bool BrandSafetyEnabled { get; set; }
        public string AdultContentPornographyMatureTopicsAndNudity { get; set; }
        public string AdultContentSwimsuit { get; set; }
        public string ControversialSubjectsAlternativeLifestyles { get; set; }
        public string ControversialSubjectsCelebrityGossip { get; set; }
        public string ControversialSubjectsGambling { get; set; }
        public string ControversialSubjectsOccult { get; set; }
        public string ControversialSubjectsSexEducation { get; set; }
        public string CopyrightInfringement { get; set; }
        public string DisasterAviation { get; set; }
        public string DisasterManMade { get; set; }
        public string DisasterNatural { get; set; }
        public string DisasterTerroristEvents { get; set; }
        public string DisasterVehicle { get; set; }
        public string DrugsAlcoholControlledSubstancesAlcohol { get; set; }
        public string DrugsAlcoholControlledSubstancesSmoking { get; set; }
        public string DrugsAlcoholControlledSubstancesSubstanceAbuse { get; set; }
        public string ExtremeGraphicExplicitViolenceWeapons { get; set; }
        public string AdImpressionFraud { get; set; }
        public string HateProfanity { get; set; }
        public string IllegalActivitiesCriminalSkills { get; set; }
        public string NuisanceSpywareMalwareWarez { get; set; }
        public string NegativeNewsFinancial { get; set; }
        public string NonStandardContentNonEnglish { get; set; }
        public string NonStandardContentParkingPage { get; set; }
        public string UnmoderatedUgcForumsImagesAndVideo { get; set; }
        public string AdServer { get; set; }
    }

    public class DoubleVerifyBotAvoidanceSettings
    {
        public Fee FeeForBotAvoidance { get; set; }
        public bool BotAvoidanceEnabled { get; set; }
    }

    public class DoubleVerifySettings
    {
        public DoubleVerifyContextualCategorySettings DoubleVerifyContextualCategorySettings { get; set; }
        public DoubleVerifyViewabilitySettings DoubleVerifyViewabilitySettings { get; set; }
        public DoubleVerifyBrandSafetySettings DoubleVerifyBrandSafetySettings { get; set; }
        public DoubleVerifyBotAvoidanceSettings DoubleVerifyBotAvoidanceSettings { get; set; }
    }

    public class SiteQualitySettings
    {
        public IntegralSettings IntegralSettings { get; set; }
        public GrapeshotSettings GrapeshotSettings { get; set; }
        public DoubleVerifySettings DoubleVerifySettings { get; set; }
    }

    public class Fee
    {
        public double? PercentOfMediaCostRate { get; set; }
        public double? PercentOfDataCostRate { get; set; }
        public Money CPMRate { get; set; }
        public Money CPMRateInAdvertiserCurrency { get; set; }
        public Money CPCRate { get; set; }
        public Money CPCRateInAdvertiserCurrency { get; set; }
    }

    public class NielsenSettings
    {
        public Fee Fee { get; set; }
        public NielsenTrackingAttributes NielsenTrackingAttributes { get; set; }
    }

    public class NielsenTrackingAttributes
    {
        public string EnhancedReportingOption { get; set; }
        public string Gender { get; set; }
        public string StartAge { get; set; }
        public string EndAge { get; set; }
        public List<string> Countries { get; set; }
    }

    public class ComscoreSettings
    {
        public Fee Fee { get; set; }
        public bool Enabled { get; set; }
        public int? PopulationId { get; set; }
        public List<int> DemographicMemberIds { get; set; }
    }

    public class AdGroupLanguageTargeting
    {
        public Fee Peer39LanguageTargetingFee { get; set; }
        public Fee GrapeshotLanguageTargetingFee { get; set; }
        public BidAdjustments<Int32> Peer39LanguageTargetingAdjustments { get; set; }
        public BidAdjustments<Int32> GrapeshotLanguageTargetingAdjustments { get; set; }
        public BidAdjustments<Int32> LanguageTargetingAdjustments { get; set; }
    }

    public class QualityAllianceViewabilityTargeting
    {
        public Fee Fee { get; set; }
        public string QualityAllianceViewabilityEnabledState { get; set; }
        public int QualityAllianceViewabilityMinimalPercentage { get; set; }
        public string QualityAllianceViewabilityProfile { get; set; }
    }

    public class BidAdjustment<TId>
    {
        public TId Id { get; set; }

        public double Adjustment { get; set; }
    }

    public class BidAdjustments<TId>
    {
        public double DefaultAdjustment { get; set; }

        public List<BidAdjustment<TId>> Adjustments { get; set; }
    }

    public class RTBAdGroupAttributes
    {
        public AdGroupBudgetSettings BudgetSettings { get; set; }
        public Money BaseBidCPM { get; set; }
        public Money MaxBidCPM { get; set; }
        //public AdGroupAudienceTargeting AudienceTargeting { get; set; }
        //public AdGroupROIGoal ROIGoal { get; set; }
        public AdGroupFrequencySettings FrequencySettings { get; set; }
        public List<string> CreativeIds { get; set; }
        //+public List<BidAdjustment<string>> AdFormatAdjustments { get; set; } :+ implies these columns were not included in the table in first iteration
        //+public List<BidAdjustment<string>> GeoSegmentAdjustments { get; set; }
        //public AdGroupUserTimeTargeting AdGroupUserTimeTargeting { get; set; }
        //public AdGroupSiteTargeting AdGroupSiteTargeting { get; set; }
        //public AdGroupFoldTargeting AdGroupFoldTargeting { get; set; }
        //*public BidAdjustments<Int64> SupplyVendorAdjustments { get; set; }: * implies include the RTBAdGroupAttributeId in the BidAdjustment table
        //*public BidAdjustments<Int32> MobileCarrierAdjustments { get; set; }
        //*public BidAdjustments<string> BrowserAdjustments { get; set; }
        //public OSAdjustments OsAdjustments { get; set; }
        //*public BidAdjustments<string> DeviceTypeAdjustments { get; set; }
        //*public BidAdjustments<string> RenderingContextAdjustments { get; set; }
        //public AdGroupAutoOptimizationSettings AdGroupAutoOptimizationSettings { get; set; }
        //public AdGroupContractTargeting AdGroupContractTargeting { get; set; }
        //public AdGroupVideoTargeting VideoTargeting { get; set; }
        public SiteQualitySettings SiteQualitySettings { get; set; }
        //public NielsenSettings NielsenSettings { get; set; }
        //public ComscoreSettings ComscoreSettings { get; set; }
        //public AdGroupLanguageTargeting AdGroupLanguageTargeting { get; set; }
        public QualityAllianceViewabilityTargeting QualityAllianceViewabilityTargeting { get; set; }
        public Int32? AdGroupLifetimeFrequencyCap { get; set; }
    }

    public class AdGroup
    {
        public string CampaignId { get; set; }
        public string AdGroupId { get; set; }
        public string AdGroupName { get; set; }
        public string Description { get; set; }
        public bool IsEnabled { get; set; }
        public Int64? IndustryCategoryId { get; set; }
        public RTBAdGroupAttributes RTBAttributes { get; set; }
        public string Availability { get; set; }
        public DateTime? CreatedAtUTC { get; set; }
        public DateTime? LastUpdatedAtUTC { get; set; }
    }

    public class RootAdGroup
    {
        public List<AdGroup> AdGroups { get; set; }
    }
}
