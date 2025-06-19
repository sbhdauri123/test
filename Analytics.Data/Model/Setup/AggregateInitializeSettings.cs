using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.Model.Setup;

[Serializable]
public class AggregateInitializeSettings
{
    /// <summary>
    /// The Aggregate Initialize settings is an object stored in the Source Table
    /// RegularAggregateDays contains the list of days to retrieve on a day,
    /// for example [-1,-2,-8] will retrieve yesterday, the day before and 8 days ago
    ///
    /// BackFillAggregateDays is similar to RegularAggregateDays, the only difference is
    /// that the queue created have isBackfill = true
    ///
    /// TrueUpDetails contains a list of True up details.
    /// TriggerDayRegex is a regular expression executed against an expression of the day
    /// generated in AggregateInitialize.
    /// It should look like "THURSDAY" If you want that TrueUpDetail to be selected to run
    /// on Thursdays, or "15" if you want that TrueUpDetail to be selected to run on the 15th of each month
    ///
    /// PeriodToRetrieve contains the period to retrieve for the matching TrueUpDetails selected
    /// It can either be to retrieve the previous week or the previous month
    ///
    /// The priority is useful when you have different TrueUp regex. Let's say you have a Regex "15" that retrieves a month
    /// and another "SATURDAY" that retrieves a week, like the following:
    ///
    ///{trueUpDetails:[{triggerDay:'Saturday', period:'weekly', priority: 0}], [{triggerDay:'15', period:'monthly', priority: 1}]}
    ///
    /// Here we set a higher priority to "15" so if there is a Saturday 15th in the month, that day the TrueUpDetail
    /// selected will be the one with the highest priority  (triggerDay: '15' in our example).
    ///
    /// </summary>
    [JsonProperty("regularAggregateDays")]
    public IEnumerable<int> RegularAggregateDays { get; set; }

    [JsonProperty("backFillAggregateDays")]
    public IEnumerable<int> BackFillAggregateDays { get; set; }

    [JsonProperty("useBFDateBucket")]
    public bool UseBFDateBucket { get; set; }

    [JsonProperty("trueUpDetails")]
    public ICollection<TrueUp> TrueUpDetails { get; set; }

    [Serializable]
    public class TrueUp
    {
        [JsonProperty("triggerDay")]
        public string TriggerDayRegex { get; set; }

        [JsonProperty("period")]
        public PeriodEnum PeriodToRetrieve { get; set; }

        public int Priority { get; set; }

        [JsonProperty("interval")]
        public IntervalEnum Interval { get; set; }

        [JsonProperty("queueIntegrity")]
        public QueueIntegrityEnum QueueIntegrity { get; set; }
    }

    public enum PeriodEnum { Weekly, Monthly, Single }
    public enum IntervalEnum { Every, First, Last, Daily, LastDayOfTheMonth }
    public enum QueueIntegrityEnum { AllCreated, OnlyLatest }
}
