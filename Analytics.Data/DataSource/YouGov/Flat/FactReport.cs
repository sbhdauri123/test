namespace Greenhouse.Data.DataSource.YouGov.Flat
{
    public class FactReport
    {
        public string date { get; set; }

        public string sector_id { get; set; }

        public string brand_id { get; set; }

        public string region { get; set; }

        public string metric { get; set; }
        public string volume { get; set; }

        public string score { get; set; }

        public string positives { get; set; }

        public string negatives { get; set; }

        public string neutrals { get; set; }

        public string positives_neutrals { get; set; }

        public string negatives_neutrals { get; set; }
    }
}
