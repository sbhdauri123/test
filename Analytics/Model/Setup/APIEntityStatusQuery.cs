namespace Greenhouse.Data.Model.Setup
{
    public class APIEntityStatusQuery : BasePOCO
    {
        public string SourceID { get; set; }
        public string RedshiftQuery { get; set; }
        public string SourceName { get; set; }
    }
}
