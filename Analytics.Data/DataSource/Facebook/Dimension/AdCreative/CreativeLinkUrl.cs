namespace Greenhouse.Data.DataSource.Facebook.Dimension.AdCreative;
public record CreativeLinkUrl
{
    public string CreativeID { get; init; }
    public string AccountID { get; init; }
    public string AdID { get; init; }
    public string LinkUrl { get; init; }
    public string LinkDataParentObject { get; init; }
}
