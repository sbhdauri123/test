namespace Greenhouse.Data.DataSource.Facebook.Dimension.AdCreative;

public class AssetFeedSpec
{
    public Video[] videos { get; set; }
    public Body[] bodies { get; set; }
    public string[] call_to_action_types { get; set; }
    public CallToActions[] call_to_actions { get; set; }
    public Description[] descriptions { get; set; }
    public LinkUrls[] link_urls { get; set; }
    public Title[] titles { get; set; }
    public string[] ad_formats { get; set; }
    public AssetCustomizationRules[] asset_customization_rules { get; set; }
    public string optimization_type { get; set; }
    public AdditionalData additional_data { get; set; }
    public bool reasons_to_shop { get; set; }
    public bool shops_bundle { get; set; }
}

public class AdditionalData
{
    public bool multi_share_end_card { get; set; }
    public bool is_click_to_message { get; set; }
}

public class Video
{
    public Adlabel[] adlabels { get; set; }
    public string video_id { get; set; }
    public string thumbnail_url { get; set; }
}

public class Adlabel
{
    public string name { get; set; }
    public string id { get; set; }
}

public class Body
{
    public Adlabel[] adlabels { get; set; }
    public string text { get; set; }
}

public class CallToActions
{
    public string type { get; set; }
    public Value value { get; set; }
}

public class Value
{
    public string lead_gen_form_id { get; set; }
}

public class Description
{
    public string text { get; set; }
}

public class LinkUrls
{
    public Adlabel[] adlabels { get; set; }
    public string website_url { get; set; }
    public string display_url { get; set; }
}

public class Title
{
    public Adlabel[] adlabels { get; set; }
    public string text { get; set; }
}

public class AssetCustomizationRules
{
    public CustomizationSpec customization_spec { get; set; }
    public VideoLabel video_label { get; set; }
    public BodyLabel body_label { get; set; }
    public LinkUrlLabel link_url_label { get; set; }
    public TitleLabel title_label { get; set; }
    public int priority { get; set; }
}

public class CustomizationSpec
{
    public int age_max { get; set; }
    public int age_min { get; set; }
    public string[] publisher_platforms { get; set; }
    public string[] instagram_positions { get; set; }
}

public class VideoLabel
{
    public string name { get; set; }
    public string id { get; set; }
}

public class BodyLabel
{
    public string name { get; set; }
    public string id { get; set; }
}

public class LinkUrlLabel
{
    public string name { get; set; }
    public string id { get; set; }
}

public class TitleLabel
{
    public string name { get; set; }
    public string id { get; set; }
}
