using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Facebook.Dimension.AdCreative;

public class ObjectStorySpec
{
    public string page_id { get; set; }
    public string instagram_actor_id { get; set; }
    public VideoData video_data { get; set; }
    public LinkData link_data { get; set; }
    public LinkData template_data { get; set; }
}

public class LinkData
{
    public string link { get; set; }
    public List<AdCreativeLinkDataChildAttachment> child_attachments { get; set; }
}

public class AdCreativeLinkDataChildAttachment
{
    public string link { get; set; }
}

public class VideoData
{
    public string video_id { get; set; }
    public string title { get; set; }
    public string message { get; set; }
    public CallToAction call_to_action { get; set; }
    public string image_url { get; set; }
    public string image_hash { get; set; }
}

public class CallToAction
{
    public string type { get; set; }
    public CallToActionValue value { get; set; }
}

public class CallToActionValue
{
    public string link_caption { get; set; }
    public string link { get; set; }
    public string link_format { get; set; }
}
