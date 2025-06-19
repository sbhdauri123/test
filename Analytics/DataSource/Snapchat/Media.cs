using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public partial class MediaRoot
    {
        [JsonProperty("request_status")]
        public string RequestStatus { get; set; }

        [JsonProperty("request_id")]
        public string RequestId { get; set; }

        [JsonProperty("media")]
        public Medias[] Media { get; set; }

        [JsonProperty("paging")]
        public Paging Paging { get; set; }
    }

    public partial class Medias
    {
        [JsonProperty("sub_request_status")]
        public string SubRequestStatus { get; set; }

        [JsonProperty("media")]
        public Media Media { get; set; }
    }

    public partial class Media
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("updated_at")]
        public string UpdatedAt { get; set; }

        [JsonProperty("created_at")]
        public string CreatedAt { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("ad_account_id")]
        public string AdAccountId { get; set; }

        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("media_status")]
        public string MediaStatus { get; set; }

        [JsonProperty("file_name", NullValueHandling = NullValueHandling.Ignore)]
        public string FileName { get; set; }

        [JsonProperty("download_link", NullValueHandling = NullValueHandling.Ignore)]
        public string DownloadLink { get; set; }

        [JsonProperty("duration_in_seconds", NullValueHandling = NullValueHandling.Ignore)]
        public string DurationInSeconds { get; set; }

        [JsonProperty("video_metadata", NullValueHandling = NullValueHandling.Ignore)]
        public VideoMetadata VideoMetadata { get; set; }

        [JsonProperty("file_size_in_bytes", NullValueHandling = NullValueHandling.Ignore)]
        public string FileSizeInBytes { get; set; }

        [JsonProperty("is_demo_media")]
        public string IsDemoMedia { get; set; }

        [JsonProperty("hash", NullValueHandling = NullValueHandling.Ignore)]
        public string Hash { get; set; }

        [JsonProperty("visibility")]
        public string Visibility { get; set; }

        [JsonProperty("image_metadata", NullValueHandling = NullValueHandling.Ignore)]
        public ImageMetadata ImageMetadata { get; set; }

        [JsonProperty("lens_package_metadata", NullValueHandling = NullValueHandling.Ignore)]
        public LensPackageMetadata LensPackageMetadata { get; set; }

        [JsonProperty("demo_media_id", NullValueHandling = NullValueHandling.Ignore)]
        public string DemoMediaId { get; set; }
    }

    public partial class ImageMetadata
    {
        [JsonProperty("height_px")]
        public string HeightPx { get; set; }

        [JsonProperty("width_px")]
        public string WidthPx { get; set; }

        [JsonProperty("image_format")]
        public string ImageFormat { get; set; }
    }

    public partial class LensPackageMetadata
    {
        [JsonProperty("lens_icon_media_id")]
        public string LensIconMediaId { get; set; }

        [JsonProperty("default_camera")]
        public string DefaultCamera { get; set; }
    }

    public partial class VideoMetadata
    {
        [JsonProperty("width_px")]
        public string WidthPx { get; set; }

        [JsonProperty("height_px")]
        public string HeightPx { get; set; }

        [JsonProperty("rotation")]
        public string Rotation { get; set; }

        [JsonProperty("integrated_loudness")]
        public string IntegratedLoudness { get; set; }

        [JsonProperty("true_peak")]
        public string TruePeak { get; set; }
    }
}
