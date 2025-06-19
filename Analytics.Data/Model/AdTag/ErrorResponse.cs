using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;
using System.Net;

namespace Greenhouse.Data.Model.AdTag
{
    public class ErrorResponse : BasePOCO
    {
        public string webExceptionResponse = string.Empty;
        public string errorCode = string.Empty;
        public string errorMessage = string.Empty;

        public ErrorResponse(WebException wex)
        {
            StreamReader streamReader = new StreamReader(wex.Response.GetResponseStream());
            webExceptionResponse = streamReader.ReadToEnd();
            streamReader.Close();
            streamReader.Dispose();
            var errorResponse = JsonConvert.DeserializeObject<ErrorResp>(webExceptionResponse, new JsonSerializerSettings() { MissingMemberHandling = MissingMemberHandling.Ignore });
            errorCode = errorResponse.error.code.ToString();
            errorMessage = errorResponse.error.message;
        }
    }

    public class Errors
    {
        public string domain { get; set; }
        public string reason { get; set; }
        public string message { get; set; }
    }

    public class Error
    {
        public List<Errors> errors { get; set; }
        public int code { get; set; }
        public string message { get; set; }
    }

    public class ErrorResp
    {
        public Error error { get; set; }
    }
}
