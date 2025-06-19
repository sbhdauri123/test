using Greenhouse.Data.DataSource.Skai.CustomMetrics;
using Greenhouse.Utilities;
using NLog;
using Polly;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.Skai
{
    public class SkaiServiceArguments
    {
        public ParallelOptions ParallelOptions { get; init; }
        public Action<LogLevel, string> LogMessage { get; set; }
        public Action<LogLevel, string, Exception> LogException { get; set; }
        public SkaiOAuth SkaiOAuth { get; set; }
        public List<SkaiSavedColumn> CustomColumns { get; set; }
        public IHttpClientProvider HttpClientProvider { get; set; }
        public string EndpointUri { get; set; }
        public ResiliencePipeline ResiliencePipeline { get; set; }
        public GuardrailConfig GuardrailConfig { get; set; }
    }
}
