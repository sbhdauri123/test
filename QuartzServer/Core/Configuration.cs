
using System.Collections.Specialized;
using System.Configuration;

namespace Greenhouse.QuartzServer.Core
{
    public static class Configuration
    {
        private static readonly string DefaultServerImplementationType = typeof(QuartzServer).AssemblyQualifiedName;
        private static readonly NameValueCollection configuration = (NameValueCollection)ConfigurationManager.GetSection("quartz");

        public static string ServiceName
        {
            get
            {
                return Configuration.GetConfigurationOrDefault("quartz.server.serviceName", "QuartzServer");
            }
        }

        public static string ServiceDisplayName
        {
            get
            {
                return Configuration.GetConfigurationOrDefault("quartz.server.serviceDisplayName", "Quartz Server");
            }
        }

        public static string ServiceDescription
        {
            get
            {
                return Configuration.GetConfigurationOrDefault("quartz.server.serviceDescription", "Quartz Job Scheduling Server");
            }
        }

        public static string ServerImplementationType
        {
            get
            {
                return Configuration.GetConfigurationOrDefault("quartz.server.type", Configuration.DefaultServerImplementationType);
            }
        }

        private static string GetConfigurationOrDefault(string configurationKey, string defaultValue)
        {
            string str = (string)null;
            if (Configuration.configuration != null)
                str = Configuration.configuration[configurationKey];
            if (str == null || str.Trim().Length == 0)
                str = defaultValue;
            return str;
        }
    }
}
