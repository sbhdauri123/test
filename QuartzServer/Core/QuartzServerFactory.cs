using Common.Logging;
using System;

namespace Greenhouse.QuartzServer.Core
{
    public class QuartzServerFactory
    {
        private static readonly ILog logger = LogManager.GetLogger(typeof(QuartzServerFactory));

        public static IQuartzServer CreateServer()
        {
            string implementationType = Configuration.ServerImplementationType;
            Type type = Type.GetType(implementationType, true);
            QuartzServerFactory.logger.Debug((object)("Creating new instance of server type '" + implementationType + "'"));
            IQuartzServer quartzServer = (IQuartzServer)Activator.CreateInstance(type);
            QuartzServerFactory.logger.Debug((object)"Instance successfully created");
            return quartzServer;
        }
    }
}
