using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Jobs.Infrastructure.IOC
{
    /// <summary>
    /// This job registry has all Quartz native jobs (IJob derived classes) to be resolved by the JobFactory
    /// </summary>
    public static class NativeJobRegistrar
    {
        public static IEnumerable<Type> GetJobTypes()
        {
            var v = AppDomain.CurrentDomain.GetAssemblies().Where(t => t.FullName.Contains("Greenhouse")).ToList()
              .SelectMany(s => s.GetTypes()).Where(p => (typeof(IDragoJob).IsAssignableFrom(p) && !p.IsInterface)
                  || (typeof(IJob).IsAssignableFrom(p) && !p.IsInterface));

            return v;
        }
    }
}