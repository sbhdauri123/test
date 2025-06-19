using Microsoft.Extensions.DependencyInjection;
using Quartz;
using Quartz.Spi;
using System;

namespace Greenhouse.Jobs.Infrastructure.IOC
{
    public class SingletonJobFactory : IJobFactory
    {
        protected readonly IServiceScope _scope;
        public SingletonJobFactory(IServiceProvider serviceProvider)
        {
            _scope = serviceProvider.CreateScope();
        }

        public IJob NewJob(TriggerFiredBundle bundle, IScheduler scheduler)
        {
            return _scope.ServiceProvider.GetRequiredService(bundle.JobDetail.JobType) as IJob;
        }

        public void ReturnJob(IJob job) { }
    }
}