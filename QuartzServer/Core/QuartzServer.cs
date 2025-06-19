
using Common.Logging;
using Quartz;
using Quartz.Impl;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Greenhouse.QuartzServer.Core
{
    public class QuartzServer : IQuartzServer
    {
        private readonly ILog logger;
        private ISchedulerFactory schedulerFactory;
        private IScheduler scheduler;

        public QuartzServer()
        {
            this.logger = LogManager.GetLogger(this.GetType());
        }

        public virtual async Task InitializeAsync()
        {
            try
            {
                this.schedulerFactory = this.CreateSchedulerFactory();
                this.scheduler = await this.schedulerFactory.GetScheduler();
            }
            catch (Exception ex)
            {
                this.logger.Error((object)("Server initialization failed:" + ex.Message), ex);
                throw;
            }
        }

        protected virtual ISchedulerFactory CreateSchedulerFactory()
        {
            return (ISchedulerFactory)new StdSchedulerFactory();
        }

        public virtual async Task StartAsync()
        {
            await scheduler.Start();
            try
            {
                Thread.Sleep(3000);
            }
            catch (ThreadInterruptedException)
            {
            }
            this.logger.Info((object)"Scheduler started successfully");
        }

        public virtual async Task StopAsync()
        {
            await this.scheduler.Shutdown(false);
            this.logger.Info((object)"Scheduler shutdown complete");
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {

            }
        }

        ~QuartzServer()
        {
            Dispose(false);
        }
    }
}
