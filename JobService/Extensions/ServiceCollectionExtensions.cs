using Greenhouse.Jobs.Infrastructure;
using Greenhouse.JobService.Interfaces;
using Greenhouse.JobService.Services;
using Greenhouse.JobService.Strategies.Factory;
using Greenhouse.JobService.Strategies.LinkedIn;

namespace Greenhouse.JobService.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddJobServices(this IServiceCollection services)
    {
        services.AddScoped<IJobStrategy, JobStrategy>();
        // add more strategies here

        services.AddScoped<IDragoJob, Jobs.Aggregate.LinkedIn.ImportJob>();
        // add more jobs here

        services.AddScoped<IJobExecutionService, JobExecutionService>();
        services.AddScoped<IJobStrategyFactory, JobStrategyFactory>();

        return services;
    }
}