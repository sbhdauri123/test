using Greenhouse.Caching;
using Greenhouse.Configuration;
using Greenhouse.Contracts.Messages;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Jobs.Infrastructure.IOC;
using Greenhouse.Logging;
using Greenhouse.Server;
using Greenhouse.Utilities;
using Greenhouse.Utilities.IO;
using Greenhouse.WinBackgroundService;
using MassTransit;
using NLog;
using NLog.Extensions.Logging;
using Quartz;
using System.Collections.Specialized;

Logger logger = LogManager.Setup()
    .SetupExtensions(builder =>
        builder.RegisterLayoutRenderer<DotNetRuntimeVersionLayoutRenderer>("dotnet-runtime-version"))
    .GetCurrentClassLogger();

logger.Info("--> ASPNETCORE_ENVIRONMENT: {environment}",
    System.Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"));

try
{
    HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);
    builder.AddServiceDefaults();

    Console.WriteLine(
        $"Greenhouse Environment is configured as: {builder.Configuration["GreenhouseApplicationSettings:Environment"]}");

    // messaging
    string rabbitMqHostUri = builder.Configuration["ConnectionStrings:rabbitmq"];
    if (string.IsNullOrWhiteSpace(rabbitMqHostUri))
    {
        builder.Services.AddSingleton<IBus, NullBus>();
    }
    else
    {
        builder.Services.AddMassTransit(s =>
        {
            s.UsingRabbitMq((context, cfg) =>
            {
                cfg.Host(new Uri(rabbitMqHostUri));
                cfg.Message<ExecuteJob>(e => e.SetEntityName("job-execution-queue"));
                cfg.ConfigureEndpoints(context);
            });
        });
    }

    builder.Services.AddWindowsService(options =>
    {
        options.ServiceName =
            $"GreenhouseWinService${Greenhouse.Configuration.Settings.Current.ServiceUser.ServiceName}";
    });

    logger.Debug(
        "ServiceName=GreenhouseWinService${serviceName}", Greenhouse.Configuration.Settings.Current.ServiceUser.ServiceName);

    // Add Quartz services
    NameValueCollection properties = Greenhouse.Configuration.Settings.GetDefaultQuartzConfig();
    builder.Services.AddQuartz(properties);
    logger.Debug(
        $"Greenhouse.Configuration.Settings.GetDefaultQuartzConfig={string.Join(" - ", properties.AllKeys.Select(key => $"{key}: {properties[key]}"))}");

    // Register IJob and IDrago jobs
    IEnumerable<Type> jobTypes = NativeJobRegistrar.GetJobTypes();
    foreach (Type jobType in jobTypes)
    {
        builder.Services.AddTransient(jobType);
        logger.Debug("add transientJob={jobTypeFullName}", jobType.FullName);
    }

    // caching
    builder.Services.AddSingleton(typeof(ICacheStore),
        new Greenhouse.Caching.Memory.MemoryCacheStore(new Greenhouse.Caching.Serializers.JsonSerializer()));
    builder.Services.AddMemoryCache();

    // logging
    builder.Logging.AddNLog(builder.Configuration);
    builder.Services.AddSingleton<NLog.ILogger>(_ => LogManager.GetCurrentClassLogger());
    builder.Services.AddSingleton<IJobLoggerFactory, JobLoggerFactory>();

    // services
    builder.Services.AddHttpClient<IHttpClientProvider, HttpClientProvider>();
    builder.Services.AddSingleton<ITokenCache, TokenCache>();
    builder.Services.AddSingleton<IJobExecutionHandler, JobExecutionHandler>();
    builder.Services.AddSingleton<ILookupService, LookupServiceWrapper>();
    builder.Services.AddSingleton<IStreamProcessor, TempFileStreamProcessor>();

    Server server;
    string serviceName = Greenhouse.Configuration.Settings.Current.ServiceUser.ServiceName;
    logger.Debug($"Settings.Current.ServiceUser.ServiceName: {serviceName ?? "NULL!"}");

    if (string.Equals(serviceName, "local", StringComparison.InvariantCultureIgnoreCase))
    {
        server = new Server { ServerAlias = serviceName };
    }
    else
    {
        string? cs = Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseConfigDbConnectionString;
        logger.Debug(
            $"Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseConfigDbConnectionString: {(cs == null ? "NULL!" : cs.Substring(0, 55))}");

        List<Server> servers = SetupService.GetAll<Server>()
            .Where(s => s.ServerAlias == serviceName).ToList();

        switch (servers.Count)
        {
            case 0:
                logger.Fatal(
                    "No matching Greenhouse.Data.Model.Setup.Server record found matching this service name: {serviceName}. This Quartz scheduler will not be able to schedule jobs", serviceName);
                break;
            case > 1:
                logger.Fatal(
                    "Multiple Greenhouse.Data.Model.Setup.Server records found matching this service name: {serviceName}. There should be one and only one Server record.", serviceName);
                break;
        }

        server = servers.First();
    }

    logger.Debug(
        "Server: ServerMachineName={serverMachineName} ServerID={serverID} ServerIP={serverIP}", server.ServerMachineName, server.ServerID, server.ServerIP);

    builder.Services.AddSingleton(typeof(Server), server);
    builder.Services.AddSingleton(typeof(SchedulerServer), new SchedulerServer(server));
    builder.Services.AddHostedService<GreenhouseWinService>();

    IHost host = builder.Build();

    using IServiceScope serviceScope = host.Services.CreateScope();
    ISchedulerFactory schedulerFactory = serviceScope.ServiceProvider.GetService<ISchedulerFactory>()!;
    SchedulerServer schedulerServer = serviceScope.ServiceProvider.GetService<SchedulerServer>()!;
    schedulerServer.SchedulerInstance = await schedulerFactory.GetScheduler();

    host.Run();
}
catch (Exception e)
{
    System.Diagnostics.Debug.WriteLine(e.Message);
    logger.Error(e, "--> Stopped because of exception.");
    throw;
}
finally
{
    logger.Debug("--> Shutting down...");
    LogManager.Shutdown();
}