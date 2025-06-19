using Greenhouse.Caching;
using Greenhouse.Configuration.Extensions;
using Greenhouse.JobService.Consumers;
using Greenhouse.JobService.Extensions;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using Greenhouse.Utilities.IO;
using MassTransit;
using NLog;
using NLog.Extensions.Logging;
using NLog.Web;

Logger logger = LogManager.Setup()
    .LoadConfigurationFromAppSettings()
    .GetCurrentClassLogger();

logger.Info("--> JobService Started...");
logger.Debug("--> ASPNETCORE_ENVIRONMENT: {environment}", Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"));

try
{
    WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
    builder.AddServiceDefaults();

    // options 
    builder.Services.AddGreenhouseConfiguration(builder.Configuration);

    // caching
    builder.Services.AddMemoryCache();

    // services
    builder.Services.AddHttpClient<IHttpClientProvider, HttpClientProvider>();
    builder.Services.AddSingleton<ITokenCache, TokenCache>();
    builder.Services.AddSingleton<IStreamProcessor, TempFileStreamProcessor>();
    builder.Services.AddJobServices();

    // messaging
    builder.Services.AddMassTransit(s =>
    {
        s.AddConsumer<JobConsumer>();
        s.UsingRabbitMq((context, config) =>
        {
            config.Host(new Uri(builder.Configuration["ConnectionStrings:rabbitmq"]!));
            config.ReceiveEndpoint("job-execution-queue", cfg =>
            {
                cfg.ConfigureConsumeTopology = false;
                cfg.ConfigureConsumer<JobConsumer>(context);
            });
            config.ConfigureEndpoints(context);
        });
    });

    // logger configuration
    builder.Logging.ClearProviders();
    builder.Logging.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace);
    builder.Logging.AddNLog(builder.Configuration);
    builder.Services.AddSingleton<IJobLoggerFactory, JobLoggerFactory>();
    builder.Services.AddSingleton<NLog.ILogger>(_ => LogManager.GetCurrentClassLogger());

    await using WebApplication app = builder.Build();
    app.MapDefaultEndpoints();
    app.Run();
}
catch (Exception ex)
{
    System.Diagnostics.Debug.WriteLine(ex.Message);
    logger.Error(ex, "--> JobService Stopped because of exception");
    throw;
}
finally
{
    logger.Debug("--> Shutting down JobService...");
    LogManager.Shutdown();
}