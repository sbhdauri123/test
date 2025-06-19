using Amazon.S3;
using Amazon.SecretsManager;
using Greenhouse.Configuration;
using Greenhouse.Data.Model.Auth;
using Greenhouse.Data.Services;
using Greenhouse.UI;
using Greenhouse.UI.Infrastructure;
using Greenhouse.UI.Services.AdTag;
using Greenhouse.UI.Services.Setup;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.Authorization;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;
using NLog;
using NLog.Extensions.Logging;
using Quartz;
using System.Security.Claims;

Logger logger = LogManager.Setup()
    .SetupExtensions(builder =>
        builder.RegisterLayoutRenderer<DotNetRuntimeVersionLayoutRenderer>("dotnet-runtime-version"))
    .GetCurrentClassLogger();

logger.Info("--> ASPNETCORE_ENVIRONMENT: {environment}",
    Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"));

try
{
    WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

    builder.Services.AddAWSService<IAmazonS3>();
    builder.Services.AddAWSService<IAmazonSecretsManager>();

    // logging
    builder.Logging.ClearProviders();
    builder.Logging.AddNLog(builder.Configuration);
    builder.Services.AddSingleton<NLog.ILogger>(_ => LogManager.GetCurrentClassLogger());

    builder.Services.AddSingleton(typeof(Greenhouse.Data.Services.AdTagService), new Greenhouse.Data.Services.AdTagService());

    builder.Services.AddAuthentication(OpenIdConnectDefaults.AuthenticationScheme)
        .AddCookie()
        .AddOpenIdConnect(
        options =>
        {
            options.SignInScheme = CookieAuthenticationDefaults.AuthenticationScheme;
            options.ClientId = Settings.Current.AzureActiveDirectory.ClientID;
            options.CallbackPath = Settings.Current.AzureActiveDirectory.RedirectURI;
            options.ResponseType = OpenIdConnectResponseType.CodeIdToken;
            options.SignedOutRedirectUri = "https://login.microsoftonline.com/common/oauth2/v2.0/logout";
            options.Authority = $"https://login.microsoftonline.com/{Settings.Current.AzureActiveDirectory.TenantID}/v2.0";
            options.ClientSecret = Settings.Current.AzureActiveDirectory.ClientSecret;
            options.TokenValidationParameters = new TokenValidationParameters
            {
                // Ensure that User.Identity.Name is set correctly after login
                NameClaimType = "name",
                ValidateIssuer = false,
            };
            options.Events = new OpenIdConnectEvents
            {
                OnAuthorizationCodeReceived = (context) =>
                {
                    ClaimsIdentity claimsIdentity = (ClaimsIdentity)context.Principal.Identity;

                    string userName = context.Principal.Claims.First(c => c.Type.EndsWith("preferred_username")).Value.Split('@').First();

                    var displayName = context.Principal.Claims.First(c => c.Type.EndsWith("name")).Value;

                    var allUsers = SetupService.GetAll<UserAuthorization>();
                    var user = allUsers.FirstOrDefault(x => x.SAMAccountName.Equals(userName, StringComparison.InvariantCultureIgnoreCase));

                    if (user != null)
                    {
                        claimsIdentity.AddClaim(new Claim(ClaimTypes.Name, displayName));
                        if (user.IsAdmin)
                        {
                            claimsIdentity.AddClaim(new Claim(ClaimTypes.Role, "admin"));
                        }
                    }
                    else
                    {
                        throw new UnauthorizedAccessException("User not authorized");
                    };
                    return Task.CompletedTask;
                },
                OnAuthenticationFailed = context =>
                {
                    context.Response.Redirect("/Home/Error");
                    context.HandleResponse(); // Suppress the exception
                    return Task.CompletedTask;
                }
            };
        });

    builder.Services.AddControllersWithViews(options =>
    {
        var policy = new AuthorizationPolicyBuilder()
            .RequireAuthenticatedUser()
            .Build();
        options.Filters.Add(new AuthorizeFilter(policy));
    }).AddJsonOptions(jsonOptions =>
    {
        jsonOptions.JsonSerializerOptions.PropertyNamingPolicy = null;//makes Json Pascal
    });

    builder.Services.AddSignalR().AddJsonProtocol(options =>
    {
        options.PayloadSerializerOptions.PropertyNamingPolicy = null; // SignalR changes model names to camel case, so this option disables this feature
    });

    builder.Services.AddWebOptimizer(pipeline =>
    {
        var foo = Directory.GetCurrentDirectory();

        pipeline.AddJavaScriptBundle("/bundles/jquery", "/Scripts/jquery-2.2.4.js");
        pipeline.AddJavaScriptBundle("/bundles/jqueryval", "/Scripts/jquery.validate*");
        pipeline.AddJavaScriptBundle("/bundles/modernizr", "/Scripts/modernizr-*");
        pipeline.AddJavaScriptBundle("/bundles/libs", "/Scripts/Greenhouse/Foundation.js"
                , "/Scripts/move-top.js"
                , "/Scripts/easing.js");
        pipeline.AddJavaScriptBundle("/bundles/bootstrap", "/Scripts/bootstrap.js",
                      "/Scripts/respond.js");
        pipeline.AddJavaScriptBundle("/bundles/kendo", "/Scripts/kendo.all.min.js");
        pipeline.AddJavaScriptBundle("/bundles/unobtrusive", "/Scripts/jquery.unobtrusive*");
        pipeline.AddCssBundle("/Content/css", "/Content/bootstrap.css",
                      "/Content/site.css",
                      "/Content/kendo.common.min.css",
                      "/Content/kendo.default.min.css",
                      "/Content/font-awesome.css");
        pipeline.AddJavaScriptBundle("/bundles/signalr", "/Scripts/jquery.signalR-2.2.2.js");
        pipeline.AddJavaScriptBundle("/bundles/kendo", "/Scripts/kendo/kendo.web.min.js",
                "/Scripts/kendo/jszip.min.js");
        pipeline.AddCssBundle("/Content/kendo/css", "/Content/kendo/kendo.common-bootstrap.css",
                "/Content/kendo/kendo.bootstrap.css");
    });

    builder.Services.AddQuartz(q =>
    {
        q.UsePersistentStore(store =>
        {
            // it's generally recommended to stick with
            // string property keys and values when serializing
            store.UseProperties = true;
            store.UseSqlServer(Settings.Current.Quartz.ConnectionString);

            store.UseNewtonsoftJsonSerializer();
        });
    });

    builder.Services.AddSingleton(typeof(SharedJobSchedulers), new SharedJobSchedulers());

    WebApplication app = builder.Build();

    if (!builder.Environment.IsProduction())
    {
        app.UseDeveloperExceptionPage();
    }

    app.UseHttpsRedirection();
    app.UseWebOptimizer();
    app.UseStaticFiles();

    app.UseAuthentication();
    app.UseAuthorization();

    app.MapHub<ServerHub>("/serverHub");
    app.MapHub<GreenhouseUIMainHub>("/greenhouseUIMainHub");
    app.MapHub<InstanceHub>("/instanceHub");
    app.MapHub<DataSourceHub>("/dataSourceHub");
    app.MapHub<SourceHub>("/sourceHub");
    app.MapHub<SourceFileHub>("/sourceFileHub");
    app.MapHub<CredentialHub>("/credentialHub");
    app.MapHub<IntegrationHub>("/integrationHub");
    app.MapHub<APIEntityHub>("/apiEntityHub");
    app.MapHub<CustomFieldHub>("/customFieldHub");
    app.MapHub<ManageUserHub>("/manageUserHub");
    app.MapHub<AdvertiserAdminHub>("/advertiserAdminHub");
    app.MapHub<AuthorizationHub>("/authorizationHub");
    app.MapHub<LookupHub>("/lookupHub");
    app.MapHub<AdvertiserHub>("/advertiserHub");
    app.MapHub<JobRunHub>("/jobRunHub");

    app.UseRouting();

    // global cors policy
    app.UseCors(x => x
        .AllowAnyMethod()
        .AllowAnyHeader()
        .SetIsOriginAllowed(origin => true) // allow any origin 
        .AllowCredentials());

    app.MapControllerRoute(
        name: "RegexBuilder",
        pattern: "Utilities/RegexBuilder",
        defaults: new { controller = "Regex", action = "Index" });

    app.MapControllerRoute(
        name: "Authorization",
        pattern: "Utilities/Authorization",
        defaults: new { controller = "Authorize", action = "Index" });

    app.MapControllerRoute(
        name: "Setups",
        pattern: "{controller=Setup}/{action=Index}");

    app.MapControllerRoute(
        name: "default",
        pattern: "{controller=Home}/{action=Index}/{id?}");

    app.Run();
}
catch (Exception e)
{
    System.Diagnostics.Debug.WriteLine(e.Message);
    logger.Error(e, "--> Global exception.");
}
finally
{
    logger.Debug("--> Shutting down...");
    LogManager.Shutdown();
}

