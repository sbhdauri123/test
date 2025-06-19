using Greenhouse.Auth;
using Greenhouse.Common.Infrastructure;
using Greenhouse.DAL.SOAP;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.DAL.BingAds.Reporting;

public class Reports
{
    private readonly ITokenApiClient _tokenApiClient;
    private readonly Action<string> logError;
    public Greenhouse.Data.Model.Setup.Credential BingCredential { get; set; }
    private readonly IReportingService proxy;
    private readonly CustomInspectorBehavior SOAPInspector;
    public string DevToken { get; set; }

    public Reports(Credential creds, ITokenApiClient tokenApiClient, Action<string> logError)
    {
        this.BingCredential = creds;
        this.DevToken = BingCredential?.CredentialSet?.DevToken;
        _tokenApiClient = tokenApiClient;
        this.logError = logError;
        this.SOAPInspector = new CustomInspectorBehavior();
        proxy = SOAP.Proxy.CreateWebServiceChannelProxy<IReportingService>(BingCredential, SOAP.Proxy.GetDefaultBinding(), this.SOAPInspector);
    }

    private static T Initialize<T>(string reportName) where T : ReportRequest, new()
    {
        T reportRequest = new T()
        {
            ReportName = reportName,
            Format = ReportFormat.Csv,
            ExcludeReportHeader = true,
            ExcludeReportFooter = true,
            ExcludeColumnHeaders = false,
            ReturnOnlyCompleteData = true,
        };

        return (T)reportRequest;
    }

    public PollGenerateReportResponse GetReportStatus(string reportID)
    {
        PollGenerateReportResponse response = null;

        string accessToken = _tokenApiClient.GetAccessTokenAsync(TokenDataSource.Bing).GetAwaiter().GetResult();

        PollGenerateReportRequest pollRequest = new()
        {
            ReportRequestId = reportID,
            AuthenticationToken = accessToken,
            DeveloperToken = DevToken,
            UserName = BingCredential.CredentialSet.UserName,
            ApplicationToken = BingCredential.CredentialSet.ClientID,
        };

        try
        {
            response = proxy.PollGenerateReport(pollRequest);
        }
        catch
        {
            // logging the soap messages in 2 separate splunk event to avoid truncating them
            logError($"BingAds SOAP ERROR - Request: {this.SOAPInspector.Request}");
            logError($"BingAds SOAP ERROR - Reply: {this.SOAPInspector.Reply}");

            throw;
        }

        return response;
    }
    /// <summary>
    /// Helper to return SubmitGenerateReportRequest.
    /// </summary>
    /// <param name="req">ReportRequest type</param>
    /// <param name="customerAccountID"></param>
    /// <returns></returns>
    private SubmitGenerateReportRequest GetRequest(ReportRequest req, string customerAccountID)
    {
        string accessToken = _tokenApiClient.GetAccessTokenAsync(TokenDataSource.Bing).GetAwaiter().GetResult();

        return new SubmitGenerateReportRequest()
        {
            DeveloperToken = DevToken,
            AuthenticationToken = accessToken,
            UserName = BingCredential.CredentialSet.UserName,
            ApplicationToken = BingCredential.CredentialSet.ClientID,
            CustomerId = customerAccountID,
            ReportRequest = req
        };
    }

    /// <summary>
    /// Generic function to submit BingAds report.
    /// </summary>
    /// <typeparam name="R">Report Request Type</typeparam>
    /// <typeparam name="T">Report Column Type</typeparam>
    /// <param name="customerAccountID"></param>
    /// <param name="data"></param>
    /// <param name="dateRange"></param>
    public SubmitGenerateReportResponse CreateReport<R, T>(string customerAccountID, string reportName, IEnumerable<T> data, ReportTime dateRange, IRetry retryPolicy) where R : ReportRequest, new()
    {
        SubmitGenerateReportResponse response = null;
        try
        {
            dynamic reportReq = Initialize<R>(reportName);
            var objScope = typeof(R).GetProperties().FirstOrDefault(p => p.Name.Equals("scope", StringComparison.InvariantCultureIgnoreCase));
            dynamic scope = Activator.CreateInstance(objScope.PropertyType);
            scope.AccountIds = new long[] { long.Parse(customerAccountID) };
            reportReq.Columns = data.ToArray();
            reportReq.Time = dateRange;
            reportReq.Scope = scope;

            if (data.Any(c => c.ToString().Equals("TimePeriod")))
            {
                reportReq.Aggregation = ReportAggregation.Daily;
            }

            var reportGenerator = GetRequest(reportReq, customerAccountID);

            if (retryPolicy != null)
            {
                response = retryPolicy.Execute<SubmitGenerateReportResponse>(() =>
                {
                    try
                    {
                        return proxy.SubmitGenerateReport(reportGenerator);
                    }
                    catch
                    {
                        // logging the soap messages in 2 separate splunk event to avoid truncating them
                        logError($"BingAds SOAP ERROR - Request: {this.SOAPInspector.Request}");
                        logError($"BingAds SOAP ERROR - Reply: {this.SOAPInspector.Reply}");

                        throw;
                    }
                });
            }
            else
            {
                try
                {
                    response = proxy.SubmitGenerateReport(reportGenerator);
                }
                catch
                {
                    // logging the soap messages in 2 separate splunk event to avoid truncating them
                    logError($"BingAds SOAP ERROR - Request: {this.SOAPInspector.Request}");
                    logError($"BingAds SOAP ERROR - Reply: {this.SOAPInspector.Reply}");

                    throw;
                }
            }
        }
        catch (System.ServiceModel.FaultException<ApiFaultDetail> apiException)
        {
            string trackingId = apiException.Detail.TrackingId;
            var errDetails = string.Join("==>", apiException.Detail.OperationErrors?.Select(e => e.Message));
            return HandleException($"Bing Ads API FaultException: TrackingId: {trackingId} -> Report Type: {reportName} has following errors: {errDetails}");
        }
        catch (HttpClientProviderRequestException exc)
        {
            return HandleException($"Bing Ads API Exception: Report Type: {reportName} | Exception details : {exc}");
        }
        catch (Exception exc)
        {
            return HandleException($"Bing Ads API Exception: Report Type: {reportName} has following errors: {exc.Message}");
        }

        return response;
    }

    private SubmitGenerateReportResponse HandleException(string message)
    {
        logError(message);
        return null;
    }
}
