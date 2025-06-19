using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.Databricks;
using Greenhouse.Utilities;
using Moq;
using Newtonsoft.Json;
using NLog;
using Polly;
using System.ComponentModel;
using System.Dynamic;
using System.Runtime.ExceptionServices;

namespace Greenhouse.DAL.Tests.Unit;

public class DatabricksCallsTests
{
    private readonly Mock<IHttpClientProvider> mockHttpClientProvider = new Mock<IHttpClientProvider>();
    private readonly Mock<Data.Model.Setup.Credential> mockCredential = new Mock<Data.Model.Setup.Credential>();
    private Mock<DataBricksRequest> mockDataBricksReq;


    public DatabricksCallsTests()
    {
        dynamic credentialSet = new ExpandoObject();
        credentialSet.Endpoint = "some-endpoint";
        credentialSet.AuthToken = "some-token";

        mockCredential.Object.CredentialSet = credentialSet;
    }

    [Fact]
    public void TestGetErrorMessagesForFailedTasksAsyncWithMultipleJobTasks()
    {
        string errorMsg = "SOME ERROR";
        Response response = new Response { Error = errorMsg };

        mockHttpClientProvider
            .Setup(x => x.SendRequestAsync(It.IsAny<Utilities.HttpRequestOptions>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(JsonConvert.SerializeObject(response)));
        mockDataBricksReq = new Mock<DataBricksRequest>(mockHttpClientProvider.Object, mockCredential.Object, HttpMethod.Get);

        DatabricksCalls dc = new DatabricksCalls(mockCredential.Object, 10, mockHttpClientProvider.Object);

        var jobId = "172378378237832";
        var jobTasks = new List<Databricks.RunListResponse.JobTask> {
            new Databricks.RunListResponse.JobTask{ RunId = 1000, RunPageUrl= "some-url-1"},
            new Databricks.RunListResponse.JobTask{ RunId = 1000, RunPageUrl= "some-url-2"},
        };

        string[] messages = new string[2]
        {
            $"Databricks Job Run Failed: Job ID: {jobId}, Run ID: {1000}, Error Message: {errorMsg}, Run Page URL: {"some-url-1"}",
            $"Databricks Job Run Failed: Job ID: {jobId}, Run ID: {1000}, Error Message: {errorMsg}, Run Page URL: {"some-url-2"}"
        };
        string expected = string.Join(System.Environment.NewLine, messages);
        string actual = DatabricksCalls.GetErrorMessagesForFailedTasksAsync(
            jobTasks,
            mockDataBricksReq.Object,
            "172378378237832").GetAwaiter().GetResult();

        Assert.Equal(expected, actual);
    }

    /// <summary>
    ///  THESE UNIT TESTS ARE COMMENTED DUE TO THE 'LOGGER' DEPENDENCY INSIDE DatabricksCalls CLASS
    /// </summary>
    /// <exception cref="DatabricksResultNotSuccessfulException"></exception>
    //[Fact]
    //public void TestGetErrorMessagesForFailedTasksAsyncWhenThrowsException()
    //{
    //    mockHttpClientProvider
    //        .Setup(x => x.SendRequestAsync(It.IsAny<Utilities.HttpRequestOptions>(), It.IsAny<CancellationToken>()))
    //        .Throws(new HttpClientProviderRequestException(new HttpResponseMessage()));

    //    mockDataBricksReq = new Mock<DataBricksRequest>(mockHttpClientProvider.Object, mockCredential.Object, HttpMethod.Get);
    //    DatabricksCalls dc = new DatabricksCalls(mockCredential.Object, 10, mockHttpClientProvider.Object);

    //    var jobTasks = new List<Databricks.RunListResponse.JobTask> {
    //        new Databricks.RunListResponse.JobTask{ RunId = 1000, RunPageUrl= "some-url-1"},
    //        new Databricks.RunListResponse.JobTask{ RunId = 1000, RunPageUrl= "some-url-2"},
    //    };

    //    Assert.Throws<HttpClientProviderRequestException>(()=> DatabricksCalls.GetErrorMessagesForFailedTasksAsync(
    //        jobTasks,
    //        mockDataBricksReq.Object,
    //        "172378378237832").GetAwaiter().GetResult());
    //}

    //[Fact]
    //public void TestGetErrorMessagesForFailedTasksAsyncWhenSendRequestReturnsMultipleResults()
    //{
    //    Response response = new Response { Error = "SOME ERROR" };
    //    mockHttpClientProvider
    //        .SetupSequence(x => x.SendRequestAsync(It.IsAny<Utilities.HttpRequestOptions>(), It.IsAny<CancellationToken>()))
    //        .Throws(new HttpClientProviderRequestException(new HttpResponseMessage()))
    //        .Returns(Task.FromResult(JsonConvert.SerializeObject(response)));

    //    mockDataBricksReq = new Mock<DataBricksRequest>(mockHttpClientProvider.Object, mockCredential.Object, HttpMethod.Get);
    //    DatabricksCalls dc = new DatabricksCalls(mockCredential.Object, 10, mockHttpClientProvider.Object);

    //    var jobTasks = new List<Databricks.RunListResponse.JobTask> {
    //        new Databricks.RunListResponse.JobTask{ RunId = 1000, RunPageUrl= "some-url-1"},
    //        new Databricks.RunListResponse.JobTask{ RunId = 1000, RunPageUrl= "some-url-2"},
    //    };

    //    string expected = "";
    //    //string actual = dc.GetErrorMessagesForFailedTasks(
    //    //    jobTasks,
    //    //    mockDataBricksReq.Object,
    //    //    "172378378237832").GetAwaiter().GetResult();

    //    Assert.Throws<HttpClientProviderRequestException>(() => DatabricksCalls.GetErrorMessagesForFailedTasksAsync(
    //        jobTasks,
    //        mockDataBricksReq.Object,
    //        "172378378237832").GetAwaiter().GetResult());
    //}

    [Fact]
    [Description("")]
    public void TestOfOutcomeObjectInfoBehaviour()
    {
        #region POLLY SOURCE CODE
        /* POLLY SOURCE CODE
         var outcome = await Component.ExecuteCore(
        static async (context, state) =>
        {
            try
            {
                await state.callback(context, state.state).ConfigureAwait(context.ContinueOnCapturedContext);
                return Outcome.Void;
            }
            catch (Exception e)
            {
                return Outcome.FromException(e);
            }
        },
        context,
        (callback, state)).ConfigureAwait(context.ContinueOnCapturedContext);

    outcome.GetResultOrRethrow();
         
         */
        #endregion

        Outcome<VoidResult> outcome;
        string errorMessages = "TRY to preserve databricks message";
        try
        {
            throw new DatabricksResultNotSuccessfulException(errorMessages);
        }
        catch (Exception e)
        {
            outcome = Outcome.FromException<VoidResult>(e);
        }

        Assert.Equal(errorMessages, outcome.Exception.Message);
    }

    [Fact]
    [Description("")]
    public void TestOfExceptionDispatchInfoBehaviour()
    {
        #region POLLY SOURCE CODE
        /* POLLY SOURCE CODE
         var outcome = await Component.ExecuteCore(
        static async (context, state) =>
        {
            try
            {
                await state.callback(context, state.state).ConfigureAwait(context.ContinueOnCapturedContext);
                return Outcome.Void;
            }
            catch (Exception e)
            {
                return Outcome.FromException(e);
            }
        },
        context,
        (callback, state)).ConfigureAwait(context.ContinueOnCapturedContext);

    outcome.GetResultOrRethrow();
         
         */
        #endregion

        ExceptionDispatchInfo dispInfo;
        string errorMessages = "TRY to preserve databricks message";
        try
        {
            throw new DatabricksResultNotSuccessfulException(errorMessages);
        }
        catch (Exception e)
        {
            dispInfo = ExceptionDispatchInfo.Capture(e);
        }

        DatabricksResultNotSuccessfulException ex = Assert.Throws<DatabricksResultNotSuccessfulException>(() => dispInfo.Throw());

        Assert.Equal(errorMessages, ex.Message);
    }
}

public class VoidResult
{
    private VoidResult()
    {
    }

    public static readonly VoidResult Instance = new();

    public override string ToString() => "void";
}
