using Greenhouse.Common.Exceptions;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceProcess;
using System.Text;

namespace Greenhouse.DeploymentService.Controllers
{
    [ApiController]
    //[Produces("application/xml")]
    [Route("greenhousedeploymentservice")]
    public class VivakiRemoteServiceController : BaseController
    {
        //private static string domain = System.Configuration.ConfigurationManager.AppSettings["DOMAIN"];
        //private static string user = System.Configuration.ConfigurationManager.AppSettings["DOMAIN_USER"];
        //private static string password = System.Configuration.ConfigurationManager.AppSettings["DOMAIN_PASSWORD"];
        private enum ServerType
        {
            SERVICE = 1,
            WEBSERVER = 2,
            BOTH = 3
        }
        private const string baseServiceName = "GreenhouseWinService";
        private readonly string _machineName = System.Environment.MachineName.ToString();

        public VivakiRemoteServiceController(NLog.ILogger logger) : base(logger)
        {
            //if(string.IsNullOrEmpty(domain) || string.IsNullOrEmpty(user) || string.IsNullOrEmpty(password)) {
            //    throw new ArgumentException("Domain, user and password must be specified to impersonate remote user");
            //}

            //LogMessage(string.Format("Attempting impersonation of remote user {0}",user), LogLevel.Info);

            //if (!ImpersonationManager.Impersonate(user, password, domain)) {
            //    string errmsg = string.Format("Impersonation failed for user {0}, check your config settings.");
            //    LogMessage(errmsg, LogLevel.Fatal);
            //    throw new ArgumentException(errmsg);
            //}
            //LogMessage(string.Format("Impersonation successful for user {0}", user), LogLevel.Info);
        }

        #region Helper Methods

        [ApiExplorerSettings(IgnoreApi = true)]
        public string ConvertHexToString(string hex)
        {
            string str = "";
            while (hex.Length > 0)
            {
                str += System.Convert.ToChar(System.Convert.ToUInt32(hex.Substring(0, 2), 16)).ToString();
                hex = hex.Substring(2, hex.Length - 2);
            }
            return str;
        }

        //private void LogMessage(string message, NLog.LogLevel level, Exception exc = null) {
        //    try {
        //        if (exc != null) {
        //            EventLog.WriteEntry("Application", exc.ToString(), EventLogEntryType.Error);
        //        }
        //        message = String.Format("{0}: {1}", this.GetType().Name, message);
        //        LogEventInfo lei = Greenhouse.Logging.Msg.Create(level, logger.Name, message, exc);
        //        logger.Log(lei);
        //    }
        //    catch (Exception exc2) {
        //        EventLog.WriteEntry("Application", exc2.ToString(), EventLogEntryType.Error);
        //    }
        //}

        private static string GetFullServiceName(Server svr)
        {
            //Topshelf naming of services is always BaseServiceName + $ + InstanceName
            return string.Format("{0}${1}", baseServiceName, svr.ServerAlias);
        }

        private static ServiceController LookupService(string machineName, string serviceName)
        {
            var services = new List<ServiceController>(ServiceController.GetServices(machineName));
            return services.SingleOrDefault(s => s.ServiceName == serviceName);
        }
        #endregion

        #region Informational

        [HttpGet]
        [Route("listclusters/{env}")]
        public OkObjectResult ListClusters(string env)
        {
            string jsonResult = string.Empty;
            List<Cluster> clusterList = new List<Cluster>();
            try
            {
                var clusters = Data.Services.SetupService.GetClustersByEnv(env);
                clusterList.AddRange(clusters);
                jsonResult = JsonConvert.SerializeObject(clusters);
            }
            catch (Exception exc)
            {
                string msg = string.Format("Deployment service running on server {0} failed to list clusters. Error was: {1} - {2}", _machineName, exc.Message, exc.InnerException != null ? exc.InnerException.Message : string.Empty);
                LogErrorMsg(exc, msg);
            }

            return Ok(clusterList);

            //return FormatStreamResponse(jsonResult);
        }

        [HttpGet]
        [Route("listservers/{env}/{clusterName}")]
        public Stream ListServers(string env, string clusterName)
        {
            string jsonResult = string.Empty;
            if (clusterName == null)
            {
                throw new ArgumentNullException(nameof(clusterName), "ClusterName cannot be null. Should be the name of the cluster or 'ALL' for everything.");
            }
            try
            {
                List<Server> servers = GetServers(env, (clusterName == "ALL" ? null : clusterName));
                jsonResult = JsonConvert.SerializeObject(servers);
            }
            catch (Exception exc)
            {
                string msg = string.Format("Deployment service running on server {0} failed to list services for region: {1}. Error was: {2} - {3}", _machineName, clusterName, exc.Message, exc.InnerException != null ? exc.InnerException.Message : string.Empty);
                LogErrorMsg(exc, msg);
            }
            return FormatStreamResponse(jsonResult);
        }

        private static List<Server> GetServers(string env, string clusterName)
        {
            var clusters = Data.Services.SetupService.GetClustersByEnv(env);
            Cluster cluster = clusters.SingleOrDefault(c => c.ClusterName == clusterName);

            var servers = cluster == null ?
                Data.Services.SetupService.GetServersByEnv(env).Where(s => s.IsActive)
                : Data.Services.SetupService.GetServersByEnv(env).Where(s => s.IsActive && s.ClusterID == cluster.ClusterID);

            return servers.ToList();
        }

        [HttpGet]
        [Route("statusall/{env}/{clusterName}")]
        public Stream StatusAll(string env, string clusterName)
        {
            string jsonResult = string.Empty;
            ServiceCallResponse response = new ServiceCallResponse();

            List<Server> servers = GetServers(env, (clusterName == "ALL" ? null : clusterName));

            foreach (Server svr in servers.Where(s => s.ServerTypeID != (int)ServerType.WEBSERVER))
            {
                var res = StatusThis(env, svr.ServerName);
                response.ServiceResults.Add(res.ServiceResults[0]);
            }
            jsonResult = JsonConvert.SerializeObject(response);
            return FormatStreamResponse(jsonResult);
        }

        [HttpGet]
        [Route("status/{env}/{serverName}")]
        public Stream Status(string env, string clusterName)
        {
            return FormatStreamResponse(JsonConvert.SerializeObject(StatusThis(env, clusterName)));
        }

        private ServiceCallResponse StatusThis(string env, string serverName)
        {
            ServiceCallResponse response = new ServiceCallResponse();
            try
            {
                Server svr = Data.Services.SetupService.GetServersByEnv(env).SingleOrDefault(s => s.ServerName == serverName);
                string serviceNameLookup = GetFullServiceName(svr);
                response.ServiceResults.Add(new ServiceResult() { Operation = ServiceResult.ServiceOperation.Status, Server = svr, ServiceName = serviceNameLookup });

                LogInfoMsg($"Attempting to retrieve status: {serviceNameLookup} on {svr.ServerName}");
                var sc = LookupService(svr.ServerIP, serviceNameLookup);
                if (sc != null)
                {
                    response.ServiceResults[0].Status = sc.Status;
                    response.ServiceResults[0].Success = true;
                }
                else
                {
                    response.ServiceResults[0].Success = false;
                    response.ServiceResults[0].Error = new LookupException(string.Format("Service: {0} not found on {1}", serviceNameLookup, svr.ServerName));
                }
            }
            catch (Exception exc)
            {
                LogErrorMsg(exc, $"Deployment service running on server {serverName} failed to retreieve status. Error was: {exc.Message}");
                response.ServiceResults[0].Success = false;
                response.ServiceResults[0].Error = exc;
            }
            return response;
        }

        #endregion

        [HttpGet]
        #region Functional
        [Route("startall/{env}/{clusterName}")]
        public Stream StartAll(string env, string clusterName)
        {
            string jsonResult = string.Empty;
            ServiceCallResponse response = new ServiceCallResponse();

            List<Server> servers = GetServers(env, (clusterName == "ALL" ? null : clusterName));

            foreach (Server svr in servers.Where(s => s.ServerTypeID != (int)ServerType.WEBSERVER))
            {
                var res = StartThis(env, svr.ServerName);
                response.ServiceResults.Add(res.ServiceResults[0]);
            }
            jsonResult = JsonConvert.SerializeObject(response);
            return FormatStreamResponse(jsonResult);
        }

        [HttpGet]
        [Route("start/{env}/{serverName}")]
        public Stream Start(string env, string serverName)
        {
            return FormatStreamResponse(JsonConvert.SerializeObject(StartThis(env, serverName)));
        }

        private ServiceCallResponse StartThis(string env, string serverName)
        {
            ServiceCallResponse response = new ServiceCallResponse();
            try
            {
                Server svr = Data.Services.SetupService.GetServersByEnv(env).SingleOrDefault(s => s.ServerName == serverName);
                string serviceNameLookup = GetFullServiceName(svr);
                response.ServiceResults.Add(new ServiceResult() { Operation = ServiceResult.ServiceOperation.Start, Server = svr, ServiceName = serviceNameLookup });

                LogInfoMsg($"Attempting to start service: {serviceNameLookup} on {svr.ServerName}...");
                var sc = new System.ServiceProcess.ServiceController(serviceNameLookup, svr.ServerName);
                LogInfoMsg($"service controller for: {serviceNameLookup} is: {sc?.Status}");

                string msg = string.Empty;
                switch (sc.Status)
                {
                    case ServiceControllerStatus.Stopped:
                        sc.Start();
                        sc.WaitForStatus(System.ServiceProcess.ServiceControllerStatus.Running);
                        response.ServiceResults[0].Status = ServiceControllerStatus.Running;
                        response.ServiceResults[0].Success = true;
                        msg = string.Format("{0} transitioned from Stopped to Running", serviceNameLookup);
                        response.ServiceResults[0].AdditionalInfo = msg;
                        LogInfoMsg(msg);
                        break;
                    case ServiceControllerStatus.Running:
                        response.ServiceResults[0].Status = ServiceControllerStatus.Running;
                        response.ServiceResults[0].Success = true;
                        msg = string.Format("{0} was already running y'know, forgetaboutit", serviceNameLookup);
                        response.ServiceResults[0].AdditionalInfo = msg;
                        LogInfoMsg(msg);
                        break;
                    default:
                        response.ServiceResults[0].Success = false;
                        msg = string.Format("{0} was in an invalid state: {1} - could not proceed", serviceNameLookup, sc.Status);
                        response.ServiceResults[0].AdditionalInfo = msg;
                        LogInfoMsg(msg);
                        break;
                }
            }
            catch (Exception exc)
            {
                string msg = string.Format("Deployment service running on server {0} failed to start. Error was: {1}", serverName, exc.Message);
                LogErrorMsg(exc, msg);
                response.ServiceResults[0].Status = ServiceControllerStatus.Stopped;
                response.ServiceResults[0].Error = exc;
                response.ServiceResults[0].Success = false;
            }

            return response;
        }

        [HttpGet]
        [Route("stopall/{env}/{clusterName}")]
        public OkObjectResult StopAll(string env, string clusterName)
        {
            string jsonResult = string.Empty;
            ServiceCallResponse response = new ServiceCallResponse();

            List<Server> servers = GetServers(env, (clusterName == "ALL" ? null : clusterName));

            foreach (Server svr in servers.Where(s => s.ServerTypeID != (int)ServerType.WEBSERVER))
            {
                var res = StopThis(env, svr.ServerName);
                response.ServiceResults.Add(res.ServiceResults[0]);
            }

            return Ok(response);

            //jsonResult = JsonConvert.SerializeObject(response);
            //return FormatStreamResponse(jsonResult);
        }

        [HttpGet]
        [Route("stop/{env}/{serverName}")]
        public Stream Stop(string env, string serverName)
        {
            return FormatStreamResponse(JsonConvert.SerializeObject(StopThis(env, serverName)));
        }

        private ServiceCallResponse StopThis(string env, string serverName)
        {
            ServiceCallResponse response = new ServiceCallResponse();
            try
            {
                Server svr = Data.Services.SetupService.GetServersByEnv(env).SingleOrDefault(s => s.ServerName == serverName);
                string serviceNameLookup = GetFullServiceName(svr);
                response.ServiceResults.Add(new ServiceResult() { Operation = ServiceResult.ServiceOperation.Stop, Server = svr, ServiceName = serviceNameLookup });

                LogInfoMsg($"Attempting to start service: {serviceNameLookup} on {svr.ServerName}...");
                var sc = new System.ServiceProcess.ServiceController(serviceNameLookup, svr.ServerName);
                LogInfoMsg($"service controller for: {serviceNameLookup} is: {sc?.Status}");

                string msg = string.Empty;

                switch (sc.Status)
                {
                    case ServiceControllerStatus.Running:
                        sc.Stop();
                        sc.WaitForStatus(System.ServiceProcess.ServiceControllerStatus.Stopped);
                        response.ServiceResults[0].Status = ServiceControllerStatus.Stopped;
                        response.ServiceResults[0].Success = true;
                        msg = string.Format("{0} transitioned from Running to Stopped", serviceNameLookup);
                        response.ServiceResults[0].AdditionalInfo = msg;
                        LogInfoMsg(msg);

                        break;
                    case ServiceControllerStatus.Stopped:
                        response.ServiceResults[0].Status = ServiceControllerStatus.Stopped;
                        response.ServiceResults[0].Success = true;
                        msg = string.Format("{0} was already stopped y'know, forgetaboutit", serviceNameLookup);
                        response.ServiceResults[0].AdditionalInfo = msg;
                        LogInfoMsg(msg);
                        break;
                    default:
                        response.ServiceResults[0].Success = false;
                        msg = string.Format("{0} was in an invalid state: {1} - could not proceed", serviceNameLookup, sc.Status);
                        response.ServiceResults[0].AdditionalInfo = msg;
                        LogInfoMsg(msg);
                        break;
                }
            }
            catch (Exception exc)
            {
                string msg = string.Format("Deployment service running on server {0} failed to stop. Error was: {1}", serverName, exc.Message);
                LogErrorMsg(exc, msg);
                response.ServiceResults[0].Status = ServiceControllerStatus.Running;
                response.ServiceResults[0].Error = exc;
                response.ServiceResults[0].Success = false;
            }
            return response;
        }

        [HttpGet]
        [Route("deployall/{env}/{clusterName}/{buildDirectory}")]
        public Stream DeployAll(string env, string clusterName, string buildDirectory)
        {
            string jsonResult = string.Empty;
            ServiceCallResponse response = new ServiceCallResponse();

            List<Server> servers = GetServers(env, (clusterName == "ALL" ? null : clusterName));

            foreach (Server svr in servers.Where(s => s.ServerTypeID != (int)ServerType.WEBSERVER))
            {
                var res = DeployThis(env, svr.ServerName, buildDirectory);
                response.ServiceResults.Add(res.ServiceResults[0]);
            }
            jsonResult = JsonConvert.SerializeObject(response);
            return FormatStreamResponse(jsonResult);
        }

        [HttpGet]
        [Route("deploy/{env}/{serverName}/{buildDirectory}")]
        public Stream Deploy(string env, string serverName, string buildDirectory)
        {
            return FormatStreamResponse(JsonConvert.SerializeObject(DeployThis(env, serverName, buildDirectory)));
        }

        private ServiceCallResponse DeployThis(string env, string serverName, string buildDirectory)
        {
            buildDirectory = ConvertHexToString(buildDirectory);

            ServiceCallResponse response = new ServiceCallResponse();
            FileSystemDirectory localDir = null;
            FileSystemDirectory remoteDir = null;
            try
            {
                Server svr = Data.Services.SetupService.GetServersByEnv(env).SingleOrDefault(s => s.ServerName == serverName);
                response.ServiceResults.Add(new ServiceResult() { Operation = ServiceResult.ServiceOperation.Deploy, Server = svr, ServiceName = this.GetType().Name });

                Uri localUri = new Uri(buildDirectory);
                Uri remoteUri = new Uri(string.Format(@"\\{0}\c$\GreenhouseService\", svr.ServerIP));

                localDir = new FileSystemDirectory(localUri);
                remoteDir = new FileSystemDirectory(remoteUri);

                LogInfoMsg($"Attempting to start service: {localDir} on {remoteDir}...");

                CopyDirectoryStructure(localDir, remoteDir);
                response.ServiceResults[0].Status = ServiceControllerStatus.Running;
                response.ServiceResults[0].Success = true;
                LogInfoMsg($"Attempting to start service: {localDir} on {remoteDir}...");
            }
            catch (Exception exc)
            {
                response.ServiceResults[0].Status = ServiceControllerStatus.Stopped;
                response.ServiceResults[0].Error = exc;
                response.ServiceResults[0].Success = false;
                LogErrorMsg(exc, $"Attempting to start service: {localDir} on {remoteDir}...");
            }
            return response;
        }

        private static void DeleteAll(IDirectory rootRemoteDirectory)
        {
            IEnumerable<IDirectory> dirs = null;
            IEnumerable<IFile> files = null;

            files = rootRemoteDirectory.GetFiles();
            foreach (IFile file in files)
            {
                file.Delete();
            }
            dirs = rootRemoteDirectory.GetDirectories();

            foreach (IDirectory dir in dirs)
            {
                DeleteAll(dir);
            }
        }

        private static void CopyDirectoryStructure(IDirectory rootLocalDirectory, IDirectory rootRemoteDirectory)
        {
            IEnumerable<IDirectory> dirs = null;
            IEnumerable<IFile> files = null;

            //DeleteAll(rootRemoteDirectory);

            files = rootLocalDirectory.GetFiles();
            foreach (IFile file in files)
            {
                IFile destFile = new FileSystemFile(RemoteUri.CombineUri(rootRemoteDirectory.Uri, file.Name));
                file.CopyTo(destFile, true);
            }

            dirs = rootLocalDirectory.GetDirectories();

            foreach (IDirectory dir in dirs)
            {
                FileSystemDirectory remoteDir = new FileSystemDirectory(RemoteUri.CombineUri(rootRemoteDirectory.Uri, dir.Name));
                remoteDir.Create();
                CopyDirectoryStructure(dir, remoteDir);
            }
        }

        #endregion

        [HttpGet]
        [Route("hello")]
        public string Hello()
        {
            return string.Format("Hello from GreenhouseDeploymentService");
        }

        private static MemoryStream FormatStreamResponse(string json)
        {
            byte[] resultBytes = Encoding.UTF8.GetBytes(json);
            //WebOperationContext.Current.OutgoingResponse.ContentType = "application/x-json-stream";
            var messageProperty = new HttpResponseMessageProperty();
            OperationContext.Current.OutgoingMessageProperties[HttpResponseMessageProperty.Name] = messageProperty;
            messageProperty.Headers["Content-Type"] = "application/x-json-stream";
            return new MemoryStream(resultBytes);
        }

        //public string EchoDBServer(string env) {
        //    string key = string.Format("{0}_GREENHOUSE", env);
        //    string connStr = System.Configuration.ConfigurationManager.ConnectionStrings[key].ConnectionString;
        //    return connStr;
        //}
    }
}
