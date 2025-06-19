using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;
using System.ServiceModel.Web;
using System.ServiceProcess;
using NLog;
using Newtonsoft.Json;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;

namespace Greenhouse.Deployment.WCF {
    public class VivakiRemoteServiceController : IRemoteServiceController{
        private static string domain = System.Configuration.ConfigurationManager.AppSettings["DOMAIN"];
        private static string user = System.Configuration.ConfigurationManager.AppSettings["DOMAIN_USER"];
        private static string password = System.Configuration.ConfigurationManager.AppSettings["DOMAIN_PASSWORD"];

        public VivakiRemoteServiceController() {

            if(string.IsNullOrEmpty(domain) || string.IsNullOrEmpty(user) || string.IsNullOrEmpty(password)) {
                throw new ArgumentException("Domain, user and password must be specified to impersonate remote user");
            }

            LogMessage(string.Format("Attempting impersonation of remote user {0}",user), LogLevel.Info);

            if (!ImpersonationManager.Impersonate(user, password, domain)) {
                string errmsg = string.Format("Impersonation failed for user {0}, check your config settings.");
                LogMessage(errmsg, LogLevel.Fatal);
                throw new ArgumentException(errmsg);
            }
            LogMessage(string.Format("Impersonation successful for user {0}", user), LogLevel.Info);
        }

        private enum ServerType {
            SERVICE =1,
            WEBSERVER =2,
            BOTH=3
        }
        private const string baseServiceName = "GreenhouseWinService";
        private string _machineName = System.Environment.MachineName.ToString();
        private static Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private Greenhouse.Data.Services.SetupService setupServ = new Data.Services.SetupService();


        #region Helper Methods

      
        public string ConvertHexToString(string hex) {
            string str = "";
            while (hex.Length > 0) {
                str += System.Convert.ToChar(System.Convert.ToUInt32(hex.Substring(0, 2), 16)).ToString();
                hex = hex.Substring(2, hex.Length - 2);
            }
            return str;
        }

        private void LogMessage(string message, NLog.LogLevel level, Exception exc = null) {
            try {
                if (exc != null) {
                    EventLog.WriteEntry("Application", exc.ToString(), EventLogEntryType.Error);
                }
                message = String.Format("{0}: {1}", this.GetType().Name, message);
                LogEventInfo lei = Greenhouse.Logging.Msg.Create(level, logger.Name, message, exc);
                logger.Log(lei);
            }
            catch (Exception exc2) {
                EventLog.WriteEntry("Application", exc2.ToString(), EventLogEntryType.Error);
            }
        }

        private string GetFullServiceName(Server svr) {
            //Topshelf naming of services is always BaseServiceName + $ + InstanceName
            return string.Format("{0}${1}", baseServiceName, svr.ServerAlias);
        }

        private ServiceController LookupService(string machineName, string serviceName) {
            var services = new List<ServiceController>(ServiceController.GetServices(machineName));
            return services.SingleOrDefault(s => s.ServiceName == serviceName);
        }
        #endregion

        #region Informational

        public Stream ListClusters(string env) {
            string jsonResult = string.Empty;
            List<string> clusterList = new List<string>();
            try {
                var clusters = setupServ.GetClustersByEnv(env);
                jsonResult = JsonConvert.SerializeObject(clusters);
            }
            catch (Exception exc) {
                string msg = string.Format("Deployment service running on server {0} failed to list clusters. Error was: {1} - {2}", _machineName, exc.Message, exc.InnerException != null ? exc.InnerException.Message : string.Empty);
                LogMessage(msg, LogLevel.Error, exc);
            }
            return FormatStreamResponse(jsonResult);
        }

        public Stream ListServers(string env, string clusterName) {
            string jsonResult = string.Empty;
            if (clusterName == null) {
                throw new ArgumentNullException("ClusterName cannot be null. Should be the name of the cluster or 'ALL' for everything.");
            }
            try {
                List<Server> servers = GetServers(env, (clusterName == "ALL" ? null : clusterName));
                jsonResult = JsonConvert.SerializeObject(servers);
            }
            catch (Exception exc) {
                string msg = string.Format("Deployment service running on server {0} failed to list services for region: {1}. Error was: {2} - {3}", _machineName, clusterName, exc.Message, exc.InnerException != null ? exc.InnerException.Message : string.Empty);
                LogMessage(msg, LogLevel.Error, exc);
            }
            return FormatStreamResponse(jsonResult);
        }

        private List<Server> GetServers(string env, string clusterName) {

            var clusters = setupServ.GetClustersByEnv(env);
            Cluster cluster = clusters.SingleOrDefault(c => c.ClusterName == clusterName);
            
            var servers = cluster == null ?
                setupServ.GetServersByEnv(env).Where(s => s.IsActive)
                : setupServ.GetServersByEnv(env).Where(s => s.IsActive && s.ClusterID == cluster.ClusterID);

            return servers.ToList();
        }

        public Stream StatusAll(string env, string clusterName) {
            string jsonResult = string.Empty;
            ServiceCallResponse response = new ServiceCallResponse();

            List<Server> servers = GetServers(env, (clusterName == "ALL" ? null : clusterName));

            foreach (Server svr in servers.Where(s => s.ServerTypeID != (int)ServerType.WEBSERVER)) {
                var res = StatusThis(env, svr.ServerName);
                response.ServiceResults.Add(res.ServiceResults[0]);
            }
            jsonResult = JsonConvert.SerializeObject(response);
            return FormatStreamResponse(jsonResult);
        }

        public Stream Status(string env, string clusterName) {
            return FormatStreamResponse(JsonConvert.SerializeObject(StatusThis(env, clusterName)));
        }

        private ServiceCallResponse StatusThis(string env, string serverName) {
            ServiceCallResponse response = new ServiceCallResponse();
            try {
                Server svr = setupServ.GetServersByEnv(env).SingleOrDefault(s => s.ServerName == serverName);
                string serviceNameLookup = GetFullServiceName(svr);
                response.ServiceResults.Add(new ServiceResult() { Operation = ServiceResult.ServiceOperation.Status, Server = svr, ServiceName = serviceNameLookup });

                LogMessage(string.Format("Attempting to retrieve status: {0} on {1}", serviceNameLookup, svr.ServerName), LogLevel.Info);
                var sc = LookupService(svr.ServerIP, serviceNameLookup);
                if (sc != null) {
                    response.ServiceResults[0].Status = sc.Status;
                    response.ServiceResults[0].Success = true;
                }
                else {
                    response.ServiceResults[0].Success = false;
                    response.ServiceResults[0].Error = new Exception(string.Format("Service: {0} not found on {1}", serviceNameLookup, svr.ServerName));
                }
            }
            catch (Exception exc) {
                StringBuilder msg = new StringBuilder(string.Format("Deployment service running on server {0} failed to retreieve status. Error was: {1}", serverName, exc.Message));
                LogMessage(msg.ToString(), LogLevel.Error, exc);                
                response.ServiceResults[0].Success = false;
                response.ServiceResults[0].Error = exc;
            }
            return response;
        }

        #endregion


        #region Functional
        public Stream StartAll(string env, string clusterName) {
            string jsonResult = string.Empty;
            ServiceCallResponse response = new ServiceCallResponse();

            List<Server> servers = GetServers(env, (clusterName == "ALL" ? null : clusterName));

            foreach (Server svr in servers.Where(s => s.ServerTypeID != (int)ServerType.WEBSERVER)) {
                var res = StartThis(env, svr.ServerName);
                response.ServiceResults.Add(res.ServiceResults[0]);
            }
            jsonResult = JsonConvert.SerializeObject(response);
            return FormatStreamResponse(jsonResult);
        }

        public Stream Start(string env, string serverName) {
            return FormatStreamResponse(JsonConvert.SerializeObject(StartThis(env, serverName)));
        }


        private ServiceCallResponse StartThis(string env, string serverName) {
            ServiceCallResponse response = new ServiceCallResponse();
            try {
                Server svr = setupServ.GetServersByEnv(env).SingleOrDefault(s => s.ServerName == serverName);
                string serviceNameLookup = GetFullServiceName(svr);
                response.ServiceResults.Add(new ServiceResult() { Operation = ServiceResult.ServiceOperation.Start, Server = svr, ServiceName = serviceNameLookup });

                LogMessage(string.Format("Attempting to start service: {0} on {1}...", serviceNameLookup, svr.ServerName), LogLevel.Info);
                var sc = new System.ServiceProcess.ServiceController(serviceNameLookup, svr.ServerName);
                LogMessage(string.Format("service controller for: {0} is: {1}", serviceNameLookup, sc == null ? "null" : sc.Status.ToString()), LogLevel.Info);
               

                string msg = string.Empty;
                switch (sc.Status) {
                    case ServiceControllerStatus.Stopped:
                        sc.Start();
                        sc.WaitForStatus(System.ServiceProcess.ServiceControllerStatus.Running);
                        response.ServiceResults[0].Status = ServiceControllerStatus.Running;
                        response.ServiceResults[0].Success = true;
                        msg = string.Format("{0} transitioned from Stopped to Running", serviceNameLookup);
                        response.ServiceResults[0].AdditionalInfo = msg;
                        LogMessage(msg, LogLevel.Info);
                        break;
                    case ServiceControllerStatus.Running:
                        response.ServiceResults[0].Status = ServiceControllerStatus.Running;
                        response.ServiceResults[0].Success = true;
                        msg = string.Format("{0} was already running y'know, forgetaboutit", serviceNameLookup);
                        response.ServiceResults[0].AdditionalInfo = msg;
                        LogMessage(msg, LogLevel.Info);
                        break;
                    default:
                        response.ServiceResults[0].Success = false;
                        msg = string.Format("{0} was in an invalid state: {1} - could not proceed", serviceNameLookup, sc.Status);
                        response.ServiceResults[0].AdditionalInfo = msg;
                        LogMessage(msg, LogLevel.Info);
                        break;
                }


            }
            catch (Exception exc) {
                string msg = string.Format("Deployment service running on server {0} failed to start. Error was: {1}", serverName, exc.Message);
                LogMessage(msg, LogLevel.Error, exc);
                response.ServiceResults[0].Status = ServiceControllerStatus.Stopped;
                response.ServiceResults[0].Error = exc;
                response.ServiceResults[0].Success = false;
            }
            
            return response;
        }

        public Stream StopAll(string env, string clusterName) {
            string jsonResult = string.Empty;
            ServiceCallResponse response = new ServiceCallResponse();

            List<Server> servers = GetServers(env, (clusterName == "ALL" ? null : clusterName));

            foreach (Server svr in servers.Where(s => s.ServerTypeID != (int)ServerType.WEBSERVER)) {
                var res = StopThis(env, svr.ServerName);
                response.ServiceResults.Add(res.ServiceResults[0]);
            }
            jsonResult = JsonConvert.SerializeObject(response);
            return FormatStreamResponse(jsonResult);
        }

        public Stream Stop(string env, string serverName) {
            return FormatStreamResponse(JsonConvert.SerializeObject(StopThis(env, serverName)));
        }


        private ServiceCallResponse StopThis(string env, string serverName) {
            ServiceCallResponse response = new ServiceCallResponse();
            try {
                Server svr = setupServ.GetServersByEnv(env).SingleOrDefault(s => s.ServerName == serverName);
                string serviceNameLookup = GetFullServiceName(svr);
                response.ServiceResults.Add(new ServiceResult() { Operation = ServiceResult.ServiceOperation.Stop, Server = svr, ServiceName = serviceNameLookup });

                LogMessage(string.Format("Attempting to stop service: {0} on {1}...", serviceNameLookup, svr.ServerName), LogLevel.Info);
                var sc = new System.ServiceProcess.ServiceController(serviceNameLookup, svr.ServerName);
                LogMessage(string.Format("service controller for: {0} is: {1}", serviceNameLookup, sc == null ? "null" : sc.Status.ToString()), LogLevel.Info);

                string msg = string.Empty;

                switch (sc.Status) {
                    case ServiceControllerStatus.Running:
                        sc.Stop();
                        sc.WaitForStatus(System.ServiceProcess.ServiceControllerStatus.Stopped);
                        response.ServiceResults[0].Status = ServiceControllerStatus.Stopped;
                        response.ServiceResults[0].Success = true;
                        msg = string.Format("{0} transitioned from Running to Stopped", serviceNameLookup);
                        response.ServiceResults[0].AdditionalInfo = msg;
                        LogMessage(msg, LogLevel.Info);

                        break;
                    case ServiceControllerStatus.Stopped:
                        response.ServiceResults[0].Status = ServiceControllerStatus.Stopped;
                        response.ServiceResults[0].Success = true;
                        msg = string.Format("{0} was already stopped y'know, forgetaboutit", serviceNameLookup);
                        response.ServiceResults[0].AdditionalInfo = msg;
                        LogMessage(msg, LogLevel.Info);
                        break;
                    default:
                        response.ServiceResults[0].Success = false;
                        msg = string.Format("{0} was in an invalid state: {1} - could not proceed", serviceNameLookup, sc.Status);
                        response.ServiceResults[0].AdditionalInfo = msg;
                        LogMessage(msg, LogLevel.Info);
                        break;
                }

            }
            catch (Exception exc) {
                string msg = string.Format("Deployment service running on server {0} failed to stop. Error was: {1}", serverName, exc.Message);
                LogMessage(msg, LogLevel.Error, exc);
                response.ServiceResults[0].Status = ServiceControllerStatus.Running;
                response.ServiceResults[0].Error = exc;
                response.ServiceResults[0].Success = false;
            }
            return response;
        }


        public Stream DeployAll(string env, string clusterName, string buildDirectory) {
            string jsonResult = string.Empty;
            ServiceCallResponse response = new ServiceCallResponse();
          
            List<Server> servers = GetServers(env, (clusterName == "ALL" ? null : clusterName));

            foreach (Server svr in servers.Where(s => s.ServerTypeID != (int)ServerType.WEBSERVER)) {
                var res = DeployThis(env, svr.ServerName, buildDirectory);
                response.ServiceResults.Add(res.ServiceResults[0]);
            }
            jsonResult = JsonConvert.SerializeObject(response);
            return FormatStreamResponse(jsonResult);
        }

        public Stream Deploy(string env, string serverName, string buildDirectory) {
            return FormatStreamResponse(JsonConvert.SerializeObject(DeployThis(env, serverName, buildDirectory)));
        }

        private ServiceCallResponse DeployThis(string env, string serverName, string buildDirectory) {
            buildDirectory = ConvertHexToString(buildDirectory);

            ServiceCallResponse response = new ServiceCallResponse();
            FileSystemDirectory localDir = null;
            FileSystemDirectory remoteDir = null;
            try {
                Server svr = setupServ.GetServersByEnv(env).SingleOrDefault(s => s.ServerName == serverName);
                response.ServiceResults.Add(new ServiceResult() { Operation = ServiceResult.ServiceOperation.Deploy, Server = svr, ServiceName = this.GetType().Name });

                Uri localUri = new Uri(buildDirectory);
                Uri remoteUri = new Uri(string.Format(@"\\{0}\c$\GreenhouseService\", svr.ServerIP));
                
                localDir = new FileSystemDirectory(localUri);
                remoteDir = new FileSystemDirectory(remoteUri);

                LogMessage(string.Format("Attempting to deploy from: {0} to {1}", localDir, remoteDir), LogLevel.Info);

                CopyDirectoryStructure(localDir, remoteDir);
                response.ServiceResults[0].Status = ServiceControllerStatus.Running;
                response.ServiceResults[0].Success = true;
                LogMessage(string.Format("Copy from: {0} to {1} successful", localDir, remoteDir), LogLevel.Info);
            }
            catch (Exception exc) {
                response.ServiceResults[0].Status = ServiceControllerStatus.Stopped;
                response.ServiceResults[0].Error = exc;
                response.ServiceResults[0].Success = false;
                LogMessage(string.Format("Copy from: {0} to {1} failed", localDir, remoteDir), LogLevel.Error);
            }
            return response;
        }

        private void DeleteAll(IDirectory rootRemoteDirectory) {
            IEnumerable<IDirectory> dirs = null;
            IEnumerable<IFile> files = null;

            files = rootRemoteDirectory.GetFiles();
            foreach (IFile file in files) {
                file.Delete();
            }
            dirs = rootRemoteDirectory.GetDirectories();

            foreach (IDirectory dir in dirs) {
                DeleteAll(dir);
            }
        }

        private void CopyDirectoryStructure(IDirectory rootLocalDirectory, IDirectory rootRemoteDirectory) {
            IEnumerable<IDirectory> dirs = null;
            IEnumerable<IFile> files = null;

            //DeleteAll(rootRemoteDirectory);

            files = rootLocalDirectory.GetFiles();
            foreach (IFile file in files) {
                IFile destFile = new FileSystemFile(RemoteUri.CombineUri(rootRemoteDirectory.Uri, file.Name));
                file.CopyTo(destFile, true);
            }

            dirs = rootLocalDirectory.GetDirectories();

            foreach (IDirectory dir in dirs) {
                IDirectory remoteDir = new FileSystemDirectory(RemoteUri.CombineUri(rootRemoteDirectory.Uri, dir.Name));
                remoteDir.Create();
                CopyDirectoryStructure(dir, remoteDir);
            }
        }

        #endregion


        public string Hello() {
            return string.Format("Hello from GreenhouseDeploymentService");
        }

        private Stream FormatStreamResponse(string json) {
            byte[] resultBytes = Encoding.UTF8.GetBytes(json);
            WebOperationContext.Current.OutgoingResponse.ContentType = "application/x-json-stream";
            return new MemoryStream(resultBytes);
        }

        //public string EchoDBServer(string env) {
        //    string key = string.Format("{0}_GREENHOUSE", env);
        //    string connStr = System.Configuration.ConfigurationManager.ConnectionStrings[key].ConnectionString;
        //    return connStr;
        //}
    }
}
