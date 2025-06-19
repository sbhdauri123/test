using Greenhouse.Data.Model.Setup;
using Greenhouse.DeploymentService;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using System.Diagnostics;
using System.Net;
using System.ServiceProcess;

namespace Greenhouse.DeploymentInvoker
{
    internal static class Program
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private static int Main(string[] args)
        {
            int exitCode = -1;
            string environment = System.Configuration.ConfigurationManager.AppSettings["applicationEnvironment"];
            StreamReader sr = null;
            try
            {
                if (args.Length < 4)
                {
                    return exitCode;
                }

                string command = args[0]; //start, stop, status, startAll, stopAll, statusAll, deploy, deployAll
                string env = args[1]; //%slyvester.env% DEV, QA, PROD
                string port = args[2]; //10001
                string param1 = args[3]; //%slyvester.region% clusterName (e.g. US_EAST) or serverName (e.g. DTIONEPR01-UE1D)
                string param2 = (args.Length == 5 ? args[4] : string.Empty); //%system.teamcity.build.checkoutDir% root of build directory 

                var clusters = Data.Services.SetupService.GetClustersByEnv(env);
                Cluster? cluster = param1 == "ALL" ? null : clusters.SingleOrDefault(c => c.ClusterName == param1);

                var servers = cluster == null ?
                    Data.Services.SetupService.GetServersByEnv(env).Where(s => s.IsActive)
                    : Data.Services.SetupService.GetServersByEnv(env).Where(s => s.IsActive && s.ClusterID == cluster.ClusterID);

                ServiceCallResponse deploymentInvokerResponse = new ServiceCallResponse();

                if (command.Equals("stopall", StringComparison.InvariantCultureIgnoreCase))
                {
                    //List<Server> servers = GetServers(env, (clusterName == "ALL" ? null : clusterName));

                    //var clusters = setupServ.GetClustersByEnv(env);
                    //Cluster? cluster = param1 == "ALL" ? null : clusters.SingleOrDefault(c => c.ClusterName == param1);

                    //var servers = cluster == null ?
                    //    setupServ.GetServersByEnv(env).Where(s => s.IsActive)
                    //    : setupServ.GetServersByEnv(env).Where(s => s.IsActive && s.ClusterID == cluster.ClusterID);

                    foreach (Server svr in servers.Where(s => s.ServerTypeID != (int)Greenhouse.Common.Constants.ServerType.WEBSERVER))
                    {
                        //var res = StartThis(env, svr.ServerName);
                        ServiceCallResponse response = new ServiceCallResponse();

                        try
                        {
                            string serviceNameLookup = $"GreenhouseWinService${svr.ServerAlias}";
                            response.ServiceResults.Add(new ServiceResult() { Operation = ServiceResult.ServiceOperation.Stop, Server = svr, ServiceName = serviceNameLookup });

                            LogMessage($"Attempting to stop service: {serviceNameLookup} on {svr.ServerName}...", LogLevel.Info);
                            var sc = new System.ServiceProcess.ServiceController(serviceNameLookup, svr.ServerName);
                            LogMessage($"service controller for: {serviceNameLookup} is: {(sc == null ? "null" : sc.Status.ToString())}", LogLevel.Info);

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
                        catch (Exception exc)
                        {
                            string msg = string.Format("Deployment service running on server {0} failed to stop. Error was: {1}", svr.ServerName, exc.Message);
                            LogMessage(msg, LogLevel.Error, exc);
                            response.ServiceResults[0].Status = ServiceControllerStatus.Running;
                            response.ServiceResults[0].Error = exc;
                            response.ServiceResults[0].Success = false;
                        }

                        deploymentInvokerResponse.ServiceResults.Add(response.ServiceResults[0]);
                    }
                }
                else if (command.Equals("startall", StringComparison.InvariantCultureIgnoreCase))
                {
                    foreach (Server svr in servers.Where(s => s.ServerTypeID != (int)Greenhouse.Common.Constants.ServerType.WEBSERVER))
                    {
                        ServiceCallResponse response = new ServiceCallResponse();

                        try
                        {
                            string serviceNameLookup = $"GreenhouseWinService${svr.ServerAlias}";
                            response.ServiceResults.Add(new ServiceResult() { Operation = ServiceResult.ServiceOperation.Start, Server = svr, ServiceName = serviceNameLookup });

                            LogMessage($"Attempting to start service: {serviceNameLookup} on {svr.ServerName}...", LogLevel.Info);
                            var sc = new System.ServiceProcess.ServiceController(serviceNameLookup, svr.ServerName);
                            LogMessage($"service controller for: {serviceNameLookup} is: {(sc == null ? "null" : sc.Status.ToString())}", LogLevel.Info);

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
                        catch (Exception exc)
                        {
                            string msg = string.Format("Deployment service running on server {0} failed to start. Error was: {1}", svr.ServerName, exc.Message);
                            LogMessage(msg, LogLevel.Error, exc);
                            response.ServiceResults[0].Status = ServiceControllerStatus.Stopped;
                            response.ServiceResults[0].Error = exc;
                            response.ServiceResults[0].Success = false;
                        }

                        deploymentInvokerResponse.ServiceResults.Add(response.ServiceResults[0]);
                    }
                }
                else if (command.Equals("deployall", StringComparison.InvariantCultureIgnoreCase) && !string.IsNullOrEmpty(param2))
                {
                    foreach (Server svr in servers.Where(s => s.ServerTypeID != (int)Greenhouse.Common.Constants.ServerType.WEBSERVER))
                    {
                        ServiceCallResponse response = new ServiceCallResponse();
                        FileSystemDirectory localDir = null;
                        FileSystemDirectory remoteDir = null;
                        try
                        {
                            string serviceNameLookup = $"GreenhouseWinService${svr.ServerAlias}";
                            response.ServiceResults.Add(new ServiceResult() { Operation = ServiceResult.ServiceOperation.Deploy, Server = svr, ServiceName = serviceNameLookup });

                            Uri localUri = new Uri(param2);
                            Uri remoteUri = new Uri(string.Format(@"\\{0}\c$\GreenhouseService_dotnet6\", svr.ServerIP));

                            localDir = new FileSystemDirectory(localUri);
                            remoteDir = new FileSystemDirectory(remoteUri);

                            LogMessage($"Attempting to start service: {localDir} on {remoteDir}...", LogLevel.Info);

                            CopyDirectoryStructure(localDir, remoteDir);
                            response.ServiceResults[0].Status = ServiceControllerStatus.Running;
                            response.ServiceResults[0].Success = true;
                            LogMessage($"Attempting to start service: {localDir} on {remoteDir}...", LogLevel.Info);
                        }
                        catch (Exception exc)
                        {
                            response.ServiceResults[0].Status = ServiceControllerStatus.Stopped;
                            response.ServiceResults[0].Error = exc;
                            response.ServiceResults[0].Success = false;
                            LogMessage($"Attempting to start service: {localDir} on {remoteDir}...", LogLevel.Error, exc);
                        }

                        deploymentInvokerResponse.ServiceResults.Add(response.ServiceResults[0]);
                    }
                }

                //string uri = string.Empty;
                //if (!string.IsNullOrEmpty(param2))
                //{
                //    uri = string.Format("http://localhost:{0}/greenhousedeploymentservice/{1}/{2}/{3}/{4}", port, command, env, param1, ConvertStringToHex(param2));
                //}
                //else
                //{
                //    uri = string.Format("http://localhost:{0}/greenhousedeploymentservice/{1}/{2}/{3}", port, command, env, param1);
                //}

                //Uri serviceAddress = new Uri(uri);

                //Console.WriteLine("URI is: " + serviceAddress);
                //HttpWebRequest webReq = (HttpWebRequest)System.Net.WebRequest.Create(serviceAddress);
                //webReq.Timeout = int.MaxValue;
                //webReq.ReadWriteTimeout = int.MaxValue;
                //string result = string.Empty;
                //using (sr = new StreamReader(webReq.GetResponse().GetResponseStream(), Encoding.UTF8))
                //{
                //    result = sr.ReadToEnd();
                //    Console.WriteLine(result);
                //}

                //var response = JsonConvert.DeserializeObject<Greenhouse.DeploymentService.ServiceCallResponse>(result);

                Console.WriteLine(deploymentInvokerResponse);
                exitCode = deploymentInvokerResponse.Success ? 0 : -1;
            }
            catch (WebException wexc)
            {
                Console.WriteLine(string.Format("A web exception occurred {0}", wexc.StackTrace));
                if (wexc.InnerException != null)
                {
                    Console.WriteLine(string.Format("INNER {0}", wexc.InnerException.StackTrace));
                }
                if (sr != null)
                {
                    Console.WriteLine(string.Format("Web Response {0}", sr.ReadToEnd()));
                }
            }
            catch (Exception exc)
            {
                Console.WriteLine(exc.StackTrace);
                if (exc.InnerException != null)
                {
                    Console.WriteLine(string.Format("INNER {0}", exc.InnerException.StackTrace));
                }
            }
            return exitCode;
        }

        public static string ConvertStringToHex(string str)
        {
            string hex = string.Empty;
            foreach (char c in str)
            {
                int tmp = c;
                hex += string.Format("{0:x2}", (uint)System.Convert.ToUInt32(tmp.ToString()));
            }
            return hex;
        }

        private static void LogMessage(string message, NLog.LogLevel level, Exception exc = null)
        {
            try
            {
                if (exc != null)
                {
                    EventLog.WriteEntry("Application", exc.ToString(), EventLogEntryType.Error);
                }
                message = String.Format("Greenhouse Deployment Service: {0}", message);
                LogEventInfo lei = Greenhouse.Logging.Msg.Create(level, logger.Name, message, exc);
                logger.Log(lei);
            }
            catch (Exception exc2)
            {
                EventLog.WriteEntry("Application", exc2.ToString(), EventLogEntryType.Error);
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
    }
}
