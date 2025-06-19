using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ServiceModel.Description;
using System.ServiceModel.Web;
using System.Diagnostics;
using NLog;

namespace Greenhouse.Deployment {
    public class DeploymentService {

        private static Logger logger;
        private WebServiceHost _serviceHost = null;
        private Uri _serviceAddress;
        private string environment;

        public DeploymentService() {

            try {
                logger = NLog.LogManager.GetCurrentClassLogger();
               // environment = System.Configuration.ConfigurationManager.AppSettings["applicationEnvironment"];
            }
            catch (Exception exc) {
                LogMessage("Fatal Application initializing component and NLog", LogLevel.Fatal, exc);
            }
        }

        public void Start() {
            LogMessage("Attempting Start of Greenhouse Deployment Service", LogLevel.Info);
            try {
                string machineName = Environment.MachineName;
                int startPort = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["SERVICE_PORT"]);

                _serviceAddress = new Uri(string.Format("http://{0}:{1}/greenhousedeploymentservice", machineName, startPort));

                _serviceHost = new WebServiceHost(typeof(Greenhouse.Deployment.WCF.VivakiRemoteServiceController), _serviceAddress);

                ServiceDebugBehavior debug = _serviceHost.Description.Behaviors.Find<ServiceDebugBehavior>();
                if (debug == null) {
                    _serviceHost.Description.Behaviors.Add(new ServiceDebugBehavior() { IncludeExceptionDetailInFaults = true });
                }
                else {
                    if (!debug.IncludeExceptionDetailInFaults) {
                        debug.IncludeExceptionDetailInFaults = true;
                    }
                }

                LogMessage(string.Format("Greenhouse Deployment Service binding to: {0}", _serviceAddress), LogLevel.Info);

                _serviceHost.Open();

                LogMessage(string.Format("Greenhouse Deployment Service listening on: {0}", _serviceAddress), LogLevel.Info);

            }
            catch (System.ServiceModel.CommunicationObjectFaultedException commExc) {
                LogMessage("A critical error has occurred starting the service. Access Denied, cannot bind to endpoint. You MUST run this service as an Administrator on the local machine", LogLevel.Fatal);
            }
            catch (Exception exc) {
                LogMessage("An unexpected exception occurred with the service.", LogLevel.Fatal, exc);
            }
        }

        public void Stop() {
            if (_serviceHost != null) {
                LogMessage(string.Format("Greenhouse Deployment Service attempting stop on: {0}", _serviceAddress), LogLevel.Info);
                _serviceHost.Close();
                LogMessage(string.Format("Greenhouse Deployment Service stopped on: {0}", _serviceAddress), LogLevel.Info);
            }
            else {
                LogMessage("Greenhouse Deployment Service serviceHost is null", LogLevel.Error);
            }

        }


        private void LogMessage(string message, NLog.LogLevel level, Exception exc = null) {
            try {
                if (exc != null) {
                    EventLog.WriteEntry("Application", exc.ToString(), EventLogEntryType.Error);
                }
                message = String.Format("Greenhouse Deployment Service: {0}", message);
                LogEventInfo lei = Greenhouse.Logging.Msg.Create(level, logger.Name, message, exc);
                logger.Log(lei);
            }
            catch (Exception exc2) {
                EventLog.WriteEntry("Application", exc2.ToString(), EventLogEntryType.Error);
            }

        }
    }
}
