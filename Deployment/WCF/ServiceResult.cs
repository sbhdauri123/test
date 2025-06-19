using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ServiceProcess;
using Greenhouse.Data.Model.Setup;

namespace Greenhouse.Deployment.WCF {
    public class ServiceResult {

        public enum ServiceOperation {
            Start,
            Stop,
            Status,
            Deploy
        }

        public bool Success { get; set; }
        public ServiceOperation Operation { get; set; }
        public Server Server { get; set; }
        public string ServiceName { get; set; }
        public ServiceControllerStatus Status { get; set; }
        public Exception Error { get; set; }
        public string AdditionalInfo { get; set; }
        public string Message {
            get {
                string msg = string.Format("Attempting {0} for service: {1} on server: {2}...{3} Status is: {4}. Additional info: {5}", this.Operation.ToString(), ServiceName, Server.ServerName, Success ? "Success!" : "Failed!", GetStatusDesc(Status), (string.IsNullOrEmpty(this.AdditionalInfo) ? "None" : this.AdditionalInfo));
                if (!Success) {
                    msg += string.Format("Error was: {0}. Additional info: {1}", Error == null ? "Unknown" : Error.Message, (string.IsNullOrEmpty(this.AdditionalInfo) ? "None" : this.AdditionalInfo));
                }
                return msg;
            }
        }

        private string GetStatusDesc(ServiceControllerStatus scs) {
            if(scs == 0) {
                return "Unknown";
            }
            else {
                return scs.ToString();
            }
        }
        public override string ToString() {
            StringBuilder sb = new StringBuilder();
            Type t = this.GetType();
            System.Reflection.PropertyInfo[] pis = t.GetProperties();
            for (int i = 0; i < pis.Length; i++) {
                System.Reflection.PropertyInfo pi = (System.Reflection.PropertyInfo)pis.GetValue(i);
                sb.AppendLine(string.Format("{0}: {1}", pi.Name, pi.GetValue(this, new object[] { })));
            }
            return sb.ToString();
        }
    }
}
