using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using Topshelf;
using System.Diagnostics;

namespace Greenhouse.Deployment {
    class Program {
        static void Main(string[] args) {

            
            try {
                string machineName = Environment.MachineName.ToString();
                EventLog.WriteEntry("Application", string.Format("Attempting to install/unistall of GreenhouseDeploymentService on {0}", machineName),
                    EventLogEntryType.Information);

                HostFactory.Run(x => {
                    x.Service<DeploymentService>(s => {
                        s.ConstructUsing(name => new DeploymentService());
                        s.WhenStarted(tc => tc.Start());
                        s.WhenStopped(tc => tc.Stop());
                    });
                    x.RunAsLocalSystem();


                    x.SetDescription("Deployment Service for Greenhouse");
                    x.SetDisplayName("GreenhouseDeploymentService");
                    x.SetServiceName("GreenhouseDeploymentService");
                    x.StartAutomaticallyDelayed();
                });
                EventLog.WriteEntry("Application", string.Format("Install/unistall of GreenhouseDeploymentService on {0} was successful", machineName), EventLogEntryType.Information);
            }
            catch (Exception exc) {
                EventLog.WriteEntry("Application", string.Format("Error GreenhouseDeploymentService install/unistall process failed.  - {0}", exc.ToString()), EventLogEntryType.Error);
            }
        }
    }
}
