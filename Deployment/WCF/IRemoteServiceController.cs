using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ServiceModel;
using System.Data.SqlClient;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.ServiceModel.Web;
using NLog;


namespace Greenhouse.Deployment.WCF {
    [ServiceContract]
    interface IRemoteServiceController {

            [OperationContract]
            [WebGet(UriTemplate = "startall/{env}/{clusterName}")]
            Stream StartAll(string env, string clusterName);

            [OperationContract]
            [WebGet(UriTemplate = "stopall/{env}/{clusterName}")]
            Stream StopAll
            (string env, string clusterName);

            [OperationContract]
            [WebGet(UriTemplate = "start/{env}/{serverName}")]
            Stream Start(string env, string serverName);

            [OperationContract]
            [WebGet(UriTemplate = "stop/{env}/{serverName}")]
            Stream Stop
            (string env, string serverName);

            [OperationContract]
            [WebGet(UriTemplate = "statusall/{env}/{clusterName}")]
            Stream StatusAll(string env, string clusterName);

            [OperationContract]
            [WebGet(UriTemplate = "status/{env}/{serverName}", BodyStyle = WebMessageBodyStyle.Bare)]
            Stream Status(string env, string serverName);

            [OperationContract]
            [WebGet(UriTemplate = "listservers/{env}/{clusterName}")]
            Stream ListServers(string env, string clusterName);

            [OperationContract]
            [WebGet(UriTemplate = "listclusters/{env}")]
            Stream ListClusters(string env);

            [OperationContract]
            [WebGet(UriTemplate = "hello")]
            string Hello();

            [OperationContract]
            [WebGet(UriTemplate = "deployall/{env}/{clusterName}/{buildDirectory}")]
            Stream DeployAll(string env, string clusterName, string buildDirectory);

            [OperationContract]
            [WebGet(UriTemplate = "deploy/{env}/{serverName}/{buildDirectory}")]
            Stream Deploy(string env, string serverName, string buildDirectory);
    }



}
