using Greenhouse.Data.Model.Setup;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Greenhouse.DAL.DataSource.Core
{
    public abstract class BaseReportProvider<T>
    {
        public string ReportName { get; set; }
        public string ReportDirectory { get; set; }
        public Credential GreenhouseAWSCredential
        {
            get
            {
                return Credential.GetGreenhouseAWSCredential();
            }
        }
        public abstract Uri UriPath { get; }

        public virtual string[] GetFilePath()
        {
            return new string[] { ReportDirectory, GetReportName() };
        }

        public virtual string GetReportName()
        {
            return ReportName;
        }

        public IEnumerable<T> GetReports()
        {
            string content = string.Empty;

            var RAC = new RemoteAccessClient(UriPath, GreenhouseAWSCredential);

            IFile file = RAC.WithFile(RemoteUri.CombineUri(UriPath, GetFilePath()));
            if (file.Exists)
            {
                using (var sr = new StreamReader(file.Get()))
                {
                    content = sr.ReadToEnd();
                }
            }

            var report = JsonConvert.DeserializeObject<IEnumerable<T>>(content);

            return report;
        }

        public void DeleteReport()
        {
            var RAC = new RemoteAccessClient(UriPath, GreenhouseAWSCredential);

            IFile rawFile = RAC.WithFile(RemoteUri.CombineUri(UriPath, GetFilePath()));
            if (rawFile.Exists)
            {
                rawFile.Delete();
            }
        }

        public T GetReport()
        {
            string content = string.Empty;

            var RAC = new RemoteAccessClient(UriPath, GreenhouseAWSCredential);

            IFile file = RAC.WithFile(RemoteUri.CombineUri(UriPath, GetFilePath()));
            if (file.Exists)
            {
                using (var sr = new StreamReader(file.Get()))
                {
                    content = sr.ReadToEnd();
                }
            }

            var report = JsonConvert.DeserializeObject<T>(content);

            return report;
        }

        public void SaveReport(T reports)
        {
            var RAC = new RemoteAccessClient(UriPath, GreenhouseAWSCredential);

            IFile rawFile = RAC.WithFile(RemoteUri.CombineUri(UriPath, GetFilePath()));
            if (rawFile.Exists)
            {
                rawFile.Delete();
            }

            byte[] byteArray = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(reports));

            using (MemoryStream stream = new MemoryStream(byteArray))
            {
                rawFile.Put(stream);
            }
        }

        public void SaveReport<U>(U reports)
        {
            var RAC = new RemoteAccessClient(UriPath, GreenhouseAWSCredential);

            IFile rawFile = RAC.WithFile(RemoteUri.CombineUri(UriPath, GetFilePath()));
            if (rawFile.Exists)
            {
                rawFile.Delete();
            }

            byte[] byteArray = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(reports));

            using (MemoryStream stream = new MemoryStream(byteArray))
            {
                rawFile.Put(stream);
            }
        }
    }
}
